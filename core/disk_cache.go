package core

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/disk"
	"github.com/tidwall/btree"
)

// Queue entries outlive cache FDs. Update diskCacheUse with inode.mu held.
// Never take inode.mu while holding the queue lock.
type diskCacheCleaner struct {
	mu    sync.Mutex
	queue btree.Map[uint64, *Inode]
	seq   uint64
	low   atomic.Bool
	wake  chan struct{}
	done  chan struct{}
}

func (c *diskCacheCleaner) touch(inode *Inode) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.queue.Delete(inode.diskCacheUse)
	c.seq++
	inode.diskCacheUse = c.seq
	c.queue.Set(c.seq, inode)
	c.mu.Unlock()
}

func (c *diskCacheCleaner) forget(inode *Inode) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.queue.Delete(inode.diskCacheUse)
	inode.diskCacheUse = 0
	c.mu.Unlock()
}

func (fs *Goofys) prepareDiskCache() error {
	f := fs.flags
	if f.CacheMinFreePercent < 0 || f.CacheMinFreePercent > 100 {
		return fmt.Errorf("--cache-min-free-percent must be between 0 and 100")
	}
	if !f.CacheCleanOnStart && f.CacheMinFreePercent == 0 {
		return nil
	}
	if f.CachePath == "" {
		return fmt.Errorf("disk cache cleanup requires --cache")
	}
	if err := os.MkdirAll(f.CachePath, f.CacheFileMode|((f.CacheFileMode&0777)>>2)); err != nil {
		return err
	}
	if f.CacheCleanOnStart {
		if err := clearDiskCacheDir(f.CachePath); err != nil {
			return err
		}
	}
	if f.CacheMinFreePercent > 0 {
		low, err := fs.diskSpaceLow()
		if err != nil {
			return err
		}
		fs.diskCleaner = &diskCacheCleaner{wake: make(chan struct{}, 1), done: make(chan struct{})}
		fs.diskCleaner.low.Store(low)
	}
	return nil
}

func clearDiskCacheDir(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return err
	}
	if filepath.Dir(resolved) == resolved {
		return fmt.Errorf("refusing to clear filesystem root %q", resolved)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return err
	}
	for p := cwd; ; p = filepath.Dir(p) {
		if resolved == p {
			return fmt.Errorf("refusing to clear cache directory %q", resolved)
		}
		if filepath.Dir(p) == p {
			break
		}
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return err
	}
	defer root.Close()
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := root.RemoveAll(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Goofys) diskSpaceLow() (bool, error) {
	usage, err := disk.Usage(fs.flags.CachePath)
	if err != nil {
		return false, err
	}
	if usage.Total == 0 {
		return false, fmt.Errorf("unknown capacity of %s", fs.flags.CachePath)
	}
	return float64(usage.Free)*100 < float64(usage.Total)*float64(fs.flags.CacheMinFreePercent), nil
}

func (fs *Goofys) cleanDiskCache() {
	c := fs.diskCleaner
	defer close(c.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-fs.shutdownCh:
			return
		case <-ticker.C:
		case <-c.wake:
		}
		if err := fs.reclaimDiskCache(fs.diskSpaceLow); err != nil {
			log.Errorf("Checking disk cache space: %v", err)
		}
	}
}

func (fs *Goofys) reclaimDiskCache(spaceLow func() (bool, error)) error {
	c := fs.diskCleaner
	c.mu.Lock()
	last := c.seq
	c.mu.Unlock()
	next := uint64(1)
	for {
		low, err := spaceLow()
		if err != nil {
			return err
		}
		c.low.Store(low)
		if !low {
			return nil
		}
		for {
			select {
			case <-fs.shutdownCh:
				return nil
			default:
			}
			var inode *Inode
			var id uint64
			c.mu.Lock()
			c.queue.Ascend(next, func(key uint64, i *Inode) bool {
				id, inode = key, i
				return false
			})
			c.mu.Unlock()
			if inode == nil || id > last {
				return nil
			}
			next = id + 1
			if !inode.mu.TryLock() {
				continue
			}
			reclaimed := inode.diskCacheUse == id && inode.evictDiskCache()
			inode.mu.Unlock()
			if reclaimed {
				break // Recheck free space before deleting another file.
			}
		}
	}
}

// Caller holds inode.mu. Preserve RAM buffers and discard only ranges backed
// solely by the cache file. Subsequent reads fetch those ranges from storage.
func (inode *Inode) evictDiskCache() bool {
	if !inode.OnDisk || inode.CacheState != ST_CACHED || inode.buffers.AnyUnclean() ||
		atomic.LoadInt32(&inode.fileHandles) != 0 || inode.IsFlushing != 0 || inode.mpu != nil || len(inode.readRanges) != 0 {
		return false
	}
	if err := inode.removeDiskCacheFile(); err != nil {
		log.Errorf("Evicting disk cache for %v: %v", inode.FullName(), err)
		return false
	}
	inode.buffers.RemoveRange(0, ^uint64(0), func(b *FileBuffer) bool {
		if !b.onDisk {
			return false
		}
		b.onDisk = false
		return b.data == nil && !b.zero
	})
	inode.fs.diskCleaner.forget(inode)
	return true
}

func (inode *Inode) removeDiskCacheFile() error {
	if inode.DiskCacheFD != nil {
		inode.DiskCacheFD.Close()
		inode.DiskCacheFD = nil
		inode.fs.diskFdQueue.DeleteFD(inode)
	}
	err := os.Remove(filepath.Join(inode.fs.flags.CachePath, inode.FullName()))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	inode.OnDisk = false
	return nil
}
