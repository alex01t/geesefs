package core

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestDiskCacheRejectWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := clearDiskCacheDir("."); err == nil {
		t.Fatal("accepted working directory for cleanup")
	}
}

func TestDiskCachePrepare(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "cache")
	nested := filepath.Join(dir, "sub")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(nested, "file")
	if err := os.WriteFile(stale, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "keep")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
		t.Skip(err)
	}
	fs := &Goofys{flags: &cfg.FlagStorage{CachePath: dir, CacheFileMode: 0600}}
	if err := fs.prepareDiskCache(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatal(err)
	}
	fs.flags.CacheCleanOnStart = true
	if err := fs.prepareDiskCache(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cache after cleanup: %v %v", entries, err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal(err)
	}
	fs.flags.CacheCleanOnStart = false
	fs.flags.CacheMinFreePercent = 25
	if err := fs.prepareDiskCache(); err != nil || fs.diskCleaner == nil {
		t.Fatalf("enable disk cleaner: %v", err)
	}
	fs.flags.CacheMinFreePercent = 101
	if err := fs.prepareDiskCache(); err == nil {
		t.Fatal("accepted invalid percentage")
	}
	fs.flags.CachePath = ""
	fs.flags.CacheMinFreePercent = 25
	if err := fs.prepareDiskCache(); err == nil {
		t.Fatal("accepted threshold without a cache directory")
	}
	fs.flags.CacheMinFreePercent = 0
	fs.flags.CacheCleanOnStart = true
	if err := fs.prepareDiskCache(); err == nil {
		t.Fatal("accepted startup cleanup without a cache directory")
	}
}

// Only the first four bytes should be fetched. The remaining bytes are resident.
type diskCacheReadBackend struct{ StorageBackend }

func (diskCacheReadBackend) GetBlob(req *GetBlobInput) (*GetBlobOutput, error) {
	if req.Key != "cold" || req.Start != 0 || req.Count != 4 {
		return nil, fmt.Errorf("unexpected backend read: %+v", req)
	}
	return &GetBlobOutput{Body: io.NopCloser(strings.NewReader("disk"))}, nil
}

func TestDiskCacheReclaim(t *testing.T) {
	dir := t.TempDir()
	fs := &Goofys{
		flags:       &cfg.FlagStorage{CachePath: dir, CacheFileMode: 0600},
		diskFdQueue: NewFDQueue(0),
		diskCleaner: &diskCacheCleaner{},
	}
	add := func(name string) *Inode {
		i := NewInode(fs, nil, name)
		i.buffers.helpers = &TestBLHelpers{}
		i.buffers.Add(0, []byte("disk"), BUF_CLEAN, false)
		b := i.buffers.Get(4)
		fs.tryEvictToDisk(i, b)
		if !b.onDisk {
			t.Fatal("spill failed")
		}
		i.buffers.EvictFromMemory(b)
		t.Cleanup(func() {
			if i.DiskCacheFD != nil {
				i.DiskCacheFD.Close()
			}
		})
		return i
	}
	busy := add("busy")
	busy.fileHandles = 1
	mpu := add("multipart")
	mpu.mpu = &MultipartBlobCommitInput{}
	dirty := add("dirty")
	dirty.buffers.Add(4, []byte("dirty"), BUF_DIRTY, false)
	hot := add("hot")
	cold := add("cold")
	cold.buffers.Add(4, []byte("resident"), BUF_CLEAN, false)

	fs.tryEvictToDisk(cold, cold.buffers.Get(12))
	ranges := hot.buffers.AddLoadingFromDisk(0, 4)
	if _, err := hot.loadFromDisk(ranges); err != nil {
		t.Fatal(err)
	}
	// Mimic the FD closer. The file is still eligible for LRU eviction.
	cold.DiskCacheFD.Close()
	cold.DiskCacheFD = nil
	fs.diskFdQueue.DeleteFD(cold)
	if err := fs.reclaimDiskCache(func() (bool, error) { return cold.OnDisk, nil }); err != nil {
		t.Fatal(err)
	}
	if !busy.OnDisk || !dirty.OnDisk || !mpu.OnDisk || !hot.OnDisk || cold.OnDisk || fs.diskCleaner.low.Load() {
		t.Fatal("wrong eviction order or pressure state")
	}
	if _, err := os.Stat(filepath.Join(dir, "cold")); !os.IsNotExist(err) {
		t.Fatalf("cold file still present: %v", err)
	}
	holes, _, _ := cold.buffers.GetHoles(0, 12)
	if len(holes) != 1 || holes[0] != (Range{0, 4}) {
		t.Fatalf("unexpected holes: %v", holes)
	}
	b := cold.buffers.Get(12)
	if b == nil || b.onDisk || string(b.data) != "resident" {
		t.Fatal("lost resident buffer")
	}
	if err := fs.reclaimDiskCache(func() (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	fs.tryEvictToDisk(cold, b)
	if b.onDisk || !fs.diskCleaner.low.Load() {
		t.Fatal("spill under disk pressure")
	}
	fs.flags.ReadAheadParallelKB = 1
	fs.flags.ReadRetryAttempts = 1
	fs.bufferPool = &BufferPool{max: 1024}
	cold.Parent = &Inode{dir: &DirInodeData{cloud: diskCacheReadBackend{}}}
	cold.userMetadata = make(map[string][]byte)
	cold.knownSize, cold.Attributes.Size = 12, 12
	cold.mu.Lock()
	defer cold.mu.Unlock()
	miss, err := cold.LoadRange(0, 12, 0, false)
	if !miss || err != nil {
		t.Fatalf("backend read after eviction: miss=%v err=%v", miss, err)
	}
	parts, _, err := cold.buffers.GetData(0, 12, false)
	if err != nil || string(bytes.Join(parts, nil)) != "diskresident" {
		t.Fatalf("read after eviction: %q %v", parts, err)
	}
}
