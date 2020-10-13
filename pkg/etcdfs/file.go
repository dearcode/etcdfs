package etcdfs

import (
	"bytes"
	"context"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

type etcdFile struct {
	etcdClient *clientv3.Client
	path       string
}

func NewEtcdFile(client *clientv3.Client, path string) nodefs.File {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	file := new(etcdFile)
	file.etcdClient = client
	file.path = path
	return file
}

func (f *etcdFile) SetInode(*nodefs.Inode) {
}
func (f *etcdFile) InnerFile() nodefs.File {
	return nil
}

func (f *etcdFile) String() string {
	return "etcdFile"
}

func (f *etcdFile) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	return fuse.OK
}
func (f *etcdFile) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.OK
}

func (f *etcdFile) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.OK
}
func (f *etcdFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	ctx := context.Background()
	res, err := f.etcdClient.Get(ctx, f.path)

	if err != nil {
		log.Println("Error:", err)
		return nil, fuse.EIO
	}

	end := int(off) + int(len(buf))
	if end > len(res.Kvs[0].Value) {
		end = len(res.Kvs[0].Value)
	}

	data := []byte(res.Kvs[0].Value)
	return fuse.ReadResultData(data[off:end]), fuse.OK
}

func (f *etcdFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	ctx := context.Background()
	res, err := f.etcdClient.Get(ctx, f.path)
	if err != nil {
		log.Println("Error:", err)
		return 0, fuse.EIO
	}

	originalValue := []byte(res.Kvs[0].Value)
	leftChunk := originalValue[:off]
	end := int(off) + int(len(data))

	var rightChunk []byte
	if end > len(res.Kvs[0].Value) {
		rightChunk = []byte{}
	} else {
		rightChunk = data[int(off)+int(len(data)):]
	}

	newValue := bytes.NewBuffer(leftChunk)
	newValue.Grow(len(data) + len(rightChunk))
	newValue.Write(data)
	newValue.Write(rightChunk)
	_, err = f.etcdClient.Put(ctx, f.path, newValue.String())
	if err != nil {
		log.Println("Error:", err)
		return 0, fuse.EIO
	}

	return uint32(len(data)), fuse.OK
}

func (f *etcdFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *etcdFile) Release() {
}

func (f *etcdFile) GetAttr(out *fuse.Attr) fuse.Status {
	log.Printf("GetAttr file:%v", f.path)
	res, err := f.etcdClient.Get(context.Background(), f.path)
	if err != nil {
		log.Println("Error:", err)
		return fuse.EIO
	}

	log.Printf("GetAttr file:%v, res:%#v", f.path, res)
	out.Mode = fuse.S_IFREG | 0666
	out.Size = uint64(len(res.Kvs[0].Value))
	return fuse.OK
}

func (f *etcdFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

func (f *etcdFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ENOSYS
}

func (f *etcdFile) Truncate(size uint64) fuse.Status {
	return fuse.OK
}

func (f *etcdFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *etcdFile) Chmod(perms uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *etcdFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.OK
}
