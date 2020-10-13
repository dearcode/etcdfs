package etcdfs

import (
	"bytes"
	"context"
	"log"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/coreos/etcd/clientv3"
)

type EtcdFs struct {
	pathfs.FileSystem
	client *clientv3.Client
}

func New(user, password, endpoint string) *EtcdFs {
	c, err := clientv3.New(clientv3.Config{
		Username:  user,
		Password:  password,
		Endpoints: strings.Split(endpoint, ","),
	})
	if err != nil {
		panic(err)
	}

	return &EtcdFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		client:     c,
	}

}

func (fs *EtcdFs) Unlink(name string, ctx *fuse.Context) (code fuse.Status) {
	log.Printf("Unlink name:%v\n", name)
	if name == "" {
		return fuse.OK
	}

	_, err := fs.client.Delete(context.Background(), name)

	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (fs *EtcdFs) Rmdir(name string, ctx *fuse.Context) (code fuse.Status) {
	log.Printf("Rmdir name:%v\n", name)
	if name == "" {
		return fuse.OK
	}

	_, err := fs.client.Delete(context.Background(), name, clientv3.WithPrefix())
	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (fs *EtcdFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	log.Printf("Create name:%v\n", name)
	_, err := fs.client.Put(context, name, "")
	if err != nil {
		log.Println("Create Error:", err)
		return nil, fuse.ENOENT
	}

	return NewEtcdFile(fs.client, name), fuse.OK
}

func (fs *EtcdFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Printf("Mkdir name:%v\n", name)
	if name == "" {
		return fuse.OK
	}

	_, err := fs.client.Put(context, name, "")

	if err != nil {
		log.Println(err)
		return fuse.ENOENT
	}

	return fuse.OK
}

func (fs *EtcdFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Printf("GetAttr name:%v\n", name)
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0666}, fuse.OK
	}

	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	res, err := fs.client.Get(context, name, clientv3.WithPrefix())
	if err != nil {
		log.Printf("GetAttr Get name:%v error:%v\n", name, err)
		return nil, fuse.ENOENT
	}

	if len(res.Kvs) == 0 || len(res.Kvs[0].Key) == 0 {
		log.Printf("GetAttr Get name:%v not found\n", name)
		return nil, fuse.ENOENT
	}

	var attr fuse.Attr

	log.Printf("GetAttr Get name:%v kv:%#v\n", name, res.Kvs[0])
	if bytes.Contains(res.Kvs[0].Key[len(name):], []byte("/")) {
		attr = fuse.Attr{
			Mode: fuse.S_IFDIR | 0666,
		}
	} else {
		attr = fuse.Attr{
			Mode: fuse.S_IFREG | 0666, Size: uint64(len(res.Kvs[0].Value)),
		}
	}

	return &attr, fuse.OK
}

func (fs *EtcdFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	log.Printf("OpenDir name:%v\n", name)
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	if !strings.HasSuffix(name, "/") {
		name += "/"
	}

	res, err := fs.client.Get(context, name, clientv3.WithPrefix())
	if err != nil {
		log.Println("OpenDir Error:", err)
		return nil, fuse.ENOENT
	}

	entries := []fuse.DirEntry{}

	m := make(map[string]bool)

	for _, kv := range res.Kvs {
		f := bytes.TrimPrefix(kv.Key, []byte(name))
		log.Printf("OpenDir key kv:%s, name:%v\n", f, name)
		if bytes.Contains(f, []byte("/")) {
			f = bytes.Split(f, []byte("/"))[0]
			m[string(f)] = true
		} else {
			m[string(f)] = false
		}
		log.Printf("OpenDir key 22222:%s\n", f)
	}

	for f, ok := range m {
		if ok {
			entries = append(entries, fuse.DirEntry{Name: f, Mode: fuse.S_IFDIR})
		} else {
			entries = append(entries, fuse.DirEntry{Name: f, Mode: fuse.S_IFREG})
		}
	}

	return entries, fuse.OK
}

func (fs *EtcdFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	log.Printf("Open name:%v\n", name)
	_, err := fs.client.Get(context, name)
	if err != nil {
		log.Println("Open Error:", err)
		return nil, fuse.ENOENT
	}

	return NewEtcdFile(fs.client, name), fuse.OK
}
