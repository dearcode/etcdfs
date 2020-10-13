package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/dearcode/etcdfs/pkg/etcdfs"
)

var (
	etcdUser      = flag.String("etcd_user", "", "etcd user")
	etcdPassword  = flag.String("etcd_password", "", "etcd password")
	etcdEndpoints = flag.String("etcd_endpoints", "", "etcd endpoints")
	mountPath     = flag.String("mount_path", "", "mount path")
)

func main() {
	flag.Parse()

	if *etcdEndpoints == "" || *mountPath == "" {
		log.Fatal("Usage:\n  etcdfs -etcd_user root -etcd_password 123456 -etcd_endpoints 127.0.0.1:2379 -mount_path /tmp/kaka")
	}

	fs := etcdfs.New(*etcdUser, *etcdPassword, *etcdEndpoints)

	fmt.Printf("fs:%#v\n", fs)

	nfs := pathfs.NewPathNodeFs(fs, nil)
	server, _, err := nodefs.MountRoot(*mountPath, nfs.Root(), &nodefs.Options{Debug: true})
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
