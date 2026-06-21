//go:build !linux

package main

func hostMemTotalBytes() int64 {
	return 0
}

func hostCPUModel() string {
	return ""
}

type diskInfo struct {
	TotalBytes int64
	FreeBytes  int64
	Filesystem string
}

func hostDiskInfo(string) diskInfo {
	return diskInfo{}
}
