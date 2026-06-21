//go:build linux

package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func hostMemTotalBytes() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() {
		_ = f.Close()
	}()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	_ = sc.Err()
	return 0
}

func hostCPUModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer func() {
		_ = f.Close()
	}()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "model name") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		return strings.TrimSpace(parts[1])
	}
	_ = sc.Err()
	return ""
}

// fsTypes maps well-known Linux statfs magic numbers to filesystem names.
var fsTypes = map[int64]string{
	0xEF53:     "ext4",
	0x9123683E: "btrfs",
	0x58465342: "xfs",
	0x01021994: "tmpfs",
	0x794c7630: "overlayfs",
	0x6969:     "nfs",
	0x65735546: "fuse",
	0x53464846: "wslfs",
}

type diskInfo struct {
	TotalBytes int64
	FreeBytes  int64
	Filesystem string
}

func hostDiskInfo(path string) diskInfo {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return diskInfo{}
	}
	fsType := fsTypes[int64(stat.Type)] //nolint:unconvert // Type width differs per arch.
	if fsType == "" {
		fsType = "unknown"
	}
	return diskInfo{
		TotalBytes: int64(stat.Blocks) * stat.Bsize, //nolint:gosec // benchmark host info, not security sensitive.
		FreeBytes:  int64(stat.Bavail) * stat.Bsize, //nolint:gosec // benchmark host info, not security sensitive.
		Filesystem: fsType,
	}
}
