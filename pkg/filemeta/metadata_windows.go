package filemeta

import (
	"os"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Layout of Win32 FILE_BASIC_INFO, including its 8-byte alignment padding.
type fileBasicInfo struct {
	creationTime   int64
	lastAccessTime int64
	lastWriteTime  int64
	changeTime     int64
	fileAttributes uint32
	_              uint32
}

// Capture file identity and ChangeTime from the same handle.
// CreationTime and LastWriteTime cannot substitute for ChangeTime.
func snapshot(path string, info os.FileInfo) (os.FileInfo, time.Time) {
	f, err := os.Open(path)
	if err != nil {
		return info, time.Time{}
	}
	defer f.Close()

	current, err := f.Stat()
	if err != nil {
		return info, time.Time{}
	}
	var basic fileBasicInfo
	err = windows.GetFileInformationByHandleEx(windows.Handle(f.Fd()), windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&basic)), uint32(unsafe.Sizeof(basic)))
	if err != nil || basic.changeTime <= 0 {
		return current, time.Time{}
	}

	// Windows timestamps count 100-nanosecond ticks since 1601-01-01.
	const ticksPerSecond = 10_000_000
	const unixEpochSeconds = 11_644_473_600
	changeTime := time.Unix(basic.changeTime/ticksPerSecond-unixEpochSeconds,
		basic.changeTime%ticksPerSecond*100)
	return current, changeTime
}
