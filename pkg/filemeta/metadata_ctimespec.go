//go:build darwin || freebsd || netbsd

package filemeta

import (
	"os"
	"syscall"
	"time"
)

func snapshot(_ string, info os.FileInfo) (os.FileInfo, time.Time) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil || (stat.Ctimespec.Sec == 0 && stat.Ctimespec.Nsec == 0) {
		return info, time.Time{}
	}
	return info, time.Unix(int64(stat.Ctimespec.Sec), int64(stat.Ctimespec.Nsec))
}
