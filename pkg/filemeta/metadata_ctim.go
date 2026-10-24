//go:build linux || openbsd || dragonfly || solaris || illumos

package filemeta

import (
	"os"
	"syscall"
	"time"
)

func snapshot(_ string, info os.FileInfo) (os.FileInfo, time.Time) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil || (stat.Ctim.Sec == 0 && stat.Ctim.Nsec == 0) {
		return info, time.Time{}
	}
	return info, time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec))
}
