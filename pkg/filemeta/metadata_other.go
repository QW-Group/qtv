//go:build !(linux || openbsd || dragonfly || solaris || illumos || darwin || freebsd || netbsd || windows)

package filemeta

import (
	"os"
	"time"
)

// No reliable change-time API is implemented for this platform.
func snapshot(_ string, info os.FileInfo) (os.FileInfo, time.Time) {
	return info, time.Time{}
}
