//go:build linux || darwin

package measure

import (
	"syscall"
	"time"
)

var clockOrigin = time.Now()

// Now returns a monotonic interval from an unspecified process-local origin.
func Now() time.Duration { return time.Since(clockOrigin) }

func CPUSeconds() (float64, error) {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		return 0, err
	}
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6, nil
}
