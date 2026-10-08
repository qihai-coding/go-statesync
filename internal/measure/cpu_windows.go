//go:build windows

package measure

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

var processTimes = syscall.NewLazyDLL("kernel32.dll").NewProc("GetProcessTimes")
var performanceCounter = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceCounter")
var counterFrequency = func() int64 {
	var frequency int64
	ok, _, err := syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&frequency)))
	if ok == 0 || frequency <= 0 {
		panic(fmt.Errorf("QueryPerformanceFrequency: %w", err))
	}
	return frequency
}()

// Now uses QPC because Go's Windows monotonic clock can round short steps to zero.
// The origin is unspecified; subtract two readings to measure an interval.
func Now() time.Duration {
	var counter int64
	ok, _, err := performanceCounter.Call(uintptr(unsafe.Pointer(&counter)))
	if ok == 0 {
		panic(fmt.Errorf("QueryPerformanceCounter: %w", err))
	}
	seconds, remainder := counter/counterFrequency, counter%counterFrequency
	return time.Duration(seconds)*time.Second + time.Duration(float64(remainder)/float64(counterFrequency)*1e9)
}

func CPUSeconds() (float64, error) {
	var created, exited, kernel, user syscall.Filetime
	ok, _, err := processTimes.Call(^uintptr(0), uintptr(unsafe.Pointer(&created)), uintptr(unsafe.Pointer(&exited)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user)))
	if ok == 0 {
		return 0, fmt.Errorf("GetProcessTimes: %w", err)
	}
	k := uint64(kernel.HighDateTime)<<32 | uint64(kernel.LowDateTime)
	u := uint64(user.HighDateTime)<<32 | uint64(user.LowDateTime)
	return float64(k+u) / 1e7, nil
}
