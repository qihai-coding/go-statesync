package main

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if role := os.Getenv(workerEnv); role != "" {
		if err := workerMain(role); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestIsolatedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := runIsolated(ctx, isolatedOptions{Duration: 2 * time.Second, Clients: 2, Rooms: 2, ResumeEvery: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Passed || !out.Converged || !out.ConnectionsAlive || out.MemoryChecked || out.MemoryBounded {
		t.Fatal("isolated run failed or claimed long-term stability", out.Failures)
	}
	s, l := out.ServerProcess, out.LoadProcess
	if s == nil || l == nil || s.PID == l.PID || s.PID == os.Getpid() || l.PID == os.Getpid() {
		t.Fatal("measurement is not isolated")
	}
	if math.Abs(out.ProcessCPUSeconds-s.ProcessCPUSeconds-l.ProcessCPUSeconds) > 1e-9 || out.AllocatedBytes != s.AllocatedBytes+l.AllocatedBytes {
		t.Fatal("aggregate process costs do not equal worker totals")
	}
	for i, sample := range out.Samples {
		if sample.HeapBytes != s.Samples[i].HeapBytes+l.Samples[i].HeapBytes || sample.Goroutines != s.Samples[i].Goroutines+l.Samples[i].Goroutines {
			t.Fatal("aggregate samples include the coordinator or omit a worker")
		}
	}
	for _, room := range out.Rooms {
		if room.Resumes == 0 || !room.Converged {
			t.Fatal("room never resumed or converged", room)
		}
	}
}

func TestIsolatedCancellation(t *testing.T) {
	for _, timeout := range []time.Duration{0, 2 * time.Second} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		started := time.Now()
		out, err := runIsolated(ctx, isolatedOptions{Duration: time.Hour, Clients: 1, Rooms: 1})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || out.Passed {
			t.Fatal("cancellation did not fail the run", err)
		}
		if time.Since(started) > timeout+8*time.Second {
			t.Fatal("cancellation did not bound worker cleanup")
		}
	}
}

func TestChildFailureAndCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, err := startChild(ctx, "server")
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	load, err := startChild(ctx, "load")
	if err != nil {
		t.Fatal(err)
	}
	defer load.close()
	if err := server.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := waitChildren(ctx, server, load, time.Now().Add(time.Hour)); err == nil || !strings.Contains(err.Error(), "server worker exited") {
		t.Fatal("unexpected worker exit was not reported", err)
	}
	load.close()
	select {
	case <-load.done:
		if load.cmd.ProcessState == nil {
			t.Fatal("worker was not reaped")
		}
	default:
		t.Fatal("worker still running after cleanup")
	}
}

func TestWorkerRejectsInvalidControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	child, err := startChild(ctx, "server")
	if err != nil {
		t.Fatal(err)
	}
	defer child.close()
	if _, err := child.call(ctx, control{Op: "start"}); err == nil {
		t.Fatal("uninitialized worker started")
	}
	select {
	case <-child.done:
	case <-ctx.Done():
		t.Fatal("failed worker did not exit")
	}
}
