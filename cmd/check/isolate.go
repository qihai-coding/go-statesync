package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/qihai-coding/go-statesync/internal/measure"
	"github.com/qihai-coding/go-statesync/internal/nettest"
)

const workerEnv = "STATESYNC_CHECK_WORKER"

type isolatedOptions struct {
	Duration, ResumeEvery time.Duration
	Clients, Rooms        int
	Profile               nettest.Profile
	CPUProfile            string
}

type processReport struct {
	PID                                             int
	Started, CapturedAt                             time.Time
	DurationSeconds                                 float64
	Samples                                         []Sample
	ProcessCPUSeconds, ProcessCPUPercentOneCore     float64
	ProcessCPUPercentHost, AllocationBytesPerSecond float64
	AllocatedBytes                                  uint64
	MemoryChecked, MemoryBounded                    bool
}

type control struct {
	Op      string
	At      time.Time
	Options isolatedOptions
	Address string
	CA      []byte
}

type response struct {
	Error   string `json:",omitempty"`
	PID     int
	Address string `json:",omitempty"`
	CA      []byte `json:",omitempty"`
	At      time.Time
	Sample  Sample
	Views   []syncnet.View `json:",omitempty"`
	Clients []clientState  `json:",omitempty"`
}

type child struct {
	role string
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  io.ReadCloser
	enc  *json.Encoder
	dec  *json.Decoder
	done chan struct{}
	err  error
}

func startChild(ctx context.Context, role string) (*child, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	c := &child{role: role, cmd: exec.CommandContext(ctx, executable), done: make(chan struct{})}
	c.cmd.Env = append(os.Environ(), workerEnv+"="+role)
	c.cmd.Stderr = os.Stderr
	c.in, err = c.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	c.out, err = c.cmd.StdoutPipe()
	if err != nil {
		c.in.Close()
		return nil, err
	}
	if err = c.cmd.Start(); err != nil {
		c.in.Close()
		c.out.Close()
		return nil, err
	}
	c.enc, c.dec = json.NewEncoder(c.in), json.NewDecoder(c.out)
	go func() { c.err = c.cmd.Wait(); close(c.done) }()
	return c, nil
}

// Calls are serialized per child; closing the pipes unblocks a canceled call.
func (c *child) call(ctx context.Context, request control) (response, error) {
	type result struct {
		reply response
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		var reply response
		err := c.enc.Encode(request)
		if err == nil {
			err = c.dec.Decode(&reply)
		}
		if err == nil && reply.Error != "" {
			err = errors.New(reply.Error)
		}
		resultCh <- result{reply, err}
	}()
	select {
	case result := <-resultCh:
		if result.err != nil {
			return result.reply, fmt.Errorf("%s worker: %w", c.role, result.err)
		}
		return result.reply, nil
	case <-ctx.Done():
		c.in.Close()
		c.out.Close()
		return response{}, ctx.Err()
	}
}

func (c *child) close() error {
	c.in.Close()
	select {
	case <-c.done:
	case <-time.After(3 * time.Second):
		c.cmd.Process.Kill()
		<-c.done
	}
	c.out.Close()
	return c.err
}

func waitUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(max(0, time.Until(at)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func workerMain(role string) error {
	if role != "server" && role != "load" {
		return errors.New("invalid worker role")
	}
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	var server *syncnet.Server
	var rooms []*syncnet.Room
	var workload *load
	var started time.Time
	var cpuBefore float64
	var allocatedBefore uint64
	var profileFile *os.File
	defer func() {
		if profileFile != nil {
			pprof.StopCPUProfile()
			profileFile.Close()
		}
		if workload != nil {
			workload.close()
		}
		if server != nil {
			server.Close()
		}
	}()
	initialized := false
	for {
		var request control
		if err := decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		reply := response{PID: os.Getpid()}
		err := func() error {
			switch request.Op {
			case "init":
				if initialized {
					return errors.New("already initialized")
				}
				o := request.Options
				if err := o.validate(); err != nil {
					return err
				}
				names := make([]string, o.Rooms)
				for i := range names {
					names[i] = fmt.Sprintf("bench-%d", i)
				}
				if role == "server" {
					cert, pem, err := syncnet.LocalCertificate()
					if err != nil {
						return err
					}
					server, err = syncnet.Listen("127.0.0.1:0", syncnet.ServerTLS(cert), syncnet.DefaultConfig())
					if err != nil {
						return err
					}
					for _, name := range names {
						room, err := server.CreateRoom(name, arena.New())
						if err != nil {
							return err
						}
						rooms = append(rooms, room)
					}
					reply.Address, reply.CA = server.Addr().String(), pem
					if o.CPUProfile != "" {
						profileFile, err = os.Create(o.CPUProfile)
						if err != nil {
							return err
						}
						if err = pprof.StartCPUProfile(profileFile); err != nil {
							return err
						}
					}
				} else {
					tc, err := syncnet.ClientTLS(request.CA, "localhost")
					if err != nil {
						return err
					}
					workload, err = newLoad(context.Background(), request.Address, names, tc, o.Clients, o.ResumeEvery, o.Profile)
					if err != nil {
						return err
					}
				}
				initialized = true
			case "start", "sample", "finish", "recover":
				if !initialized {
					return errors.New("worker not initialized")
				}
				if request.Op == "start" {
					if !started.IsZero() {
						return errors.New("already started")
					}
					runtime.GC()
				} else if started.IsZero() {
					return errors.New("worker not started")
				}
				if err := waitUntil(context.Background(), request.At); err != nil {
					return err
				}
				if request.Op == "recover" {
					if workload == nil {
						return errors.New("not a load worker")
					}
					workload.recoverNetwork()
					reply.At = time.Now()
					return nil
				}
				reply.At = time.Now()
				if request.Op == "finish" && workload != nil {
					reply.Clients = workload.states()
				}
				for _, room := range rooms {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					view, metrics, err := room.Inspect(ctx)
					cancel()
					if err != nil {
						return err
					}
					reply.Sample.Rooms = append(reply.Sample.Rooms, metrics)
					if request.Op == "finish" {
						reply.Views = append(reply.Views, view)
					}
				}
				if request.Op != "start" {
					runtime.GC()
				}
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				cpu, err := measure.CPUSeconds()
				if err != nil {
					return err
				}
				if request.Op == "start" {
					started, cpuBefore, allocatedBefore = time.Now(), cpu, memory.TotalAlloc
					reply.At = started
					if workload != nil {
						workload.start(started)
					}
				}
				reply.Sample.Seconds = time.Since(started).Seconds()
				reply.Sample.HeapBytes, reply.Sample.HeapObjects = memory.HeapAlloc, memory.HeapObjects
				reply.Sample.Goroutines = runtime.NumGoroutine()
				reply.Sample.CPUSeconds = cpu - cpuBefore
				reply.Sample.AllocatedBytes = memory.TotalAlloc - allocatedBefore
			default:
				return errors.New("invalid worker command")
			}
			return nil
		}()
		if err != nil {
			reply.Error = err.Error()
		}
		if sendErr := encoder.Encode(reply); sendErr != nil {
			return sendErr
		}
		if err != nil {
			return err
		}
	}
}

func (o isolatedOptions) validate() error {
	if o.Duration < time.Second || o.Clients < 1 || o.Clients > 16 || o.Rooms < 1 || o.Rooms > 64 || o.ResumeEvery < 0 {
		return errors.New("invalid workload size or duration")
	}
	return o.Profile.Validate()
}

func samplePair(ctx context.Context, server, load *child, request control) (response, response, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	type result struct {
		reply response
		err   error
	}
	serverCh := make(chan result, 1)
	go func() { reply, err := server.call(ctx, request); serverCh <- result{reply, err} }()
	l, err := load.call(ctx, request)
	if err != nil {
		cancel()
	}
	s := <-serverCh
	return s.reply, l, errors.Join(s.err, err)
}

func runIsolated(ctx context.Context, o isolatedOptions) (out Report, err error) {
	out = Report{Mode: "isolated", Started: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Processor: os.Getenv("PROCESSOR_IDENTIFIER"), LogicalCPUs: runtime.NumCPU(), Clients: o.Clients * o.Rooms, ClientsPerRoom: o.Clients, Items: o.Rooms * 16, Profile: o.Profile}
	if err = o.validate(); err != nil {
		return out, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server, err := startChild(ctx, "server")
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, server.close())
		if err != nil {
			out.Passed = false
		}
	}()
	load, err := startChild(ctx, "load")
	if err != nil {
		return out, err
	}
	defer func() {
		err = errors.Join(err, load.close())
		if err != nil {
			out.Passed = false
		}
	}()
	initCtx, initCancel := context.WithTimeout(ctx, time.Duration(o.Clients*o.Rooms)*10*time.Second+15*time.Second)
	defer initCancel()
	ready, err := server.call(initCtx, control{Op: "init", Options: o})
	if err != nil {
		return out, err
	}
	if _, err = load.call(initCtx, control{Op: "init", Options: o, Address: ready.Address, CA: ready.CA}); err != nil {
		return out, err
	}
	s, l, err := samplePair(ctx, server, load, control{Op: "start", At: time.Now().Add(200 * time.Millisecond)})
	if err != nil {
		return out, err
	}
	base := s.Sample.Rooms
	out.ServerProcess = &processReport{PID: s.PID, Started: s.At}
	out.LoadProcess = &processReport{PID: l.PID, Started: l.At}
	start := maxTime(s.At, l.At)
	addSample := func(s, l response) {
		out.ServerProcess.Samples = append(out.ServerProcess.Samples, s.Sample)
		out.LoadProcess.Samples = append(out.LoadProcess.Samples, l.Sample)
		out.Samples = append(out.Samples, Sample{Seconds: min(s.Sample.Seconds, l.Sample.Seconds), HeapBytes: s.Sample.HeapBytes + l.Sample.HeapBytes,
			HeapObjects: s.Sample.HeapObjects + l.Sample.HeapObjects, Goroutines: s.Sample.Goroutines + l.Sample.Goroutines,
			CPUSeconds: s.Sample.CPUSeconds + l.Sample.CPUSeconds, AllocatedBytes: s.Sample.AllocatedBytes + l.Sample.AllocatedBytes, Rooms: s.Sample.Rooms})
		fmt.Printf("%.0f 秒：服务器堆=%.2f MiB，客户端及代理堆=%.2f MiB\n", out.Samples[len(out.Samples)-1].Seconds,
			float64(s.Sample.HeapBytes)/(1<<20), float64(l.Sample.HeapBytes)/(1<<20))
	}
	interval := min(30*time.Second, o.Duration)
	for at := start.Add(interval); at.Before(start.Add(o.Duration)); at = at.Add(interval) {
		if err = waitChildren(ctx, server, load, at); err != nil {
			return out, err
		}
		s, l, err = samplePair(ctx, server, load, control{Op: "sample"})
		if err != nil {
			return out, err
		}
		addSample(s, l)
	}
	if err = waitChildren(ctx, server, load, start.Add(o.Duration)); err != nil {
		return out, err
	}
	recoverCtx, recoverCancel := context.WithTimeout(ctx, 15*time.Second)
	recovered, err := load.call(recoverCtx, control{Op: "recover"})
	recoverCancel()
	if err != nil {
		return out, err
	}
	s, l, err = samplePair(ctx, server, load, control{Op: "finish", At: recovered.At.Add(time.Second)})
	if err != nil {
		return out, err
	}
	addSample(s, l)
	out.ServerProcess.CapturedAt, out.LoadProcess.CapturedAt = s.At, l.At
	finishProcess(out.ServerProcess, o)
	finishProcess(out.LoadProcess, o)
	out.DurationSeconds = max(out.ServerProcess.DurationSeconds, out.LoadProcess.DurationSeconds)
	out.ProcessCPUSeconds = out.ServerProcess.ProcessCPUSeconds + out.LoadProcess.ProcessCPUSeconds
	out.ProcessCPUPercentOneCore = out.ProcessCPUSeconds / out.DurationSeconds * 100
	out.ProcessCPUPercentHost = out.ProcessCPUPercentOneCore / float64(out.LogicalCPUs)
	out.AllocatedBytes = out.ServerProcess.AllocatedBytes + out.LoadProcess.AllocatedBytes
	out.AllocationBytesPerSecond = float64(out.AllocatedBytes) / out.DurationSeconds
	out.MemoryChecked = out.ServerProcess.MemoryChecked && out.LoadProcess.MemoryChecked
	out.MemoryBounded = out.ServerProcess.MemoryBounded && out.LoadProcess.MemoryBounded
	out.Converged, out.ConnectionsAlive, out.StepBudgetPassed = true, true, true
	if len(s.Views) != o.Rooms || len(s.Sample.Rooms) != o.Rooms || len(l.Clients) != o.Rooms*o.Clients {
		return out, errors.New("incomplete worker results")
	}
	var datagrams, reliable uint64
	for i, view := range s.Views {
		m := s.Sample.Rooms[i]
		rr := RoomReport{Name: fmt.Sprintf("bench-%d", i), Metrics: m, Converged: true}
		out.ConnectionsAlive = out.ConnectionsAlive && m.Players == o.Clients && m.RetainedPlayers == 0
		out.StepBudgetPassed = out.StepBudgetPassed && m.StepP99 < 10*time.Millisecond
		for _, client := range l.Clients[i*o.Clients : (i+1)*o.Clients] {
			rr.Converged = rr.Converged && client.Room == i && converged(view, client)
			rr.Resumes += client.Resumes
			out.ConnectionsAlive = out.ConnectionsAlive && client.Alive
		}
		out.Converged = out.Converged && rr.Converged
		out.Rooms = append(out.Rooms, rr)
		datagrams += m.DatagramBytes - base[i].DatagramBytes
		reliable += m.ReliableBytes - base[i].ReliableBytes
	}
	out.DatagramPayloadBytesPerSecond = float64(datagrams) / out.ServerProcess.DurationSeconds
	out.PerClientPayloadBytesPerSecond = float64(datagrams+reliable) / out.ServerProcess.DurationSeconds / float64(out.Clients)
	assess(&out, o.Duration)
	return out, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func waitChildren(ctx context.Context, server, load *child, at time.Time) error {
	timer := time.NewTimer(max(0, time.Until(at)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-server.done:
		return fmt.Errorf("server worker exited unexpectedly: %v", server.err)
	case <-load.done:
		return fmt.Errorf("load worker exited unexpectedly: %v", load.err)
	case <-timer.C:
		return nil
	}
}

func finishProcess(p *processReport, o isolatedOptions) {
	s := p.Samples[len(p.Samples)-1]
	p.DurationSeconds, p.ProcessCPUSeconds, p.AllocatedBytes = s.Seconds, s.CPUSeconds, s.AllocatedBytes
	p.ProcessCPUPercentOneCore = s.CPUSeconds / s.Seconds * 100
	p.ProcessCPUPercentHost = p.ProcessCPUPercentOneCore / float64(runtime.NumCPU())
	p.AllocationBytesPerSecond = float64(s.AllocatedBytes) / s.Seconds
	p.MemoryChecked, p.MemoryBounded = checkMemory(o.Duration, p.Samples, o.Rooms)
}
