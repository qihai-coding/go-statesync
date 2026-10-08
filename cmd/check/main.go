// Command check validates real QUIC rooms, headless clients and bounded UDP impairment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/qihai-coding/go-statesync/internal/measure"
	"github.com/qihai-coding/go-statesync/internal/nettest"
)

type Sample struct {
	Seconds                float64
	HeapBytes, HeapObjects uint64
	Goroutines             int
	CPUSeconds             float64
	Rooms                  []syncnet.Metrics
}
type RoomReport struct {
	Name      string
	Metrics   syncnet.Metrics
	Converged bool
	Resumes   uint64
}
type Report struct {
	Started                                                                                 time.Time
	DurationSeconds                                                                         float64
	GoVersion, Platform, Processor                                                          string
	LogicalCPUs, Clients, ClientsPerRoom, Items                                             int
	Profile                                                                                 nettest.Profile
	Samples                                                                                 []Sample
	Rooms                                                                                   []RoomReport
	ProcessCPUSeconds, ProcessCPUPercentOneCore, ProcessCPUPercentHost                      float64
	AllocatedBytes                                                                          uint64
	AllocationBytesPerSecond, DatagramPayloadBytesPerSecond, PerClientPayloadBytesPerSecond float64
	Converged, ConnectionsAlive, MemoryChecked, MemoryBounded, StepBudgetPassed, Passed     bool
	Failures                                                                                []string
}
type bot struct {
	room    int
	current atomic.Pointer[syncnet.Client]
	proxy   atomic.Pointer[nettest.Proxy]
	resumes atomic.Uint64
}

func main() {
	duration := flag.Duration("duration", 10*time.Minute, "持续运行时间")
	count := flag.Int("clients", 16, "每房间客户端数量")
	roomCount := flag.Int("rooms", 1, "房间数量")
	resumeEvery := flag.Duration("resume-every", 0, "每房间轮换一个客户端续接的间隔，0 为关闭")
	reportPath := flag.String("report", "reports/phase2/soak.json", "结果文件")
	cpuProfile := flag.String("cpuprofile", "", "可选的处理器采样文件")
	rtt := flag.Duration("rtt", 0, "往返延迟")
	jitter := flag.Duration("jitter", 0, "单向抖动幅度")
	loss := flag.Float64("loss", 0, "丢包概率")
	duplicate := flag.Float64("duplicate", 0, "重复概率")
	reorder := flag.Float64("reorder", 0, "额外乱序概率")
	flag.Parse()
	if *duration < time.Second || *count < 1 || *count > 16 || *roomCount < 1 || *roomCount > 64 || *resumeEvery < 0 {
		fatal(errors.New("时间至少一秒；每房间 1..16 人；房间 1..64；续接间隔非负"))
	}
	var stopProfile func()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fatal(err)
		}
		if err = pprof.StartCPUProfile(f); err != nil {
			f.Close()
			fatal(err)
		}
		stopProfile = func() { pprof.StopCPUProfile(); f.Close() }
	}
	report, err := run(*duration, *count, *roomCount, *resumeEvery, nettest.Profile{RTT: *rtt, Jitter: *jitter, Loss: *loss, Duplicate: *duplicate, Reorder: *reorder})
	if stopProfile != nil {
		stopProfile()
	}
	if err != nil {
		fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(*reportPath), 0755); err != nil {
		fatal(err)
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err = os.WriteFile(*reportPath, append(b, '\n'), 0644); err != nil {
		fatal(err)
	}
	memory := "未执行（需至少十分钟及有效采样）"
	if report.MemoryChecked {
		memory = fmt.Sprintf("通过=%v", report.MemoryBounded)
	}
	fmt.Printf("完成：通过=%v，房间=%d，客户端=%d，进程处理器占用=%.2f%%（单核），每客户端下行=%.0f 字节/秒，长期内存检查=%s，报告=%s\n", report.Passed, len(report.Rooms), report.Clients, report.ProcessCPUPercentOneCore, report.PerClientPayloadBytesPerSecond, memory, *reportPath)
	if !report.Passed {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func run(duration time.Duration, count, roomCount int, resumeEvery time.Duration, profile nettest.Profile) (Report, error) {
	if err := profile.Validate(); err != nil {
		return Report{}, err
	}
	out := Report{Started: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Processor: os.Getenv("PROCESSOR_IDENTIFIER"), LogicalCPUs: runtime.NumCPU(), Clients: count * roomCount, ClientsPerRoom: count, Items: 16 * roomCount, Profile: profile}
	cert, pem, err := syncnet.LocalCertificate()
	if err != nil {
		return out, err
	}
	tc, err := syncnet.ClientTLS(pem, "localhost")
	if err != nil {
		return out, err
	}
	server, err := syncnet.Listen("127.0.0.1:0", syncnet.ServerTLS(cert), syncnet.DefaultConfig())
	if err != nil {
		return out, err
	}
	defer server.Close()
	rooms := make([]*syncnet.Room, roomCount)
	out.Rooms = make([]RoomReport, roomCount)
	for i := range rooms {
		name := fmt.Sprintf("bench-%d", i)
		rooms[i], err = server.CreateRoom(name, arena.New())
		if err != nil {
			return out, err
		}
		out.Rooms[i].Name = name
	}
	bots := make([]*bot, 0, count*roomCount)
	defer func() {
		for _, b := range bots {
			if c := b.current.Load(); c != nil {
				c.Close()
			}
			if p := b.proxy.Load(); p != nil {
				p.Close()
			}
		}
	}()
	var stopped, failed atomic.Bool
	connect := func(b *bot, token syncnet.ResumeToken, seed int64) (*syncnet.Client, error) {
		addr := server.Addr().String()
		if profile != (nettest.Profile{}) {
			if old := b.proxy.Swap(nil); old != nil {
				old.Close()
			}
			p, err := nettest.New(addr, seed, profile)
			if err != nil {
				return nil, err
			}
			b.proxy.Store(p)
			if stopped.Load() {
				p.Set(nettest.Profile{})
			}
			addr = p.Addr()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if token == (syncnet.ResumeToken{}) {
			return syncnet.Dial(ctx, addr, out.Rooms[b.room].Name, tc, arena.Model{})
		}
		return syncnet.DialResume(ctx, addr, out.Rooms[b.room].Name, tc, arena.Model{}, token)
	}
	for i := 0; i < count*roomCount; i++ {
		b := &bot{room: i / count}
		bots = append(bots, b)
		c, err := connect(b, syncnet.ResumeToken{}, int64(100+i))
		if err != nil {
			return out, err
		}
		b.current.Store(c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	workloadStarted := time.Now()
	for i, b := range bots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := b.current.Load()
			player := c.Player()
			ticker := time.NewTicker(time.Second / time.Duration(c.TickRate()))
			defer ticker.Stop()
			nextResume := workloadStarted.Add(resumeEvery * time.Duration(i%count+1))
			step := i * 7
			zero := arena.Move(0, 0)
			moves := [][]byte{arena.Move(1, 0), arena.Move(0, 1), arena.Move(-1, 0), arena.Move(0, -1)}
			for {
				select {
				case <-ctx.Done():
					return
				case <-c.Done():
					failed.Store(true)
					return
				case now := <-ticker.C:
					if !stopped.Load() && resumeEvery > 0 && !now.Before(nextResume) {
						token := c.ResumeToken()
						c.Disconnect()
						next, err := connect(b, token, int64(1000+i)+int64(b.resumes.Load())*10000)
						if err != nil {
							failed.Store(true)
							return
						}
						c = next
						b.current.Store(c)
						if c.Player() != player {
							failed.Store(true)
							return
						}
						b.resumes.Add(1)
						nextResume = nextResume.Add(resumeEvery * time.Duration(count))
					}
					input := zero
					if !stopped.Load() {
						input = moves[(step/60)%4]
					}
					step++
					if err := c.SubmitInput(input); err != nil && !errors.Is(err, syncnet.ErrBusy) {
						failed.Store(true)
						return
					}
				}
			}
		}()
	}
	defer func() { cancel(); wg.Wait() }()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, err := measure.CPUSeconds()
	if err != nil {
		return out, err
	}
	base := make([]syncnet.Metrics, roomCount)
	inspect := func(r *syncnet.Room) (syncnet.View, syncnet.Metrics, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return r.Inspect(ctx)
	}
	for i, r := range rooms {
		_, base[i], err = inspect(r)
		if err != nil {
			return out, err
		}
	}
	start := time.Now()
	sampleNow := func() error {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		cpu, err := measure.CPUSeconds()
		if err != nil {
			return err
		}
		s := Sample{Seconds: time.Since(start).Seconds(), HeapBytes: m.HeapAlloc, HeapObjects: m.HeapObjects, Goroutines: runtime.NumGoroutine(), CPUSeconds: cpu - cpuBefore, Rooms: make([]syncnet.Metrics, roomCount)}
		players, retained := 0, 0
		p99 := time.Duration(0)
		for i, r := range rooms {
			_, s.Rooms[i], err = inspect(r)
			if err != nil {
				return err
			}
			players += s.Rooms[i].Players
			retained += s.Rooms[i].RetainedPlayers
			p99 = max(p99, s.Rooms[i].StepP99)
		}
		out.Samples = append(out.Samples, s)
		fmt.Printf("%.0f 秒：在线=%d，保留=%d，堆=%.2f MiB（兆二进制字节），最慢房间第99百分位=%v\n", s.Seconds, players, retained, float64(m.HeapAlloc)/(1<<20), p99)
		return nil
	}
	sampler := time.NewTicker(min(30*time.Second, duration))
	defer sampler.Stop()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
running:
	for {
		select {
		case <-sampler.C:
			if err := sampleNow(); err != nil {
				return out, err
			}
		case <-deadline.C:
			break running
		}
	}
	stopped.Store(true)
	for _, b := range bots {
		if p := b.proxy.Load(); p != nil {
			p.Set(nettest.Profile{})
		}
	}
	time.Sleep(time.Second)
	out.Converged = true
	out.ConnectionsAlive = !failed.Load()
	out.StepBudgetPassed = true
	var datagrams, reliable uint64
	for i, r := range rooms {
		view, m, err := inspect(r)
		if err != nil {
			return out, err
		}
		rr := &out.Rooms[i]
		rr.Metrics = m
		rr.Converged = true
		if m.Players != count || m.RetainedPlayers != 0 {
			out.ConnectionsAlive = false
		}
		if m.StepP99 >= 10*time.Millisecond {
			out.StepBudgetPassed = false
		}
		datagrams += m.DatagramBytes - base[i].DatagramBytes
		reliable += m.ReliableBytes - base[i].ReliableBytes
		for _, b := range bots[i*count : (i+1)*count] {
			c := b.current.Load()
			rr.Resumes += b.resumes.Load()
			v := c.Authoritative()
			if len(v.Entities) != len(view.Entities) {
				rr.Converged = false
			}
			byID := make(map[uint32]syncnet.Entity, len(v.Entities))
			for _, e := range v.Entities {
				byID[e.ID] = e
			}
			for _, e := range view.Entities {
				got, ok := byID[e.ID]
				if !ok || got.Owner != e.Owner || got.Generation != e.Generation || !bytes.Equal(got.State, e.State) {
					rr.Converged = false
				}
				if e.Owner == c.Player() {
					predicted, ok := c.Sample(e.ID, time.Now())
					if !ok || !bytes.Equal(predicted, e.State) {
						rr.Converged = false
					}
				}
			}
		}
		if !rr.Converged {
			out.Converged = false
		}
	}
	if err := sampleNow(); err != nil {
		return out, err
	}
	out.DurationSeconds = time.Since(start).Seconds()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	cpu, err := measure.CPUSeconds()
	if err != nil {
		return out, err
	}
	out.ProcessCPUSeconds = cpu - cpuBefore
	out.ProcessCPUPercentOneCore = out.ProcessCPUSeconds / out.DurationSeconds * 100
	out.ProcessCPUPercentHost = out.ProcessCPUPercentOneCore / float64(runtime.NumCPU())
	out.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	out.AllocationBytesPerSecond = float64(out.AllocatedBytes) / out.DurationSeconds
	out.DatagramPayloadBytesPerSecond = float64(datagrams) / out.DurationSeconds
	out.PerClientPayloadBytesPerSecond = float64(datagrams+reliable) / out.DurationSeconds / float64(out.Clients)
	out.MemoryChecked, out.MemoryBounded = checkMemory(duration, out.Samples, roomCount)
	if !out.Converged {
		out.Failures = append(out.Failures, "网络恢复一秒后状态未收敛")
	}
	if !out.ConnectionsAlive {
		out.Failures = append(out.Failures, "连接丢失或续接失败")
	}
	if !out.StepBudgetPassed {
		out.Failures = append(out.Failures, "房间单步第99百分位超出10毫秒")
	}
	if duration >= 10*time.Minute && !out.MemoryChecked {
		out.Failures = append(out.Failures, "十分钟内存检查缺少有效采样")
	} else if out.MemoryChecked && !out.MemoryBounded {
		out.Failures = append(out.Failures, "内存或协程增长超出阈值")
	}
	out.Passed = len(out.Failures) == 0
	return out, nil
}

func checkMemory(duration time.Duration, samples []Sample, roomCount int) (checked, bounded bool) {
	if duration < 10*time.Minute || len(samples) < 3 {
		return false, false
	}
	baseline, last := samples[1], samples[len(samples)-1]
	if baseline.Seconds < 60 || last.Seconds < duration.Seconds() || last.Seconds <= baseline.Seconds {
		return false, false
	}
	return true, last.HeapBytes <= baseline.HeapBytes+baseline.HeapBytes/4+4*(1<<20) && last.Goroutines <= baseline.Goroutines+roomCount*16
}
