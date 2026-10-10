// Command check validates real QUIC rooms, headless clients and bounded UDP impairment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/internal/measure"
	"github.com/qihai-coding/go-statesync/internal/nettest"
)

type Sample struct {
	Seconds                float64
	HeapBytes, HeapObjects uint64
	Goroutines             int
	CPUSeconds             float64
	AllocatedBytes         uint64
	Rooms                  []syncnet.Metrics
}
type RoomReport struct {
	Name      string
	Metrics   syncnet.Metrics
	Converged bool
	Resumes   uint64
}
type Report struct {
	Workload, Encoding                                                                      string
	Seed                                                                                    int64
	Entities, StateBytes, DatagramSize                                                      int
	BandwidthScope                                                                          string
	ServerOutboundBytes, ClientUpstreamBytes, TotalApplicationBytes                         uint64
	ExpectedEntityUpdates, SentEntityUpdates, AppliedEntityUpdates, SnapshotDrops           uint64
	TrafficDurationSeconds, ApplicationBytesPerSecond                                       float64
	ClockCalibration                                                                        []roomClock
	EntityAges                                                                              []EntityAge
	DisplayAgeEvaluated, DisplayAgePassed                                                   bool
	WorstEntityDisplayAgeP95Seconds                                                         float64
	RecoverySeconds                                                                         float64
	RecoveryWithoutReliableCorrection                                                       bool
	QueuesBounded                                                                           bool
	Mode                                                                                    string
	ServerProcess, LoadProcess                                                              *processReport `json:",omitempty"`
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

func main() {
	if role := os.Getenv(workerEnv); role != "" {
		if err := workerMain(role); err != nil {
			fatal(err)
		}
		return
	}
	duration := flag.Duration("duration", 10*time.Minute, "持续运行时间")
	count := flag.Int("clients", 16, "每房间客户端数量")
	roomCount := flag.Int("rooms", 1, "房间数量")
	resumeEvery := flag.Duration("resume-every", 0, "每房间轮换一个客户端续接的间隔，0 为关闭")
	reportPath := flag.String("report", "reports/local/soak.json", "结果文件")
	isolate := flag.Bool("isolate", false, "分开运行服务器与客户端进程")
	cpuProfile := flag.String("cpuprofile", "", "可选的处理器采样文件")
	rtt := flag.Duration("rtt", 0, "往返延迟")
	jitter := flag.Duration("jitter", 0, "单向抖动幅度")
	loss := flag.Float64("loss", 0, "丢包概率")
	duplicate := flag.Float64("duplicate", 0, "重复概率")
	reorder := flag.Float64("reorder", 0, "额外乱序概率")
	options := workloadOptions{}
	flag.StringVar(&options.Workload, "workload", "arena", "负载 arena（演示）/motion（运动）/entropy（高熵）")
	flag.StringVar(&options.Encoding, "encoding", "delta", "快照编码 delta（差量）/full（完整）")
	flag.IntVar(&options.Entities, "entities", 256, "运动负载动态实体总数，包含玩家")
	flag.IntVar(&options.StateBytes, "state-bytes", 32, "运动负载每实体状态字节数 32..512")
	flag.IntVar(&options.DatagramSize, "datagram-size", 1000, "数据报应用负载上限 600..1000 字节")
	flag.Int64Var(&options.Seed, "seed", 7, "弱网随机种子")
	flag.Parse()
	if err := options.validate(*count); err != nil {
		fatal(err)
	}
	if *duration < time.Second || *count < 1 || *count > 16 || *roomCount < 1 || *roomCount > 64 || *resumeEvery < 0 {
		fatal(errors.New("时间至少一秒；每房间 1..16 人；房间 1..64；续接间隔非负"))
	}
	var stopProfile func()
	if *cpuProfile != "" && !*isolate {
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
	profile := nettest.Profile{RTT: *rtt, Jitter: *jitter, Loss: *loss, Duplicate: *duplicate, Reorder: *reorder}
	o := isolatedOptions{Duration: *duration, ResumeEvery: *resumeEvery, Clients: *count, Rooms: *roomCount, Profile: profile, CPUProfile: *cpuProfile, workloadOptions: options}
	if err := o.validate(); err != nil {
		fatal(err)
	}
	var report Report
	var err error
	if *isolate {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		report, err = runIsolated(ctx, o)
		cancel()
	} else {
		report, err = runConfigured(o)
	}
	if stopProfile != nil {
		stopProfile()
	}
	if err != nil {
		report.Passed = false
		report.Failures = append(report.Failures, err.Error())
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
	return runConfigured(isolatedOptions{Duration: duration, Clients: count, Rooms: roomCount, ResumeEvery: resumeEvery, Profile: profile})
}

func runConfigured(o isolatedOptions) (Report, error) {
	o.workloadOptions = o.workloadOptions.defaults()
	duration, count, roomCount, resumeEvery, profile := o.Duration, o.Clients, o.Rooms, o.ResumeEvery, o.Profile
	if err := o.validate(); err != nil {
		return Report{}, err
	}
	out := Report{Mode: "in-process", Started: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Processor: os.Getenv("PROCESSOR_IDENTIFIER"), LogicalCPUs: runtime.NumCPU(), Clients: count * roomCount, ClientsPerRoom: count, Items: 16 * roomCount, Profile: profile, Workload: o.Workload, Encoding: o.Encoding, Entities: o.Entities, StateBytes: o.StateBytes, Seed: o.Seed, DatagramSize: o.DatagramSize, QueuesBounded: true}
	if o.Workload == "arena" {
		out.Entities = count + 16
		out.StateBytes = 13
	} else {
		out.Items = 0
	}
	cert, pem, err := syncnet.LocalCertificate()
	if err != nil {
		return out, err
	}
	tc, err := syncnet.ClientTLS(pem, "localhost")
	if err != nil {
		return out, err
	}
	server, err := syncnet.Listen("127.0.0.1:0", syncnet.ServerTLS(cert), o.config())
	if err != nil {
		return out, err
	}
	defer server.Close()
	rooms := make([]*syncnet.Room, roomCount)
	out.Rooms = make([]RoomReport, roomCount)
	for i := range rooms {
		name := fmt.Sprintf("bench-%d", i)
		game, gameErr := o.game(count)
		if gameErr != nil {
			return out, gameErr
		}
		rooms[i], err = server.CreateRoom(name, game)
		if err != nil {
			return out, err
		}
		out.Rooms[i].Name = name
	}
	names := make([]string, len(rooms))
	for i := range rooms {
		names[i] = out.Rooms[i].Name
	}
	load, err := newLoad(context.Background(), server.Addr().String(), names, tc, count, resumeEvery, profile, o.workloadOptions)
	if err != nil {
		return out, err
	}
	defer load.close()
	load.clocks, err = calibrateRooms(context.Background(), rooms)
	if err != nil {
		return out, err
	}
	load.start(time.Now())
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
		s := Sample{Seconds: time.Since(start).Seconds(), HeapBytes: m.HeapAlloc, HeapObjects: m.HeapObjects, Goroutines: runtime.NumGoroutine(), CPUSeconds: cpu - cpuBefore, AllocatedBytes: m.TotalAlloc - before.TotalAlloc, Rooms: make([]syncnet.Metrics, roomCount)}
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
		out.QueuesBounded = out.QueuesBounded && queuesBounded(s.Rooms, count, out.Entities, out.StateBytes)
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
	load.recoverNetwork()
	var views []syncnet.View
	var recoveryStates []clientState
	if o.Workload != "arena" {
		recoverCtx, cancel := context.WithDeadline(context.Background(), load.recoveryAt.Add(time.Second))
		err = load.stopGames(recoverCtx)
		if err == nil {
			views, _, err = inspectRooms(recoverCtx, rooms)
		}
		if err != nil {
			cancel()
			return out, err
		}
		var elapsed time.Duration
		recoveryStates, elapsed, out.Converged, out.RecoveryWithoutReliableCorrection = load.awaitConvergence(recoverCtx, views)
		cancel()
		out.RecoverySeconds = elapsed.Seconds()
	} else {
		time.Sleep(time.Second)
		out.Converged = true
		out.RecoveryWithoutReliableCorrection = true
	}
	states := load.states()
	if recoveryStates != nil {
		states = recoveryStates
	}
	out.ConnectionsAlive = true
	out.StepBudgetPassed = true
	var datagrams, reliable uint64
	views = make([]syncnet.View, len(rooms))
	for i, r := range rooms {
		view, m, err := inspect(r)
		if err != nil {
			return out, err
		}
		rr := &out.Rooms[i]
		views[i] = view
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
		for _, state := range states[i*count : (i+1)*count] {
			rr.Resumes += state.Resumes
			rr.Converged = rr.Converged && converged(view, state)
			out.ConnectionsAlive = out.ConnectionsAlive && state.Alive
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
	reportAges(&out, load.ageReports(), load.clocks, duration)
	reportTraffic(&out, states)
	checkClientBounds(&out, states)
	assess(&out, duration)
	return out, nil
}

func assess(out *Report, duration time.Duration) {
	if !out.Converged {
		out.Failures = append(out.Failures, "网络恢复一秒后状态未收敛")
	}
	if out.DisplayAgeEvaluated && !out.DisplayAgePassed {
		out.Failures = append(out.Failures, "远端实体显示年龄第95百分位超过一秒或存在缺失采样")
	}
	if out.Workload != "" && out.Workload != "arena" && !out.RecoveryWithoutReliableCorrection {
		out.Failures = append(out.Failures, "恢复期间依赖可靠实体变更或完整重同步")
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
	if !out.QueuesBounded {
		out.Failures = append(out.Failures, "队列或基线内存超出硬上限")
	}
	out.Passed = len(out.Failures) == 0
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
