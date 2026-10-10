package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/qihai-coding/go-statesync/internal/loadgame"
	"github.com/qihai-coding/go-statesync/internal/measure"
	"github.com/qihai-coding/go-statesync/internal/nettest"
)

type bot struct {
	room                                                     int
	current                                                  atomic.Pointer[syncnet.Client]
	proxy                                                    atomic.Pointer[nettest.Proxy]
	resumes                                                  atomic.Uint64
	retiredSent, retiredUpdates, retiredChanges, retiredFull atomic.Uint64
	local                                                    uint32
	ages                                                     []ageHistogram
	ageMu                                                    sync.Mutex
}

type load struct {
	bots            []*bot
	addr            string
	rooms           []string
	tls             *tls.Config
	count           int
	resumeEvery     time.Duration
	profile         nettest.Profile
	stopped, failed atomic.Bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	ops             sync.RWMutex
	options         workloadOptions
	model           syncnet.Model
	clocks          []roomClock
	started         time.Time
	recoveryStarted time.Duration
	recoveryAt      time.Time
	guards          []clientState
	trafficStarted  time.Duration
}

type clientState struct {
	Room                                             int
	View                                             syncnet.View
	Predicted                                        []byte
	Alive                                            bool
	Resumes                                          uint64
	Rendered                                         map[uint32][]byte `json:",omitempty"`
	Sent, EntityUpdates, ReliableChanges, FullStates uint64
	TrafficDurationSeconds                           float64
	BaselineEntities, BaselineBytes                  int
}

func newLoad(ctx context.Context, addr string, rooms []string, tc *tls.Config, count int, resumeEvery time.Duration, profile nettest.Profile, options workloadOptions) (*load, error) {
	l := &load{addr: addr, rooms: rooms, tls: tc, count: count, resumeEvery: resumeEvery, profile: profile, options: options.defaults()}
	l.model = l.options.model()
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.trafficStarted = measure.Now()
	for i := 0; i < count*len(rooms); i++ {
		b := &bot{room: i / count}
		l.bots = append(l.bots, b)
		c, err := l.connect(b, syncnet.ResumeToken{}, l.options.Seed+int64(i))
		if err != nil {
			l.close()
			return nil, err
		}
		b.current.Store(c)
		for _, e := range c.Authoritative().Entities {
			if e.Owner == c.Player() {
				b.local = e.ID
			}
		}
		if l.options.Workload != "arena" {
			b.ages = make([]ageHistogram, l.options.Entities)
		}
	}
	return l, nil
}

func (l *load) connect(b *bot, token syncnet.ResumeToken, seed int64) (*syncnet.Client, error) {
	addr := l.addr
	if l.profile != (nettest.Profile{}) {
		if old := b.proxy.Swap(nil); old != nil {
			old.Close()
		}
		p, err := nettest.New(addr, seed, l.profile)
		if err != nil {
			return nil, err
		}
		b.proxy.Store(p)
		addr = p.Addr()
	}
	ctx, cancel := context.WithTimeout(l.ctx, 10*time.Second)
	defer cancel()
	if token == (syncnet.ResumeToken{}) {
		return syncnet.Dial(ctx, addr, l.rooms[b.room], l.tls, l.model)
	}
	return syncnet.DialResume(ctx, addr, l.rooms[b.room], l.tls, l.model, token)
}

func (l *load) start(at time.Time) {
	l.started = at
	for i, b := range l.bots {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			c := b.current.Load()
			player := c.Player()
			ticker := time.NewTicker(time.Second / time.Duration(c.TickRate()))
			defer ticker.Stop()
			nextResume := at.Add(l.resumeEvery * time.Duration(i%l.count+1))
			step := i * 7
			zero := arena.Move(0, 0)
			moves := [][]byte{arena.Move(1, 0), arena.Move(0, 1), arena.Move(-1, 0), arena.Move(0, -1)}
			for {
				select {
				case <-l.ctx.Done():
					return
				case <-c.Done():
					l.failed.Store(true)
					return
				case now := <-ticker.C:
					l.ops.RLock()
					if !l.stopped.Load() && l.resumeEvery > 0 && !now.Before(nextResume) {
						token := c.ResumeToken()
						c.Disconnect()
						b.archive(c)
						next, err := l.connect(b, token, l.options.Seed+int64(1000+i)+int64(b.resumes.Load())*10000)
						if err != nil {
							l.failed.Store(true)
							l.ops.RUnlock()
							return
						}
						c = next
						b.current.Store(c)
						if c.Player() != player {
							l.failed.Store(true)
							l.ops.RUnlock()
							return
						}
						b.resumes.Add(1)
						nextResume = nextResume.Add(l.resumeEvery * time.Duration(l.count))
					}
					input := zero
					if !l.stopped.Load() {
						input = moves[(step/60)%4]
					}
					step++
					err := c.SubmitInput(input)
					l.ops.RUnlock()
					if err != nil && !errors.Is(err, syncnet.ErrBusy) {
						l.failed.Store(true)
						return
					}
					if !l.stopped.Load() && len(b.ages) > 0 && time.Since(l.started) >= 5*time.Second {
						l.observe(b, c, time.Now())
					}
				}
			}
		}()
	}
}

func (l *load) recoverNetwork() {
	// The recovery boundary must follow every in-flight movement or resume.
	l.ops.Lock()
	defer l.ops.Unlock()
	l.stopped.Store(true)
	l.guards = l.states()
	l.recoveryAt = time.Now()
	l.recoveryStarted = measure.Now()
	for _, b := range l.bots {
		if p := b.proxy.Load(); p != nil {
			p.Set(nettest.Profile{})
		}
	}
}

func (b *bot) archive(c *syncnet.Client) {
	sent, _ := c.Bytes()
	stats := c.Stats()
	b.retiredSent.Add(sent)
	b.retiredUpdates.Add(stats.EntityUpdates)
	b.retiredChanges.Add(stats.ReliableChanges)
	b.retiredFull.Add(stats.FullStates)
}

func (l *load) observe(b *bot, c *syncnet.Client, now time.Time) {
	b.ageMu.Lock()
	defer b.ageMu.Unlock()
	for id := uint32(1); id <= uint32(l.options.Entities); id++ {
		if id == b.local {
			continue
		}
		state, info, ok := c.SampleWithInfo(id, now)
		h := &b.ages[id-1]
		age := measure.Now() - l.clocks[b.room].Origin - info.SourceTime
		if !ok {
			age = 11 * time.Second
			h.missing++
		} else if l.model.ValidateState(state) != nil {
			l.failed.Store(true)
		}
		h.add(age)
		if info.Holding || !ok {
			h.held++
		}
		if h.lastSource != 0 {
			h.sourceUpdateMax = max(h.sourceUpdateMax, info.LatestStateTime-h.lastSource)
		}
		h.lastSource = info.LatestStateTime
	}
}

func (l *load) ageReports() []EntityAge {
	var ages []EntityAge
	for _, b := range l.bots {
		b.ageMu.Lock()
		for id, h := range b.ages {
			if uint32(id+1) == b.local {
				continue
			}
			ages = append(ages, EntityAge{Room: b.room, Player: b.current.Load().Player(), Entity: uint32(id + 1), Samples: h.n, HeldSamples: h.held, MissingSamples: h.missing, P95Seconds: h.p95(), MaxSeconds: h.maximum.Seconds(), SourceUpdateMaxSeconds: h.sourceUpdateMax.Seconds()})
		}
		b.ageMu.Unlock()
	}
	return ages
}

func (l *load) stopGames(ctx context.Context) error {
	if l.options.Workload == "arena" {
		return nil
	}
	for room := range l.rooms {
		c := l.bots[room*l.count].current.Load()
		if _, err := c.Action(ctx, c.LastActionID()+1, loadgame.Stop()); err != nil {
			return err
		}
	}
	return nil
}

func (l *load) awaitConvergence(ctx context.Context, views []syncnet.View) ([]clientState, time.Duration, bool, bool) {
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	deadline := l.recoveryStarted + time.Second
	var states []clientState
	for {
		states = l.states()
		matched, clean := true, true
		for i, state := range states {
			matched = matched && state.Room < len(views) && converged(views[state.Room], state)
			clean = clean && state.ReliableChanges == l.guards[i].ReliableChanges && state.FullStates == l.guards[i].FullStates
		}
		elapsed := measure.Now() - l.recoveryStarted
		if matched || measure.Now() > deadline {
			return states, elapsed, matched && elapsed <= time.Second, clean
		}
		select {
		case <-ctx.Done():
			return states, elapsed, false, clean
		case <-timer.C:
		}
	}
}

func (l *load) states() []clientState {
	out := make([]clientState, len(l.bots))
	for i, b := range l.bots {
		c := b.current.Load()
		s := clientState{Room: b.room, View: c.Authoritative(), Resumes: b.resumes.Load(), Alive: !l.failed.Load()}
		sent, _ := c.Bytes()
		stats := c.Stats()
		s.Sent = sent + b.retiredSent.Load()
		s.EntityUpdates = stats.EntityUpdates + b.retiredUpdates.Load()
		s.ReliableChanges = stats.ReliableChanges + b.retiredChanges.Load()
		s.FullStates = stats.FullStates + b.retiredFull.Load()
		s.BaselineEntities, s.BaselineBytes = stats.BaselineEntities, stats.BaselineBytes
		s.TrafficDurationSeconds = (measure.Now() - l.trafficStarted).Seconds()
		if l.options.Workload != "arena" {
			s.Rendered = make(map[uint32][]byte, len(s.View.Entities))
		}
		select {
		case <-c.Done():
			s.Alive = false
		default:
		}
		for _, e := range s.View.Entities {
			if e.Owner == s.View.Player {
				s.Predicted, _ = c.Sample(e.ID, time.Now())
			} else if s.Rendered != nil {
				s.Rendered[e.ID], _ = c.Sample(e.ID, time.Now())
			}
		}
		out[i] = s
	}
	return out
}

func (l *load) close() {
	l.cancel()
	l.wg.Wait()
	for _, b := range l.bots {
		if c := b.current.Load(); c != nil {
			c.Close()
		}
		if p := b.proxy.Load(); p != nil {
			p.Close()
		}
	}
}

func converged(view syncnet.View, client clientState) bool {
	if len(client.View.Entities) != len(view.Entities) {
		return false
	}
	byID := make(map[uint32]syncnet.Entity, len(client.View.Entities))
	for _, e := range client.View.Entities {
		byID[e.ID] = e
	}
	local := false
	for _, e := range view.Entities {
		got, ok := byID[e.ID]
		if !ok || got.Owner != e.Owner || got.Generation != e.Generation || got.Dynamic != e.Dynamic || !bytes.Equal(got.State, e.State) {
			return false
		}
		if e.Owner == client.View.Player {
			local = true
			if !bytes.Equal(client.Predicted, e.State) {
				return false
			}
		} else if client.Rendered != nil && !bytes.Equal(client.Rendered[e.ID], e.State) {
			return false
		}
	}
	return local
}
