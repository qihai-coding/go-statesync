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
	"github.com/qihai-coding/go-statesync/internal/nettest"
)

type bot struct {
	room    int
	current atomic.Pointer[syncnet.Client]
	proxy   atomic.Pointer[nettest.Proxy]
	resumes atomic.Uint64
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
}

type clientState struct {
	Room      int
	View      syncnet.View
	Predicted []byte
	Alive     bool
	Resumes   uint64
}

func newLoad(ctx context.Context, addr string, rooms []string, tc *tls.Config, count int, resumeEvery time.Duration, profile nettest.Profile) (*load, error) {
	l := &load{addr: addr, rooms: rooms, tls: tc, count: count, resumeEvery: resumeEvery, profile: profile}
	l.ctx, l.cancel = context.WithCancel(ctx)
	for i := 0; i < count*len(rooms); i++ {
		b := &bot{room: i / count}
		l.bots = append(l.bots, b)
		c, err := l.connect(b, syncnet.ResumeToken{}, int64(100+i))
		if err != nil {
			l.close()
			return nil, err
		}
		b.current.Store(c)
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
		return syncnet.Dial(ctx, addr, l.rooms[b.room], l.tls, arena.Model{})
	}
	return syncnet.DialResume(ctx, addr, l.rooms[b.room], l.tls, arena.Model{}, token)
}

func (l *load) start(at time.Time) {
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
						next, err := l.connect(b, token, int64(1000+i)+int64(b.resumes.Load())*10000)
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
	for _, b := range l.bots {
		if p := b.proxy.Load(); p != nil {
			p.Set(nettest.Profile{})
		}
	}
}

func (l *load) states() []clientState {
	out := make([]clientState, len(l.bots))
	for i, b := range l.bots {
		c := b.current.Load()
		s := clientState{Room: b.room, View: c.Authoritative(), Resumes: b.resumes.Load(), Alive: !l.failed.Load()}
		select {
		case <-c.Done():
			s.Alive = false
		default:
		}
		for _, e := range s.View.Entities {
			if e.Owner == s.View.Player {
				s.Predicted, _ = c.Sample(e.ID, time.Now())
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
		}
	}
	return local
}
