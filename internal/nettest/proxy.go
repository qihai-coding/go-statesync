// Package nettest provides a bounded, seeded UDP impairment proxy.
// It corrupts network delivery, not decoded application messages, so QUIC itself
// is exercised under handshake loss, retransmission, jitter and reordering.
package nettest

import (
	"container/heap"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Profile struct {
	RTT, Jitter              time.Duration
	Loss, Duplicate, Reorder float64
}

func (v Profile) Validate() error {
	// Positive range checks also reject NaN, whose comparisons are all false.
	if v.RTT < 0 || v.Jitter < 0 || v.RTT > 10*time.Second || v.Jitter > time.Second ||
		!(v.Loss >= 0 && v.Loss <= 1 && v.Duplicate >= 0 && v.Duplicate <= 1 && v.Reorder >= 0 && v.Reorder <= 1) {
		return errors.New("invalid impairment profile")
	}
	return nil
}

type packet struct {
	at   time.Time
	data []byte
	to   *net.UDPAddr
	up   bool
}
type packets []packet

func (p packets) Len() int           { return len(p) }
func (p packets) Less(i, j int) bool { return p[i].at.Before(p[j].at) }
func (p packets) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *packets) Push(v any)        { *p = append(*p, v.(packet)) }
func (p *packets) Pop() any {
	old := *p
	n := len(old)
	v := old[n-1]
	old[n-1] = packet{}
	*p = old[:n-1]
	return v
}

type Proxy struct {
	front, back *net.UDPConn
	mu          sync.Mutex
	profile     Profile
	random      *rand.Rand
	client      *net.UDPAddr
	queue       chan packet
	done        chan struct{}
	once        sync.Once
	wg          sync.WaitGroup
	drops       atomic.Uint64
}

func New(target string, seed int64, profile Profile) (*Proxy, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	dst, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, err
	}
	back, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		front.Close()
		return nil, err
	}
	p := &Proxy{front: front, back: back, profile: profile, random: rand.New(rand.NewSource(seed)), queue: make(chan packet, 512), done: make(chan struct{})}
	p.wg.Add(3)
	go p.read(true)
	go p.read(false)
	go p.send()
	return p, nil
}
func (p *Proxy) Addr() string { return p.front.LocalAddr().String() }
func (p *Proxy) Set(v Profile) error {
	if err := v.Validate(); err != nil {
		return err
	}
	p.mu.Lock()
	p.profile = v
	p.mu.Unlock()
	return nil
}
func (p *Proxy) Close() {
	p.once.Do(func() { close(p.done); p.front.Close(); p.back.Close() })
	p.wg.Wait()
}
func (p *Proxy) Drops() uint64 { return p.drops.Load() }
func (p *Proxy) read(up bool) {
	defer p.wg.Done()
	buf := make([]byte, 65535)
	for {
		var n int
		var addr *net.UDPAddr
		var err error
		if up {
			n, addr, err = p.front.ReadFromUDP(buf)
		} else {
			n, err = p.back.Read(buf)
		}
		if err != nil {
			return
		}
		p.mu.Lock()
		if up {
			if p.client != nil && p.client.String() != addr.String() {
				p.mu.Unlock()
				continue
			}
			p.client = addr
		}
		profile := p.profile
		to := p.client
		if to == nil || p.random.Float64() < profile.Loss {
			p.mu.Unlock()
			p.drops.Add(1)
			continue
		}
		delay := profile.RTT / 2
		if profile.Jitter > 0 {
			delay += time.Duration(p.random.Int63n(int64(profile.Jitter)*2+1)) - profile.Jitter
		}
		if p.random.Float64() < profile.Reorder {
			delay += 100 * time.Millisecond
		}
		duplicate := p.random.Float64() < profile.Duplicate
		p.mu.Unlock()
		v := packet{at: time.Now().Add(max(delay, 0)), data: append([]byte(nil), buf[:n]...), to: to, up: up}
		p.enqueue(v)
		if duplicate {
			v.at = v.at.Add(time.Millisecond)
			p.enqueue(v)
		}
	}
}
func (p *Proxy) enqueue(v packet) {
	select {
	case p.queue <- v:
	case <-p.done:
	default:
		p.drops.Add(1)
	}
}
func (p *Proxy) send() {
	defer p.wg.Done()
	q := make(packets, 0, 512)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		delay := time.Hour
		if len(q) > 0 {
			delay = max(time.Until(q[0].at), time.Nanosecond)
		}
		timer.Reset(delay)
		select {
		case <-p.done:
			return
		case v := <-p.queue:
			if len(q) < 512 {
				heap.Push(&q, v)
			} else {
				p.drops.Add(1)
			}
		case <-timer.C:
			for len(q) > 0 && !time.Now().Before(q[0].at) {
				v := heap.Pop(&q).(packet)
				if v.up {
					p.back.Write(v.data)
				} else {
					p.front.WriteToUDP(v.data, v.to)
				}
			}
		}
	}
}
