package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
)

func terminal(err error) bool {
	var certificate *tls.CertificateVerificationError
	return errors.Is(err, syncnet.ErrResumeRejected) || errors.Is(err, syncnet.ErrSessionReplaced) || errors.Is(err, syncnet.ErrProtocol) || errors.Is(err, syncnet.ErrClosed) || errors.As(err, &certificate)
}
func reconnect(ctx context.Context, old *syncnet.Client, addr, room string, tc *tls.Config) (*syncnet.Client, error) {
	if terminal(old.Err()) {
		return nil, old.Err()
	}
	if old.ResumeGracePeriod() == 0 {
		return nil, syncnet.ErrResumeRejected
	}
	budget, cancel := context.WithTimeout(ctx, old.ResumeGracePeriod())
	defer cancel()
	delay := 250 * time.Millisecond
	for {
		timer := time.NewTimer(delay)
		select {
		case <-budget.Done():
			timer.Stop()
			return nil, budget.Err()
		case <-timer.C:
		}
		attempt, stop := context.WithTimeout(budget, 5*time.Second)
		next, err := syncnet.DialResume(attempt, addr, room, tc, arena.Model{}, old.ResumeToken())
		stop()
		if err == nil {
			return next, nil
		}
		if terminal(err) {
			return nil, err
		}
		delay = min(2*time.Second, delay*2)
	}
}
func main() {
	addr := flag.String("server", "127.0.0.1:7777", "服务器地址")
	room := flag.String("room", "alpha", "房间")
	ca := flag.String("ca", "certs/server.pem", "信任的证书")
	name := flag.String("server-name", "localhost", "证书主机名")
	duration := flag.Duration("duration", 10*time.Second, "运行时间")
	x := flag.Float64("x", 0, "横向输入 -1..1")
	y := flag.Float64("y", 0, "纵向输入 -1..1")
	pickup := flag.Uint("pickup", 0, "尝试拾取的道具编号")
	disconnectAfter := flag.Duration("disconnect-after", 0, "演示用：主动临时断开一次，0 为关闭")
	flag.Parse()
	pem, err := os.ReadFile(*ca)
	if err != nil {
		log.Fatal(err)
	}
	tc, err := syncnet.ClientTLS(pem, *name)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	c, err := syncnet.Dial(ctx, *addr, *room, tc, arena.Model{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { c.Close() }()
	pending := *pickup > 0
	tryPickup := func() {
		if !pending {
			return
		}
		_, err := c.Action(ctx, 1, arena.Pickup(uint32(*pickup), 1))
		select {
		case <-c.Done():
			return
		default:
		}
		log.Printf("拾取结果：%v", err)
		pending = false
	}
	tryPickup()
	ticker := time.NewTicker(time.Second / time.Duration(c.TickRate()))
	defer ticker.Stop()
	printTimer := time.NewTicker(time.Second)
	defer printTimer.Stop()
	var disconnect <-chan time.Time
	if *disconnectAfter > 0 {
		timer := time.NewTimer(*disconnectAfter)
		defer timer.Stop()
		disconnect = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-disconnect:
			c.Disconnect()
			disconnect = nil
		case <-c.Done():
			next, err := reconnect(ctx, c, *addr, *room, tc)
			if err != nil {
				log.Printf("续接停止：%v", err)
				return
			}
			c = next
			log.Printf("续接成功：玩家=%d，最近操作=%d", c.Player(), c.LastActionID())
			tryPickup()
		case <-ticker.C:
			if err := c.SubmitInput(arena.Move(float32(*x), float32(*y))); err != nil && !errors.Is(err, syncnet.ErrBusy) && !errors.Is(err, syncnet.ErrClosed) {
				log.Printf("输入失败：%v", err)
				return
			}
		case <-printTimer.C:
			v := c.Authoritative()
			for _, e := range v.Entities {
				if e.Owner == c.Player() {
					b, _ := c.Sample(e.ID, time.Now())
					state, _ := arena.Decode(b)
					json.NewEncoder(os.Stdout).Encode(map[string]any{"玩家": c.Player(), "服务器帧": v.Tick, "本地预测": state, "已确认输入": e.Ack})
				}
			}
		}
	}
}
