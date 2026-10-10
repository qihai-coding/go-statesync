package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:7777", "监听地址")
	certFile := flag.String("cert", "certs/server.pem", "服务器证书")
	keyFile := flag.String("key", "certs/server.key", "服务器私钥")
	rooms := flag.String("rooms", "alpha,beta", "逗号分隔的房间名称")
	generate := flag.Bool("generate-cert", false, "生成本地演示证书后退出")
	cfg := syncnet.DefaultConfig()
	flag.IntVar(&cfg.TickRate, "tick", 30, "每秒逻辑步数")
	flag.IntVar(&cfg.SnapshotRate, "snapshots", 15, "每秒快照数")
	flag.IntVar(&cfg.MaxPlayers, "players", 16, "每房间人数上限")
	flag.DurationVar(&cfg.ResumeGracePeriod, "resume-grace", time.Minute, "断线会话保留时间，0 为关闭续接")
	encoding := flag.String("encoding", "delta", "快照编码 delta（差量）/full（完整）")
	flag.IntVar(&cfg.DatagramSize, "datagram-size", 1000, "数据报应用负载上限 600..1000 字节")
	flag.Parse()
	if *encoding != "delta" && *encoding != "full" {
		log.Fatal("encoding 必须为 delta 或 full")
	}
	if *encoding == "full" {
		cfg.SnapshotEncoding = syncnet.FullSnapshots
	}
	if *generate {
		for _, f := range []string{*certFile, *keyFile} {
			if err := os.MkdirAll(filepath.Dir(f), 0700); err != nil {
				log.Fatal(err)
			}
		}
		if err := syncnet.WriteLocalCertificate(*certFile, *keyFile); err != nil {
			log.Fatal(err)
		}
		log.Print("本地证书已生成，有效期 24 小时")
		return
	}
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatal(err)
	}
	server, err := syncnet.Listen(*addr, syncnet.ServerTLS(cert), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	for _, name := range strings.Split(*rooms, ",") {
		if _, err := server.CreateRoom(name, arena.New()); err != nil {
			log.Fatal(err)
		}
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{"地址": server.Addr().String(), "房间": *rooms, "逻辑频率": cfg.TickRate, "快照频率": cfg.SnapshotRate, "快照编码": *encoding})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
