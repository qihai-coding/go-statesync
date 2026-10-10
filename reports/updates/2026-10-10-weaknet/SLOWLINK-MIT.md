# 最终 MIT 控制器慢链路自检（非正式）

本次 20 秒自检通过，使用交付源码中的 `internal/bbr`，与最终服务端导入同一份控制器源码、同一构造器和 `ProfileStandard` 参数。没有用 GPL 候选代替最终证据；旧 `slowlink-bbr-standard-8kib-20s*.json`、旧二进制及 [旧候选说明](SLOWLINK.md) 全部保留，仅代表历史 sing-quic 候选，两者结果不能等价。

控制器固定源为 Hysteria `ca8fbd874fd6ca82413948de6dc5924405e103e1` 的 MIT 移植，保留 Google QUICHE 的 BSD 许可；适配范围和完整文本见 [控制器说明](../../../internal/bbr/README.md)。永久 64 KiB/s 的 pacing（发送节奏）下限和算法参数原样保留，没有为本实验调整。底层是 SagerNet/quic-go `v0.61.0-sing-box-mod.9` 的正式模块版本。

实验模块路径为 `github.com/qihai-coding/go-statesync/weaknet-slowlink`，以满足 Go 的 internal（内部包）导入规则；唯一 `replace`（模块替换）是当前项目 `github.com/qihai-coding/go-statesync => ../..`。没有替换 QUIC、修改模块缓存中的库源码或根 `go.mod`。构建模块列表和二进制依赖都没有 `sing-quic` 或 `sagernet/sing`。

## 固定场景与断言

沿用旧第二版实验代码：一条真实 QUIC 连接，服务端接受后安装 `NewBbrSender(DefaultClock{}, c.InitialPacketSize(), ProfileStandard)`；服务端持续满负荷提交 1000 字节 DATAGRAM（数据报），客户端持续接收。两端 TLS（传输层安全）正常校验证书。

代理上下行分别固定延迟 25 ms，基础 RTT（往返时延）50 ms；下行共用 8192 B/s 的串行字节预算，有限队列 8192 字节，包含服务中的包。负载排队会增加总 RTT。代理入口计数在入队/丢弃之前，反映服务器真实 UDP（用户数据报协议）发送量，不能被转发限速的上限自动保证。

20 秒运行按秒保存完整累计值；末 10 秒差值按真实时间计算速率。预先规定的断言未改：提交量和代理入口均须不超过 12288 B/s（瓶颈的 1.5 倍），有效接收不低于 3276.8 B/s；后期队列均值/末值增长不超过 3000 字节；后两个 5 秒实际在途峰值的增长不超过四个初始包（5120 字节）；上行队列零丢弃；连接持续存活，关闭后两个应用任务及四个代理 I/O（输入输出）任务退出。

qlog（QUIC 事件日志）省略部分未改变或零字段，所以只取实际正 `BytesInFlight`（在途字节）事件的每秒峰值，不把 CWND（拥塞窗口容量）当实际在途量，也不推算平均在途量。原峰值计量单元测试再次通过。

## 实测结果

| 指标 | 最终 MIT 实测 |
| --- | ---: |
| 末 10 秒应用提交，B/s | 9699.85 |
| 末 10 秒代理入口，B/s | 10216.54 |
| 末 10 秒实际转发，B/s | 8224.97 |
| 末 10 秒接收载荷，B/s | 7899.88 |
| 队列均值 10–15 / 15–20 秒，字节 | 7246.6 / 6955.2 |
| 队列最大 / 末值，字节 | 8181 / 7154 |
| 下行丢弃 / 上行丢弃，包 | 164 / 0 |
| 拥塞窗口最小 / 最大 / 末值，字节 | 5720 / 63727 / 13790 |
| 在途峰值 10–15 / 15–20 秒，字节 | 10220 / 14727 |
| 在途后窗口增加 / 允许增加，字节 | 4507 / 5120 |
| 全程 / 最后 1 秒在途峰值，字节 | 64392 / 14308 |
| 连接存活 / 自有任务退出 / 退出码 | 是 / 是 / 0 |

此场景下，末期提交和真实服务器 UDP 发送已经落在瓶颈附近的预置界限内，64 KiB/s 节奏下限没有强制实际持续发送该速率。启动过冲、持续下行丢包和后期在途峰值波动仍存在；后期峰值增加 4507 字节，不能表述为在途量持续下降。十秒有限窗口的转发计量受包边界影响可略高于配置速率，本次约 0.4%。

关闭 100 ms 后总 goroutine（Go 并发任务）数为 3，运行前为 1；已证明退出的是自有任务及实验进程，不能将该数字当作库后台任务零泄漏证明。该单连接、单带宽、20 秒实验不代替正式弱网矩阵、长时运行、16 人状态同步或公平性验收。

## 冻结关联与复现

二进制 `bin/check-slowlink-mit-lab.exe` 的 SHA-256（文件摘要）为 `02C20CCB5BD784E955023C410054DEE28A87CD27390FCA0C294F43C78A7D3F2A`。构建前记录根 `go.mod`、`go.sum` 和 51 个项目 Go 文件（含控制器生产源码及测试），运行后 53 个摘要全部一致。已将 [最终源码冻结清单](final-source-sha256.txt) 的 53 项与 `slowlink-mit-rootgo-manifest.json` 逐项独立复核，全部匹配，完成本实验与最终冻结源码的关联。若相关源码随后改变，须重新判断本证据的适用性。

- `slowlink-mit-bbr-standard-8kib-20s.json` 与同名 `.stderr.txt`：原始结果，断言 `Passed=true`，退出码 0。
- `slowlink-mit-build.json`、`slowlink-mit-build-info.txt`：构建时刻、命令、模块与二进制摘要。
- `slowlink-mit-rootgo-manifest.json`、`slowlink-mit-run-validation.json`：53 个根文件的构建前摘要及运行后复核。
- `slowlink-mit-root-go.mod.txt`、`slowlink-mit-root-go.sum.txt`：根模块配置副本。
- `slowlink-mit-main.go.txt`、`slowlink-mit-main_test.go.txt`、`slowlink-mit-go.mod.txt`、`slowlink-mit-go.sum.txt`：本次实验源码及模块副本。
- `slowlink-mit-from-gpl-v2.patch`：相对旧第二版实验，仅导入、构造器和证据标签改变；吞吐、队列、真实在途峰值及生命周期断言未改。

从包含上述最终冻结源码的仓库根目录，用新的 PowerShell（命令外壳）进程复现。先从本报告目录中的四个源码/模块副本重建新的独立实验目录，再使用普通 Go 模块缓存；无需原本被忽略的实验目录或本机私有缓存。实验模块的唯一替换 `=> ../..` 指向当前仓库控制器源码，不能仅下载报告而省略控制器。`GOTMPDIR`（Go 临时构建目录）仅在该进程设置：

```powershell
New-Item -ItemType Directory -Force -Path bin/_slowlink-mit-replay,bin/_go-temp | Out-Null
Copy-Item reports/updates/2026-10-10-weaknet/slowlink-mit-main.go.txt bin/_slowlink-mit-replay/main.go
Copy-Item reports/updates/2026-10-10-weaknet/slowlink-mit-main_test.go.txt bin/_slowlink-mit-replay/main_test.go
Copy-Item reports/updates/2026-10-10-weaknet/slowlink-mit-go.mod.txt bin/_slowlink-mit-replay/go.mod
Copy-Item reports/updates/2026-10-10-weaknet/slowlink-mit-go.sum.txt bin/_slowlink-mit-replay/go.sum
$env:GOTMPDIR=(Resolve-Path bin/_go-temp).Path
Set-Location bin/_slowlink-mit-replay
go test .
go build -o ../check-slowlink-mit-replay.exe .
Set-Location ../..
& ./bin/check-slowlink-mit-replay.exe
```

本次只执行了峰值计量单元测试及这项 20 秒自检；并发检测、正式弱网矩阵和最终冻结后的完整验收由主进程统一执行。
