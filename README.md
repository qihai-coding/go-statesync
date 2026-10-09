# Go StateSync

**用 Go（编程语言）构建联机游戏的服务器权威状态同步。**

[![CI（持续集成）](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml/badge.svg)](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml)
[![Go（编程语言）](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![License（许可证）](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Protocol（协议）](https://img.shields.io/badge/Protocol-v2-5268e8)](PROTOCOL.md)

[English](README.en.md) · [接入指南](docs/INTEGRATION.md) · [协议](PROTOCOL.md) · [断线续接](RESUME.md) · [验收报告](reports/release-v0.2.1/VALIDATION.md)

面向每房间最多 16 人的合作游戏与实时交互应用。核心库独立于游戏引擎，提供独立服务器、无界面参考客户端、二维移动与拾取示例，以及真实加密连接上的弱网测试。

当前发布 **v0.2.1**，使用第 2 版同步协议。库版本与协议版本分别管理；客户端与服务器需要使用相同协议。

## 核心能力

| 能力 | 行为 |
|---|---|
| 多房间权威模拟 | 每个房间独占游戏状态，固定步长推进，网络消息经有界队列投递 |
| 本地预测与校正 | 输入立即预测；收到权威状态后重放尚未确认的输入 |
| 远端快照插值 | 默认缓冲 200 毫秒；缓冲不足时保持最后状态 |
| 进程内断线续接 | 默认保留会话 60 秒；新连接接管原角色，最近操作结果可原样重试 |
| 生命周期与故障隔离 | 可靠同步生成、销毁和拾取；非法批次或可恢复异常只结束受影响房间 |
| 明确的资源边界 | 输入、控制和发送队列均有上限；慢客户端被剔除，旧运动快照可被替换 |

## 架构

```mermaid
flowchart LR
    subgraph C[参考客户端]
        P[输入与本地预测]
        R[权威校正与远端插值]
    end
    Q[QUIC 加密传输]
    subgraph S[服务器]
        A[房间 A 独立循环]
        B[房间 B 独立循环]
    end
    P -->|移动输入数据报| Q
    Q --> A
    Q --> B
    A -->|运动快照数据报| Q
    B -->|运动快照数据报| Q
    Q --> R
    C <-->|加入、完整状态、生命周期、操作、续接：可靠流| Q
```

QUIC（基于数据报的加密传输协议）由 quic-go（网络传输库）提供。移动输入和运动快照允许丢失，生命周期与离散操作使用可靠流；每条实体运动记录可独立应用。游戏自己定义输入、状态与编码，核心不扫描属性或依赖反射。

默认每秒执行 30 个逻辑步、发送 15 次运动快照，单个数据报不超过 1000 字节。完整配置及边界见[接入指南](docs/INTEGRATION.md#配置)。

## 快速运行

需要 Go（编程语言）1.26 或以上。

```sh
git clone https://github.com/qihai-coding/go-statesync.git
cd go-statesync
go mod download
go run ./cmd/server -generate-cert
go run ./cmd/server -rooms alpha,beta
```

在另外两个终端分别启动客户端：

```sh
go run ./cmd/client -room alpha -x 1 -duration 10s
go run ./cmd/client -room alpha -y 1 -duration 10s
```

演示拾取和断线续接：

```sh
go run ./cmd/client -room beta -pickup 1 -x 1 -duration 8s -disconnect-after 2s
```

本地演示证书有效期为 24 小时，生成命令不覆盖已有证书。重复运行时用 `-cert`、`-key` 和客户端的 `-ca` 指定新文件；客户端始终校验证书。

## 作为库使用

```sh
go get github.com/qihai-coding/go-statesync@v0.2.1
```

实现 `Game`（服务端游戏接口）和 `Model`（客户端预测及编码接口），为每个房间创建独立游戏实例。可先查看[二维示例](arena/arena.go)，再按[接入指南](docs/INTEGRATION.md#接入自己的游戏)替换游戏规则。

业务保存凭证并显式续接，成功后获得新的客户端对象：

```go
token := client.ResumeToken()
client.Disconnect()
next, err := statesync.DialResume(ctx, address, room, tlsConfig, model, token)
if err != nil {
    return err
}
client = next
```

`ResumeToken`（续接凭证）不得写入日志。未确认操作使用原编号、原负载重试；`LastActionID`（最近处理操作编号）包括失败操作，不表示业务成功。完整规则见[续接说明](RESUME.md)。

## 验证与性能

```sh
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
go run ./cmd/check -isolate -rooms 8 -clients 16 -duration 10m -resume-every 30s -report soak.json
```

竞态检测需要兼容的 C（编程语言）编译器。[持续集成](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml)在三种桌面系统上执行原生测试和构建，另在 Linux（开源操作系统）执行竞态检测与五类模糊测试。

[本次发布验收](reports/release-v0.2.1/VALIDATION.md)记录 8 房间 × 16 人十分钟负载、三档弱网、周期续接、资源及每客户端带宽。性能数据来自指定环境实测，房间单步目标为第 99 百分位低于 10 毫秒；不等于每一步都有硬性十毫秒上限。本版分别记录服务器和客户端／代理进程的资源，总量不含协调父进程；带宽为应用负载口径。测量方法与证据边界见[独立性能测量](docs/MEASUREMENT.md)。

## 接入范围

- 每个玩家会话对应一个受控动态实体；本阶段不支持角色重生或控制权切换。
- 续接仅在当前服务器进程内有效，重启后凭证失效；账号认证、持久化、匹配和跨服务器调度由接入应用提供。
- 游戏回调必须快速且不阻塞。异常隔离覆盖可恢复运行时异常，不处理中断进程或无限阻塞。
- 使用完整运动快照及显式离散事件；差量压缩和可见范围筛选尚未加入。

## 文档与贡献

| 文档 | 内容 |
|---|---|
| [接入指南](docs/INTEGRATION.md) | 配置、游戏接口、预测模型、计时与验证口径 |
| [同步协议](PROTOCOL.md) | 字节布局、生命周期、输入确认与固定报文样例 |
| [独立性能测量](docs/MEASUREMENT.md) | 分进程运行、资源字段、收敛采样和证据边界 |
| [断线续接](RESUME.md) | 会话、接管、取消、操作重试与故障处理 |
| [验收报告索引](reports/README.md) | 当前发布与历史验证证据 |
| [更新记录](CHANGELOG.md) | 版本变化与核心加固记录 |
| [贡献指南](CONTRIBUTING.md) | 复现问题、运行检查和提交变更 |

设计参考：[Nakama（游戏后端框架）的房间调度](https://github.com/heroiclabs/nakama/blob/master/server/match_handler.go)、[Mirror（游戏网络同步框架）的插值](https://github.com/MirrorNetworking/Mirror/blob/master/Assets/Mirror/Core/SnapshotInterpolation/SnapshotInterpolation.cs)、[Colyseus（状态同步框架）的编码](https://github.com/colyseus/schema)、[客户端预测与服务器校正](https://www.gabrielgambetta.com/client-side-prediction-server-reconciliation.html)。实现独立编写，这些项目不构成运行依赖。

本项目采用 [MIT（开源许可证）](LICENSE)。
