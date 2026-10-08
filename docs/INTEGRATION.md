# 接入与验证指南

需要 Go（编程语言）1.26 或以上。唯一直接运行依赖是 quic-go（加密网络传输库）0.62.0。Windows（微软桌面操作系统）、Linux（开源操作系统）和 macOS（苹果桌面操作系统）均有对应构建路径；当前使用第 2 版协议，客户端和服务器必须一起升级。第二阶段结果见 [核心加固验收报告](../reports/phase2/VALIDATION.md)，首版历史记录见 [首版报告](../reports/VALIDATION.md)。

在仓库根目录执行：

~~~powershell
go mod download
go run ./cmd/server -generate-cert
go run ./cmd/server -rooms alpha,beta
~~~

在其他终端运行两个客户端：

~~~powershell
go run ./cmd/client -room alpha -x 1 -duration 10s
go run ./cmd/client -room alpha -y 1 -duration 10s
~~~

拾取示例：

~~~powershell
go run ./cmd/client -room beta -pickup 1 -duration 5s
~~~

编号 1 的道具位于出生点。两人争抢时只有一个请求成功。客户端输出本地预测位置和服务器已确认的输入编号。

本地证书有效期为 24 小时，生成命令不会覆盖已有证书或私钥。重复演示可通过 -cert 和 -key 指定新的文件名，同时为客户端 -ca 指定对应证书。公网部署使用适合真实域名的证书，并通过 -server-name 指定该域名；客户端始终校验证书。

服务器默认只监听本机。需要外部连接时，显式设置 -listen 为适当的监听地址。

## 配置

| 参数 | 默认值 | 边界 |
|---|---:|---|
| 每秒逻辑步数 | 30 | 1～120 |
| 每秒运动快照数 | 15 | 1～逻辑频率，允许不整除 |
| 每房间人数 | 16 | 1～16，断线保留会话也占位 |
| 断线保留时间 | 60 秒 | 可配置，0 关闭续接 |
| 单个运动数据报 | 1000 字节 | 配置范围 600～1000 |
| 远端插值延迟 | 200 毫秒 | 客户端可设为 0～2 秒 |
| 本地输入历史 | 128 步 | 达到上限时请求完整状态 |
| 每玩家待处理输入 | 256 条 | 超出序号窗口拒绝 |
| 每房间活动实体 | 256 | 每实体状态最多 512 字节 |
| 每服务器房间 | 64 | 可创建、关闭、重建 |
| 每房间队列 | 控制 128，输入批次 256 | 固定容量 |
| 每连接发送队列 | 可靠 64，快照 1 批 | 快照覆盖旧批次，可靠积压断开 |

服务器命令行通过 -tick、-snapshots、-players 修改频率和人数，通过 -resume-grace 修改保留时间。库配置使用 Config（配置结构）。客户端从完整状态报文获得逻辑频率，不应自行猜测。

## 接入自己的游戏

核心包导入路径为 github.com/qihai-coding/go-statesync（公开模块路径）。

1. 实现 Game（服务端游戏接口），并为每个房间创建独立实例。
2. 明确编码输入、实体状态和离散操作。核心仅传输字节，不使用反射或自动字段扫描。
3. 为参考客户端实现 Model（客户端模型接口），复用相同的移动规则。
4. 调用 Listen（监听）、CreateRoom（创建房间）以及 Dial（连接）。
5. 按客户端 TickRate（逻辑频率）调用 SubmitInput（提交输入），通过 Sample（采样显示状态）取得预测或插值结果。

### 服务端游戏接口

| 方法 | 约定 |
|---|---|
| Join（加入） | 首次生成唯一受控角色；续接不重复调用；失败不得修改状态 |
| Leave（退出） | 主动退出、保留超时、剔除或房间结束时调用一次；删除玩家实体 |
| ValidateInput（校验输入） | 校验长度、数值范围和特殊浮点值 |
| Step（推进一步） | 固定时间步更新；每玩家最多收到一条输入，按玩家编号排序 |
| Action（离散操作） | 根据连接绑定的玩家判定权限、距离和归属，返回状态变更与结果；失败不得修改状态 |
| Entities（当前实体） | 返回完整实体列表及显式编码状态 |

创建房间时同步读取一次初始实体，此后的所有方法只在房间所属的执行循环内调用。应保持快速、非阻塞，不在这些方法中访问磁盘或等待网络。游戏返回的切片及字节在交给框架后不得修改；新的状态使用新字节值。核心会把同一份编码结果作为只读数据广播给多个连接。

Entity（实体记录）包含编号、生命周期代数、拥有者、是否持续发送快照、已确认输入编号和游戏状态字节。拥有者为 0 表示非玩家受控实体。每个客户端对应一个用于本地预测的动态实体。实体生成或删除必须通过 Change（状态变更）声明；回收实体编号时增加生命周期代数。

示例实现集中在 [二维游戏代码](../arena/arena.go)：移动、边界裁剪、输入验证、状态编码和拾取规则都不依赖引擎。

### 客户端接口

| 方法 | 用途 |
|---|---|
| SubmitInput（提交输入） | 立即预测一小步，并发送最近三条输入 |
| Authoritative（权威视图） | 返回最近收到的逻辑状态副本，适合检查一致性 |
| Sample（显示采样） | 本地角色返回预测状态；其他角色返回延迟插值状态 |
| Action（离散操作） | 使用调用方递增的请求编号；同会话最近一次操作可跨连接原样重试 |
| Resync（重新同步） | 清除预测歧义并请求完整状态；最多每秒一次，过快返回 ErrBusy（繁忙错误） |
| DialResume（续接） | 凭有效凭证获取新客户端对象，立即接管旧连接 |
| ResumeToken（续接凭证）、LastActionID（最近操作编号） | 读取恢复所需信息，凭证不得记录到日志 |
| Disconnect（临时断开） | 关闭传输并允许服务器保留会话 |
| Close（关闭） | 主动退出；等待本地后台任务退出 |
| Err（错误） | 查看传输或协议失败原因 |

操作超时表示结果可能尚未收到，不代表服务器没有执行。最近一次请求必须使用相同编号和负载重试；更旧编号或同编号不同负载被拒绝。续接保留身份、角色和最近操作结果；普通首次连接仍创建新玩家。使用方式见 [断线续接接入说明](../RESUME.md)。

## 同步行为

- 加入时先可靠发送完整状态。只有可靠生命周期事件可以生成实体。
- 每个运动快照是独立的完整实体状态；按完整实体拆包，不等待整帧所有分包。
- 全局状态版本必须与客户端一致，实体代数必须匹配，运动帧号必须更新，快照才会应用。
- 每个玩家每个服务端逻辑步最多执行一条移动输入；丢失输入被较新输入越过后视为跳过，不补跑额外模拟时间。
- 没有输入时，示例角色保持位置。游戏仍会按固定步长推进。
- 本地校正先恢复权威状态，再重放未确认输入；只预测本地角色的运动，道具归属由服务器确认。
- 远端插值缓冲不足时保持已有状态，不向未来外推。
- 房间最多连续追赶四步；如果落后超过一秒则停止该房间，避免无限追赶消耗处理器。

详细字节布局、固定报文和重同步流程见 [协议说明](../PROTOCOL.md)。

## 验证和基准

~~~powershell
go test ./... -count=1
go vet ./...
go test -run '^$' -fuzz '^FuzzDecode$' -fuzztime 15s -parallel 2
go test -run '^$' -fuzz '^FuzzReadFrame$' -fuzztime 15s -parallel 2
go test -run '^$' -fuzz '^FuzzLifecyclePreflight$' -fuzztime 15s -parallel 2
go test -run '^$' -fuzz '^FuzzFullStateResync$' -fuzztime 15s -parallel 2
go test -run '^$' -bench . -benchmem -count 3
go test -race ./... -count=1
~~~

-race（竞态检测）需要 C（编程语言）编译器和 CGO（与 C 语言互操作功能）。在 Windows（微软桌面操作系统）上可临时设置：

~~~powershell
$env:CGO_ENABLED = '1'
$env:CC = '你的编译器目录\bin\x86_64-w64-mingw32-gcc.exe'
go test -race ./... -count=1
~~~

本次验证使用校验过下载摘要的便携编译器，没有修改系统环境变量。

10 分钟稳定性和资源测试：

~~~powershell
go run ./cmd/check -duration 10m -rooms 8 -clients 16 -resume-every 30s -report reports/local/soak-8x16-10m.json
~~~

16 人弱网验证：

~~~powershell
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 0ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-0ms.json
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 150ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-150ms.json
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 300ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-300ms.json
~~~

弱网代理直接干扰加密 UDP（用户数据报协议）报文，覆盖握手、重传、丢包、重复与乱序。队列和调度缓冲都有容量上限；使用固定随机种子，但操作系统调度仍会影响具体丢包序列。

### 指标含义

- 房间单步耗时包含游戏更新、快照编码和消息入队，目标第 99 百分位小于 10 毫秒。
- Windows（微软操作系统）下的单步计时使用 QPC（系统高精度计数器），其他支持的平台使用单调时钟；直方图每格 0.1 毫秒，报告所在格的上界。首版历史计时口径不用于精确对比亚毫秒性能。
- 处理器时间、分配量和堆内存统计包含同一进程内的服务器、参考客户端及启用的弱网代理，不应当作服务器单独成本。
- 带宽统计为应用层负载，未包含加密、传输头、确认和重传开销。
- 内存每 30 秒主动回收后采样。长测以第 60 秒为基线，末次堆大小允许基线的 25% 加 4 MiB（兆二进制字节）余量；协程数允许每房间增加 16。
- 短测用于功能和收敛验证。新报告以 MemoryChecked（是否执行长期内存检查）标记证据范围；不足 10 分钟或缺少有效采样时为假，MemoryBounded（内存与协程增长是否受限）也为假，不能据此认定内存失败或通过。只有前者为真时，后者才有检查结果含义。
- Passed（本次运行通过）对短测汇总功能、连接及单步预算；对至少 10 分钟的运行还要求完成并通过内存检查。旧报告没有 MemoryChecked（是否执行长期内存检查）字段，短测中的内存通过标志不代表长期检查已执行；旧报告原样保留。
- 测试最后恢复正常网络并继续发送零移动输入，等待一秒后比较所有实体状态及本地预测值。
- 插值输出有意晚于权威状态；一致性检查使用 Authoritative（权威视图）。

## 已知范围

这是房间同步核心和可运行示例。玩家身份为服务器分配的会话编号，房间默认开放加入。断线续接只在当前进程内有效；真实账号认证、持久化、匹配、跨服务器调度和游戏引擎适配由接入应用提供。

首版采用完整运动快照和显式离散事件。差量压缩和可见范围筛选应根据实际带宽或处理耗时再加入。

## 设计参考

- [Nakama（游戏后端框架）房间调度](https://github.com/heroiclabs/nakama/blob/master/server/match_handler.go)：单房间串行更新与有界队列。
- [Mirror（游戏网络同步框架）快照插值](https://github.com/MirrorNetworking/Mirror/blob/master/Assets/Mirror/Core/SnapshotInterpolation/SnapshotInterpolation.cs)：延迟时间轴与缓冲。
- [Colyseus（房间式联机框架）状态编码](https://github.com/colyseus/schema)：明确的完整状态与变更消息。
- [客户端预测与服务器校正](https://www.gabrielgambetta.com/client-side-prediction-server-reconciliation.html)：输入编号、确认和重放。
- [quic-go（网络传输库）数据报说明](https://quic-go.net/docs/quic/datagrams/)：可靠数据流和可丢弃数据报。

这里是按上述机制独立编写的实现，没有引入这些游戏后端或同步框架的运行依赖。
