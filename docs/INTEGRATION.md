# 接入与验证指南

需要 Go（编程语言）1.26 或以上。唯一直接运行依赖是 quic-go（加密网络传输库）0.62.0。Windows（微软桌面操作系统）、Linux（开源操作系统）和 macOS（苹果桌面操作系统）均有对应构建路径；当前开发树使用第 3 版协议，客户端和服务器必须一起升级，见[迁移说明](../MIGRATION.md)。第二阶段结果见 [核心加固验收报告](../reports/phase2/VALIDATION.md)，首版历史记录见 [首版报告](../reports/VALIDATION.md)。

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
| 快照编码 | 无损差量 | 完整模式用于同协议对照 |
| 每连接每端可靠字典 | 最多 256 个实体 | 状态合计最多 128 KiB（千二进制字节） |

服务器命令行通过 -tick、-snapshots、-players 修改频率和人数，通过 -resume-grace 修改保留时间，-encoding（快照编码模式）接受 delta（差量）或 full（完整），-datagram-size（数据报上限）指定 600～1000 字节。库配置使用 Config（配置结构）的 SnapshotEncoding（快照编码配置）。客户端从完整状态报文获得逻辑频率，不应自行猜测。

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

创建房间时同步读取一次初始实体，此后的所有方法只在房间所属的执行循环内调用。应保持快速、非阻塞，不在这些方法中访问磁盘或等待网络。游戏返回的切片及字节在交给框架后不得修改；新的状态使用新字节值。各连接共享只读状态字节，运动报文按各连接已确认字典分别编码；实体列表不必由游戏排序。

Entity（实体记录）包含编号、生命周期代数、拥有者、是否持续发送快照、已确认输入编号和游戏状态字节。拥有者为 0 表示非玩家受控实体。每个客户端对应一个用于本地预测的动态实体。实体生成或删除必须通过 Change（状态变更）声明；回收实体编号时增加生命周期代数。

示例实现集中在 [二维游戏代码](../arena/arena.go)：移动、边界裁剪、输入验证、状态编码和拾取规则都不依赖引擎。

### 客户端接口

| 方法 | 用途 |
|---|---|
| SubmitInput（提交输入） | 立即预测一小步，并发送最近三条输入 |
| Authoritative（权威视图） | 返回最近收到的逻辑状态副本，适合检查一致性 |
| Sample（显示采样） | 本地角色返回预测状态；其他角色返回延迟插值状态 |
| SampleWithInfo（带来源信息采样） | 同时返回该显示状态的真实来源时间、最新状态时间、保持和预测标记 |
| Stats（客户端统计） | 实际运动记录更新、完整状态、可靠变更次数及当前字典占用 |
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
- 每条运动记录独立引用已确认的可靠基线，不依赖上一包；按完整记录预算拆包，不等待整帧所有分包。
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
go test -run '^$' -fuzz '^FuzzSnapshotAck$' -fuzztime 15s -parallel 2
go test -run '^$' -fuzz '^FuzzDelta$' -fuzztime 15s -parallel 2
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

演示负载的八房间续接与资源测试：

~~~powershell
go run ./cmd/check -isolate -duration 10m -rooms 8 -clients 16 -resume-every 30s -report reports/local/soak-8x16-10m.json
~~~

16 人弱网验证：

~~~powershell
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 0ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-0ms.json
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 150ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-150ms.json
go run ./cmd/check -duration 20s -clients 16 -resume-every 3s -rtt 300ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -report reports/local/weak-300ms.json
~~~

弱网代理直接干扰加密 UDP（用户数据报协议）报文，覆盖握手、重传、丢包、重复与乱序。队列和调度缓冲都有容量上限；使用固定随机种子，但操作系统调度仍会影响具体丢包序列。

差量标准负载使用 16 人、256 个动态实体和每实体 32 字节稀疏运动状态。原 arena（二维演示）继续使用 13 字节状态，不为压缩指标增加填充。两模式的实体分组、逻辑和快照频率相同：

~~~powershell
go run ./cmd/check -isolate -workload motion -encoding delta -entities 256 -state-bytes 32 -clients 16 -duration 60s -report reports/local/motion-delta.json
go run ./cmd/check -isolate -workload motion -encoding full -entities 256 -state-bytes 32 -clients 16 -duration 60s -report reports/local/motion-full.json
go run ./cmd/check -isolate -workload motion -encoding delta -clients 16 -duration 5m -rtt 300ms -jitter 30ms -loss 0.05 -duplicate 0.02 -reorder 0.02 -seed 7 -report reports/local/weak-delta.json
~~~

弱网对照分别使用 150／300 毫秒、种子 7／701，完整与差量每组五分钟；正常网络两模式各六十秒。稀疏负载各组总应用字节下降至少 50%，每个远端实体显示年龄第 95 百分位不超过一秒；解除弱网起一秒内权威、预测及远端显示收敛，且恢复期间没有可靠状态纠正。-workload entropy（高熵负载）的两模式对照仅要求差量流量增幅不超过 5%。另用 -state-bytes 512（最大状态长度）和 -datagram-size 600（最小数据报预算）运行边界组，标准负载运行十分钟检查资源。

### 指标含义

- 房间单步耗时包含游戏更新、快照编码和消息入队，目标第 99 百分位小于 10 毫秒。
- Windows（微软操作系统）下的单步计时使用 QPC（系统高精度计数器），其他支持的平台使用单调时钟；直方图每格 0.1 毫秒，报告所在格的上界。首版历史计时口径不用于精确对比亚毫秒性能。
- 默认同进程模式的处理器时间、分配量和堆内存包含服务器、参考客户端及启用的弱网代理。v0.2.1 可添加 `-isolate`（分进程模式）分别测量服务器与客户端／代理，见[独立性能测量](MEASUREMENT.md)。
- 带宽统计为应用层负载，未包含加密、传输头、确认和重传开销。
- 内存每 30 秒主动回收后采样。长测以第 60 秒为基线，末次堆大小允许基线的 25% 加 4 MiB（兆二进制字节）余量；协程数允许每房间增加 16。
- 短测用于功能和收敛验证。新报告以 MemoryChecked（是否执行长期内存检查）标记证据范围；不足 10 分钟或缺少有效采样时为假，MemoryBounded（内存与协程增长是否受限）也为假，不能据此认定内存失败或通过。只有前者为真时，后者才有检查结果含义。
- Passed（本次运行通过）汇总对应负载的功能、连接、单步、时效和恢复门槛；至少十分钟的运行还要求通过内存检查。压缩率由同条件两份报告比较，不是单次运行的通过标志。
- 演示负载沿用一秒等待后观察；标准负载解除弱网后通过小操作冻结模拟，操作耗时计入一秒预算，每二十毫秒检查权威状态、本地预测及远端插值显示，禁止借可靠实体变更或完整重新同步完成恢复。
- 显示年龄对应逐实体实际返回的采样来源，不使用全局最新包时间代替，保持旧状态也计入年龄。前五秒预热，之后采用十毫秒桶的保守上界；跨进程时钟校准误差记入报告。

## 已知范围

这是房间同步核心和可运行示例。玩家身份为服务器分配的会话编号，房间默认开放加入。断线续接只在当前进程内有效；真实账号认证、持久化、匹配、跨服务器调度和游戏引擎适配由接入应用提供。

本版加入已确认可靠字典上的差量快照，显式离散事件和受控角色限制不变。可见范围筛选尚未加入。

## 设计参考

- [Nakama（游戏后端框架）房间调度](https://github.com/heroiclabs/nakama/blob/master/server/match_handler.go)：单房间串行更新与有界队列。
- [Mirror（游戏网络同步框架）快照插值](https://github.com/MirrorNetworking/Mirror/blob/master/Assets/Mirror/Core/SnapshotInterpolation/SnapshotInterpolation.cs)：延迟时间轴与缓冲。
- [Colyseus（房间式联机框架）状态编码](https://github.com/colyseus/schema)：明确的完整状态与变更消息。
- [客户端预测与服务器校正](https://www.gabrielgambetta.com/client-side-prediction-server-reconciliation.html)：输入编号、确认和重放。
- [quic-go（网络传输库）数据报说明](https://quic-go.net/docs/quic/datagrams/)：可靠数据流和可丢弃数据报。

这里是按上述机制独立编写的实现，没有引入这些游戏后端或同步框架的运行依赖。
