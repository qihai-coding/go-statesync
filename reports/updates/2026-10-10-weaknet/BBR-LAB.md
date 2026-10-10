# BBRv1（瓶颈带宽与往返时延模型第 1 版）隔离构建

2026 年 10 月 10 日完成第四种实验构建，**只构建，未运行负载，也不是正式验收或交付依赖**。实验保持现有协议、逻辑／快照频率、实体覆盖、完整长度分组及单条数据报组包门控；不改交付的模块配置或源码。

## 固定来源与变量

通过主仓库 `git ls-remote`（读取远程引用）取得 [qiulaidongfeng/quic-go（网络传输库分支仓库）](https://github.com/qiulaidongfeng/quic-go) 的 `refs/heads/master`（主分支引用），固定完整提交为 [`947ce4371157f0020731bd5362b29f3b74c6e591`](https://github.com/qiulaidongfeng/quic-go/commit/947ce4371157f0020731bd5362b29f3b74c6e591)。本地独立检出目录为 `bin/weaknet-bbr-lab/quic-go`（实验依赖源码），构建前核对提交与工作树清洁状态。上游提交元数据见 [bbr-upstream-commit.txt（固定提交信息）](bbr-upstream-commit.txt)。

上游模块路径仍为 `github.com/quic-go/quic-go`，要求 Go（编程语言）1.26.0。临时模块通过本地 `replace`（依赖替换）使用该检出；没有修改上游检出或官方模块缓存。

共享前三组的诊断版 `transport_send.go`（数据报门控源码）副本仅增加以下服务器设置，其余追踪与诊断相同：

```go
cfg.Congestion = func() quic.SendAlgorithmWithDebugInfos {
    return quic.NewBBRv1(cfg)
}
```

`Congestion`（拥塞控制工厂）、`SendAlgorithmWithDebugInfos`（带调试信息的发送算法接口）和 `NewBBRv1`（创建第 1 版模型控制器）在上述固定提交中存在，构建无需接口兼容改写。客户端没有设置该工厂，继续使用分支仓库的默认发送算法。

**本实验替换整个分支仓库，不是只替换官方版本的一处算法。** 该提交包含其他代码差异，因此后续结果不能全部归因于 BBRv1（瓶颈带宽与往返时延模型第 1 版）。与前三组相比，共享诊断代码相同，但依赖实现来源不同。

## 构建和产物

从仓库根目录运行本地脚本 `bin/weaknet-bbr-lab/build.ps1`（隔离构建脚本），脚本文本留存在 [bbr-build-script.ps1.txt（原脚本副本）](bbr-build-script.ps1.txt)。核心命令如下：

```powershell
$env:GOTMPDIR = (Resolve-Path bin/_go-temp).Path
$env:GOMODCACHE = (Resolve-Path bin/weaknet-bbr-lab/mod-cache).Path
$env:GOCACHE = (Resolve-Path bin/weaknet-bbr-lab/build-cache).Path
go build -mod=mod -modfile bin/weaknet-bbr-lab/lab.mod -overlay bin/weaknet-bbr-lab/overlay.json -o bin/check-bbr-lab.exe ./cmd/check
```

`GOTMPDIR`（编译临时目录）、`GOMODCACHE`（模块缓存）和 `GOCACHE`（构建缓存）仅在构建进程设置。后两者是本实验的独立目录，不写已安装官方缓存或持久环境配置。`-modfile`（临时模块文件）和 `-overlay`（编译文件映射）使交付模块与门控源码保持原样。

实验目录另有独立 `go.mod`（模块边界），与构建用 `lab.mod`（临时交付模块配置）分开；根目录 `go list ./...`（列出所有包）不会发现实验源码。算法自检位于 `_stat-retention`（统计保留自检）下划线目录，另有独立模块配置和明确的本地依赖替换。

构建成功，产物为 `bin/check-bbr-lab.exe`（实验验收程序），SHA-256（安全散列摘要）为 `1425B8398C7D70463ED21FFED22D35F6392235E90A2238CB323C2C89B34C0748`。编译器为 Go（编程语言）1.26.5，Windows（操作系统）64 位，未启用竞态检测。

- [bbr-build.txt（构建命令与日志）](bbr-build.txt)及 [bbr-build-info.txt（可执行文件构建信息）](bbr-build-info.txt)。
- [bbr-build.json（提交、路径、散列与未变核对）](bbr-build.json)：构建退出码为 0；负载执行标记为假。
- [bbr-lab.mod（临时模块配置）](bbr-lab.mod)、[bbr-lab.sum（临时依赖校验）](bbr-lab.sum)及 [bbr-upstream-go.mod.txt（上游模块要求）](bbr-upstream-go.mod.txt)。
- [bbr-transport_send.go.txt（实际覆盖源码）](bbr-transport_send.go.txt)、[bbr-overlay.json（覆盖映射）](bbr-overlay.json)及 [bbr-transport.patch（相对共享诊断源码的差异）](bbr-transport.patch)。

脚本核对交付的 `go.mod`（模块配置）、`go.sum`（依赖校验）、`transport_send.go`（门控源码）和已安装官方丢包处理源码散列均未变化。分支仓库检出仍无本地修改。

## 验证边界与采用条件

本次只确认固定来源的接口与本项目可以构建；没有运行网络负载、上游完整测试、竞争流公平性、带宽／队列边界、真实拥塞或可靠流恢复测试。可执行文件可供主流程的第四组隔离诊断使用，不能把构建成功视为弱网压缩、显示年龄或恢复通过。

不同往返时延及与 Reno（雷诺算法）、CUBIC（立方拥塞控制算法）的竞争公平性仍需独立验证。分支仓库的维护跟进、安全更新、其他代码差异、算法回归及测试质量也需评估；实验结果不能直接证明适合作为交付依赖。后续是否采用由质量评估决定，当前官方依赖保持不变。

### 已运行的统计保留自检

仅调用固定提交中的算法对象，**没有创建网络连接或运行负载**。自检源码见 [bbr-stat-retention-check.go.txt（算法断言副本）](bbr-stat-retention-check.go.txt)，独立模块见 [bbr-stat-retention-go.mod.txt（自检依赖）](bbr-stat-retention-go.mod.txt)，运行结果见 [bbr-stat-retention-self-check.txt（实际输出）](bbr-stat-retention-self-check.txt)。

- 模拟发送 10,000 个 ACK-only（仅传输确认）包后，`sentTimes`（发送时间表）仍有 10,000 项。算法无条件记录发送时间；实际发送处理器只对计入 `bytesInFlight`（在途字节）的包回调确认，对需要确认的包回调丢失，因而仅确认包的表项无法沿正常路径清理。
- 两个独立包号空间各发送第 0 号包，时间表只有 1 项；第一个空间确认后表项变成 0，第二个空间未确认包的记录也已消失。第一个包实际等待 11 毫秒，自检看到的最小往返时延为 10 毫秒，证明发送时间覆盖会影响采样。

上述链路依据固定提交的 [算法发送与确认处理](https://github.com/qiulaidongfeng/quic-go/blob/947ce4371157f0020731bd5362b29f3b74c6e591/internal/congestion/bbrv1.go#L95)、[发送处理器回调](https://github.com/qiulaidongfeng/quic-go/blob/947ce4371157f0020731bd5362b29f3b74c6e591/internal/ackhandler/sent_packet_handler.go#L308)及[确认回调条件](https://github.com/qiulaidongfeng/quic-go/blob/947ce4371157f0020731bd5362b29f3b74c6e591/internal/ackhandler/sent_packet_handler.go#L461)。没有修改分支仓库来隐藏这些结果。

静态检查中，发送处理器的在途字节在确认、判丢和丢弃密钥空间时都有减计；未发现与发送时间表相同的无条件累积路径。`ackinfo`（近期确认采样）按最小往返时延窗口裁剪，这部分结论仅来自源码检查，未执行长时间内存测量。该提交未找到直接调用 BBRv1（瓶颈带宽与往返时延模型第 1 版）的自动化测试，现有立方算法及带宽换算测试不能替代专项算法回归。

### 最小后续验证建议

现有验收程序可在每秒 30／15 次逻辑／快照、16 人、256 实体、32 字节不变时，分别比较四种构建的 150／300 毫秒及两个固定种子；同时观察每客户端实际实体更新、每远端实体显示年龄、恢复、处理器和堆增长。十秒以上运行才能覆盖本实现的周期探测；短诊断结果仍不能代替原五分钟配对门槛。

当前代理只注入延迟、随机丢包、重复和乱序，**没有共同带宽上限或有限队列**；它可以验证本项目的显示时效，不能证明争用公平性。最小公平性实验需要两条持续有需求的流经过同一受限带宽和有限队列：先同算法对同算法，再让实验算法分别与雷诺／立方算法竞争，并互换长短往返时延。应记录每条流吞吐、显示年龄、可靠操作时延、队列延迟和真实拥塞丢包，不能只用总吞吐判断。这个共同瓶颈工具及实验本轮均未实施。

采用前还应有仅确认包清理、独立包号空间、密钥空间丢弃、MTU（最大传输单元）探测丢失、探测进入／退出、带宽突降及可靠流恢复的确定性回归。当前构建仍仅用于因果对照，不能作为正式依赖建议。
