# 第六组：SagerNet（开发组织）标准档拥塞控制隔离构建

2026 年 10 月 10 日完成隔离构建。本记录只证明固定版本、接口和测试源码可以编译，**本子任务没有运行负载**。后续主流程的负载结果及是否采用须单列；构建阶段没有修改交付源码、模块配置或已安装官方缓存。

## 固定的正规模块

两个直接依赖均使用自身公开模块路径，没有 `replace`（依赖替换）：

| 模块 | 固定版本 | 主仓库固定提交 |
|---|---|---|
| `github.com/sagernet/quic-go`（网络传输库） | `v0.61.0-sing-box-mod.9` | [`16930172ee48b008726849cc79ec576d5fa2bbc3`](https://github.com/SagerNet/quic-go/commit/16930172ee48b008726849cc79ec576d5fa2bbc3)，查询时为 `dev`（开发分支）最新提交，与此标签一致 |
| `github.com/sagernet/sing-quic`（传输扩展库） | `v0.7.2-0.20261002084117-75c3ac4fa12b` | [`75c3ac4fa12b4ded2dc01b392ed7d5eaf3ea6876`](https://github.com/SagerNet/sing-quic/commit/75c3ac4fa12b4ded2dc01b392ed7d5eaf3ea6876)，查询时为 `main`（主分支）最新提交 |

主仓库引用通过 `git ls-remote`（远程引用读取）取得；随后按完整提交查询 Go（编程语言）模块版本，解析结果的来源散列与检出一致。解析证据为 [传输库版本](sager-quic-version.json)、[扩展库版本](sager-sing-quic-version.json)，提交元数据见 [传输库提交](sager-quic-commit.txt)、[扩展库提交](sager-sing-quic-commit.txt)。

实际构建还引入 `sing`（基础工具库）、`utls`（传输加密扩展库）、`brotli`（压缩库）、`compress`（压缩库）及 `x/exp`（实验工具库）等间接依赖，完整版本与校验摘要保留在 [临时模块配置](sager-bbr-lab.mod)、[临时依赖校验](sager-bbr-lab.sum)及[构建信息](sager-bbr-build-info.txt)。不能把这次变更描述为只有两个包的维护成本。

## 最小源码覆盖

通过 `-overlay`（编译文件映射）将项目全部十个官方传输库导入源文件和测试副本改为上述正规路径。共享原三组的诊断追踪器与数据报组包门控保持相同逻辑，仅替换其模块导入路径；服务器接受连接后，在原连接处理函数第一行安装：

```go
c.SetCongestionControl(bbr.NewBbrSenderWithProfile(c.InitialPacketSize(), bbr.ProfileStandard))
```

`SetCongestionControl`（设置拥塞控制器）、`InitialPacketSize`（连接初始数据包大小）、`NewBbrSenderWithProfile`（按配置档创建发送器）和 `ProfileStandard`（标准配置档）均为固定依赖的公开接口，未添加兼容包装或项目配置项。客户端继续使用该传输库的默认控制器。

`congestion_meta2`（第二份拥塞实现目录）的[源文件注释](https://github.com/SagerNet/sing-quic/blob/75c3ac4fa12b4ded2dc01b392ed7d5eaf3ea6876/congestion_meta2/bbr_sender.go#L3)明确引用 Google（开发组织）的 `bbr_sender.cc`（发送控制源码），实现加入配置档；**目录名不能证明它是 BBRv2（瓶颈带宽与往返时延模型第 2 版）**。本实验称标准档实现，不作第 2 版算法声明。

静态检查中，传输库用 `RWMutex`（读写互斥锁）整体安装控制器，发送时在同一锁下取得全连接拥塞包号与控制器；旧包不会报告给新安装的控制器。公开包号与时间类型避免了前一个分支的内部类型暴露；扩展接口提供丢失、废弃包及应用受限清理通知。此处只是源码检查，构建成功不等同于竞态检测或长期资源验证。

## 构建产物与隔离

`bin/weaknet-sager-bbr-lab`（实验目录）及两个上游检出都有独立模块边界，根目录 `go list ./...`（列出所有包）仅发现八个交付包。原源文件不改写；编译临时目录为仓库 `bin/_go-temp`（临时目录），模块和构建缓存使用实验目录中的独立路径，环境变量仅对构建进程生效。

```powershell
$env:GOTMPDIR = (Resolve-Path bin/_go-temp).Path
$env:GOMODCACHE = (Resolve-Path bin/weaknet-sager-bbr-lab/mod-cache).Path
$env:GOCACHE = (Resolve-Path bin/weaknet-sager-bbr-lab/build-cache).Path
go build -mod=mod -modfile bin/weaknet-sager-bbr-lab/lab.mod -overlay bin/weaknet-sager-bbr-lab/overlay.json -o bin/check-sager-bbr-lab.exe ./cmd/check
```

构建成功，可执行文件 `bin/check-sager-bbr-lab.exe`（隔离验收程序）的 SHA-256（安全散列摘要）为 `0C8B4A5B6B63ABF4C445E579589118F05F9BCC4D9D55B6708482CABEDBCF31CE`，使用 Go（编程语言）1.26.5、Windows（操作系统）64 位构建。

- [构建摘要](sager-bbr-build.json)、[构建命令与日志](sager-bbr-build.txt)、[构建脚本文本](sager-bbr-build-script.ps1.txt)及[源码覆盖映射](sager-bbr-overlay.json)。
- [服务器精确差异](sager-bbr-server.patch)和[共享诊断源码差异](sager-bbr-trace.patch)，十份实际覆盖源码以 `sager-bbr-*.go.txt`（源码副本）保留。
- [仅编译测试的命令与日志](sager-bbr-tests-build.txt)及[测试构建摘要](sager-bbr-tests-build.json)：根测试源码编译成功，测试二进制未执行。这证明测试副本与正规模块接口匹配，不能称这些测试已通过。

构建摘要逐项核对交付模块、十个原源码／测试及已安装官方丢包处理源码散列均未变化。后续主流程可能另行采用该方案，本记录的未变核对只归属于这次隔离构建。

## 保留的风险

这次替换整个网络传输库实现，不能将结果全部归因于拥塞算法。算法维护、安全更新、间接依赖、真实带宽突降、可靠恢复、竞争公平性、丢包清理、长期资源和平台兼容仍需评估。正规依赖路径避免本地替换不向下游传播的问题，不能代替这些质量门槛。

本子任务未运行弱网负载、竞态检测、公平性竞争或长期资源测试；后续验收应继续用原始总应用字节、频率与覆盖门槛，不改变指标定义。
