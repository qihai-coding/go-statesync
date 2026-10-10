# 弱网传输诊断实验

本目录记录 2026 年 10 月 10 日的诊断构建，**所有实验均非交付源码、非正式验收**。实验不改变每秒 15 次快照、256 实体、32 字节状态或按完整记录长度确定的分组；每批完成后再读取新批次，单条数据报组包门控保留。

## 三种构建

| 构建 | 传输算法与丢包判定 | 可执行文件 |
|---|---|---|
| baseline（基线） | 安装版默认 Reno（拥塞控制算法），包阈值 3 | `bin/check-baseline-lab.exe` |
| cubic（立方算法） | 两处创建拥塞控制器时改用 CUBIC（立方拥塞控制算法），包阈值 3 | `bin/check-cubic-lab.exe` |
| cubic-wide32（立方算法与宽阈值） | CUBIC（立方拥塞控制算法），固定包阈值 32 | `bin/check-cubic-wide32-lab.exe` |

第三组的固定阈值 32 **只是诊断变量，不是认可的最终算法**。它可能推迟真实丢包判断和可靠流恢复，不能因某次显示年龄改善就作为交付实现。三个构建共同使用诊断版追踪器，避免把是否启用诊断本身混入算法比较。

Go（编程语言）1.26 拒绝覆盖模块缓存内部文件，报错为 `Files beneath GOMODCACHE must not be replaced`（模块缓存下的文件不得替换）。因此在 `bin/weaknet-lab/quic-go` 原样复制已安装的 `quic-go@v0.62.0`（网络传输库），仅实验的 `lab.mod`（备用模块文件）增加本地替换。三组都引用这份副本；两个改算法组再通过 `-overlay`（编译期文件映射）覆盖副本中的单个丢包处理文件。

交付的 `transport_send.go`（发送门控源码）、根目录 `go.mod`（模块配置）、`go.sum`（依赖校验）及已安装模块缓存均未修改。编译仅在子进程环境设置 `GOTMPDIR`（编译临时目录）为本仓库 `bin/_go-temp`，不改持久环境配置。

`bin/weaknet-lab/go.mod`（实验模块边界）隔离本地诊断副本，避免根目录 `go list ./...`（递归列包）和 `go test ./...`（递归测试）把它们作为交付包。该文件与构建所用的 `lab.mod`（备用模块配置）不同，不移动映射路径，也不重建已冻结程序；边界自检及原四个实验程序的散列一致性见 `lab-module-boundary-self-check.txt`（模块边界自检）。

## 诊断输出

每连接关闭时只向标准错误输出一行 `weaknet_lab`（弱网实验）摘要；标准输出继续供原进程协调协议使用。追踪器不写日志文件、不登记全局映射、不增加应用协议，仍只等待数据报已被组包消费的通知。

- `loss_reordering_threshold`（包顺序阈值判丢数）与 `loss_time_threshold`（时间阈值判丢数）：来自 `qlog.PacketLost`（传输追踪丢包事件），含握手及应用阶段。
- `spurious_loss_detected`（已检出的误判丢包数）：来自 `qlog.SpuriousLoss`（迟到确认证明误判的事件）。库仅追踪最近 64 个判丢包，该数为检测下界；不能把未出现该事件的判丢包都认定为真实丢失。
- `cwnd_min_bytes`（拥塞窗口最小字节数）、`cwnd_time_mean_bytes`（按时间加权的拥塞窗口平均字节数）、`cwnd_max_bytes`（最大字节数）及 `cwnd_observed_seconds`（观察时长）：窗口变化来自 `qlog.MetricsUpdated`（传输指标更新），时间加权平均从首次非零窗口至连接关闭计算。事件字段为零表示该项未报告，不用它把当前窗口清零。
- `rtt_sample_mean_ms`（已报告往返时延样本均值，毫秒）及 `rtt_samples`（样本数）：仅累加非零 `LatestRTT`（最近往返时延）。这不是按时间加权均值。

没有 `MarkedForRetransmission`（标记重传）事件的统计：本版本没有该公开事件类型，且不可靠数据报丢失后不会重传。

## 构建及证据

构建脚本位于本地 `bin/weaknet-lab/build.ps1`（实验构建脚本）；文件映射为同目录三份 `*-overlay.json`（编译映射文件）。从仓库根目录运行脚本，它会验证改算法标记恰好两处、包阈值标记恰好一处，并核对原交付门控源码和安装版丢包处理源码的散列未变化。

每个构建的命令形状如下，路径按构建表替换：

```powershell
$env:GOTMPDIR = (Resolve-Path bin/_go-temp).Path
go build -modfile bin/weaknet-lab/lab.mod -overlay bin/weaknet-lab/baseline-overlay.json -o bin/check-baseline-lab.exe ./cmd/check
```

`lab-builds.json`（构建摘要）记录三个可执行文件、映射与诊断源码的 SHA-256（安全散列摘要）；`*-build-info.txt`（构建信息）保留编译器及本地依赖替换信息。`original-source-hashes.json`（原源码摘要）说明读取的原始文件；三个补丁分别记录共享诊断追踪器、仅切换算法、切换算法及宽阈值的精确差异。

本子任务只构建并执行原发送门控及诊断计数的短回归测试，结果见 `lab-self-check.txt`（实验自检记录），未运行网络负载。诊断测试也通过单独映射添加虚拟测试文件，没有向交付源码目录写入测试。后续三种构建应由主流程串行执行相同的 30 秒诊断条件，并分别保留应用字节、计划／提交／接受更新、逐实体显示年龄和上述传输摘要。不能把诊断结果替换原五分钟正式矩阵，也不能将总字节门槛改成每条更新的压缩率。
