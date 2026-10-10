# BBR 控制器来源与适配

本目录固定移植 [Hysteria 提交 ca8fbd874fd6ca82413948de6dc5924405e103e1](https://github.com/apernet/hysteria/tree/ca8fbd874fd6ca82413948de6dc5924405e103e1/core/internal/congestion)：`bbr` 中 7 个生产文件、唯一测试文件中的 4 个测试，以及 `common/pacer.go`。BBR（瓶颈带宽与往返传播时间）为第一版算法；没有移入 Hysteria 业务协议或 sing-quic 代码。

使用方式：`NewBbrSender(DefaultClock{}, c.InitialPacketSize(), ProfileStandard)`，安装到 SagerNet 的 `Conn.SetCongestionControl`。没有为项目增加公共档位配置；上游档位定义和解析保留用于原测试及来源对照。

最小适配包括：

- Apernet 的 `congestion`（拥塞控制接口）和 `monotime`（单调时间）导入改为 SagerNet；共用其类型、时间起点和发送前在途字节语义。原 `x/exp/constraints`（泛型约束）依赖保留。
- 增加编译期 `CongestionControlEx`（扩展拥塞控制接口）断言，并接入 `OnPacketNeutered`、`OnPacketsLost`、`OnAppLimited` 三个真实传输回调。丢包字节只在批量拥塞事件计量；清理回调不会重复计量。
- 删除用最大确认包号减 2 猜测最旧未确认包的清理逻辑，由传输提供真实边界。清理只删除严格小于该边界的记录，乱序仍在途的包继续保留。
- 删除未用的环境变量调试输出和基于地址猜初始包尺寸的辅助函数。算法、上游参数、64 KiB/s 发送节奏下限及原 4 测试保持。

`UPSTREAM.sha256` 记录原固定源码的 SHA-256（文件摘要），`ADAPTATION.patch` 记录与原源码的差异。新增 `callback_test.go` 覆盖乱序确认不提前清理、迟到确认不重建历史状态、丢包与移出在途不重复记账、纯确认包不保留记录，以及持续发送时按实际边界清理的有界保留。它们不能替代真实弱网、长期负载和多连接公平性验证。

许可文本完整保留：[Hysteria MIT](LICENSE.HYSTERIA)、[Google QUICHE BSD 三条款](LICENSE.QUICHE)、[quic-go MIT 及 Google 版权](LICENSE.QUIC-GO)。本目录移植遵循这些宽松许可；没有采用 sing-quic/sing 的 GPL 代码或依赖。

已观察的来源历史为：Hysteria [首次加入 45c3fc54bda4ae47af57cea1ce2e3f1f1c7daa45](https://github.com/apernet/hysteria/commit/45c3fc54bda4ae47af57cea1ce2e3f1f1c7daa45) 的 `bbr_sender.go` 第 3 行明确指向 [Google QUICHE 66dea072431f94095dfc3dd2743cb94ef365f7ef](https://quiche.googlesource.com/quiche/+/66dea072431f94095dfc3dd2743cb94ef365f7ef/quic/core/congestion_control/bbr_sender.cc)。Hysteria [重写 844e94d6ca6d78811fbb07cdf5e8fdf66c92fce1](https://github.com/apernet/hysteria/commit/844e94d6ca6d78811fbb07cdf5e8fdf66c92fce1) 早于 Clash.Meta [对应目录首次加入 828b5ad8bb8b33cc8fc256e2a7e29d350570840e](https://github.com/MetaCubeX/Clash.Meta/commit/828b5ad8bb8b33cc8fc256e2a7e29d350570840e)，后者[随后修复明确来自 Hysteria](https://github.com/MetaCubeX/Clash.Meta/commit/9a16eb289565406c3b2a4bd6ec933b920da5037e)；sing-quic 后续才从 Clash.Meta 引入。没有发现 GPL 逆向抄入的明确证据；此记录是提交历史与来源声明核对，未声称逐行法律来源审计。
