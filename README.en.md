# Go StateSync

**Server-authoritative multiplayer state synchronization in Go.**

[![CI](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml/badge.svg)](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Protocol](https://img.shields.io/badge/Protocol-v3-5268e8)](PROTOCOL.md)

[中文](README.md) · [Integration guide](docs/INTEGRATION.md) · [Protocol](PROTOCOL.md) · [Session resume](RESUME.md) · [Development validation](reports/updates/2026-10-10-weaknet/VALIDATION.md)

An engine-independent core for rooms of up to 16 players, with a standalone server, headless reference client, a 2D movement and pickup example, and weak-network tests over real encrypted connections. Detailed technical documents are currently in Chinese.

The current development tree targets **v0.3.0 / wire protocol v3** and is not published. Released **v0.2.1** uses protocol v2. Upgrade clients and servers together; see [migration](MIGRATION.md).

## Features

| Capability | Behavior |
|---|---|
| Authoritative rooms | One owner loop per room; bounded queues and fixed simulation steps |
| Prediction and reconciliation | Predict local input immediately, then restore authoritative state and replay pending input |
| Remote interpolation | A configurable 200 ms default buffer; hold the latest state when the buffer runs out |
| In-process session resume | A 60-second default grace period, immediate connection takeover, and replay of the latest action result |
| Lifecycle and fault isolation | Reliable entity changes and actions; an invalid batch or recoverable panic ends only its room |
| Bounded backpressure | Replace stale snapshots; disconnect slow reliable-stream consumers |
| Lossless delta snapshots | Independent decoding against confirmed immutable reliable baselines; full-record fallback when needed |

The pinned SagerNet quic-go transport uses an internal standard BBRv1 controller on the server, with the same strategy for both snapshot encodings. QUIC reliable streams carry joins, full state, lifecycle changes, baseline acknowledgments, actions, resynchronization, and resume. Datagrams carry redundant movement input and full or delta motion records. Each entity record can be restored and applied independently. Games explicitly define input and state codecs; the core does not use reflection or engine APIs.

Defaults: **30 simulation steps/s**, **15 snapshots/s**, and **1000-byte maximum datagrams**. See the [integration guide](docs/INTEGRATION.md) for bounds and configuration.

## Quick start

Requires Go 1.26 or newer.

```sh
git clone https://github.com/qihai-coding/go-statesync.git
cd go-statesync
go mod download
go run ./cmd/server -generate-cert
go run ./cmd/server -rooms alpha,beta
```

Start these clients in separate terminals:

```sh
go run ./cmd/client -room alpha -x 1 -duration 10s
go run ./cmd/client -room alpha -y 1 -duration 10s
```

To demonstrate pickup and session resume:

```sh
go run ./cmd/client -room beta -pickup 1 -x 1 -duration 8s -disconnect-after 2s
```

The local certificate lasts 24 hours, and generation does not overwrite existing files. Use new `-cert`, `-key`, and client `-ca` paths for subsequent runs. Certificate verification stays enabled.

## Use as a library

```sh
go get github.com/qihai-coding/go-statesync@v0.2.1
```

Implement `Game` for server simulation and `Model` for prediction and codecs, using an independent game instance per room. Start with the [2D example](arena/arena.go), then follow the [integration guide](docs/INTEGRATION.md).

The installation command above fetches the released v2 library; v3 is available in this working tree. `Config.SnapshotEncoding` defaults to `DeltaSnapshots`; `FullSnapshots` provides a same-protocol bandwidth control. `SampleWithInfo` exposes the actual source time of rendered states.

Resume is explicitly initiated by the application and returns a **new client object**:

```go
token := client.ResumeToken()
client.Disconnect()
next, err := statesync.DialResume(ctx, address, room, tlsConfig, model, token)
if err != nil {
    return err
}
client = next
```

Never log resume credentials. Retry an unconfirmed action with its original ID and payload. `LastActionID` includes failed actions and does not prove business success. See [session resume](RESUME.md) for cancellation, takeover, and idempotency rules.

## Validation

```sh
go test -count=1 ./...
go vet ./...
go test -race -count=1 ./...
go run ./cmd/check -isolate -rooms 8 -clients 16 -duration 10m -resume-every 30s -report soak.json
```

Race detection requires a compatible C compiler. [CI](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml) runs native tests and builds on Windows, Linux, and macOS, with race detection and six fuzz targets on Linux.

[Development validation](reports/updates/2026-10-10-weaknet/VALIDATION.md) records same-protocol comparisons, display age, recovery, and resources for this working tree. [Initial development validation](reports/development-v0.3.0/VALIDATION.md) preserves the earlier weak-network failures; [v0.2.1 validation](reports/release-v0.2.1/VALIDATION.md) remains evidence for the published version. The room-step target is **p99 below 10 ms**. Server and client/proxy resource measurements exclude the coordinator. Total application bytes include both directions, initial synchronization, and resume; transport overhead is excluded. See [measurement methodology](docs/MEASUREMENT.md).

With 16 clients, 256 dynamic entities, 32-byte sparse states, and unchanged 30/15 Hz tick/snapshot rates, normal-network total bytes fell by 54.79%. All four five-minute weak-network pairs (150/300 ms RTT, seeds 7/701) saved 55.70%–56.09%, exceeding the 50% target. Worst per-entity p95 display age stayed at or below 0.35 seconds; every weak-network member recovered within 0.29 seconds without reliable state correction. The report preserves raw bytes, expected/submitted/applied updates, and resource evidence.

## Boundaries

- Each session controls one dynamic entity; respawn and ownership transfer are outside this release.
- Resume works only within a running server process. Credentials expire after restart; application code supplies authentication, persistence, matchmaking, and cross-server routing.
- Game callbacks must return quickly without blocking. Recoverable panics are isolated; process termination and indefinitely blocked callbacks are not.
- Delta savings depend on state changes. The target for the 32-byte sparse workload does not apply to the 13-byte demo or high-entropy states. Interest management is not implemented.

## Documentation and contributing

- [Integration guide](docs/INTEGRATION.md)
- [Protocol and golden packets](PROTOCOL.md)
- [Protocol v3 migration](MIGRATION.md)
- [Isolated measurement](docs/MEASUREMENT.md)
- [Session resume](RESUME.md)
- [Validation report index](reports/README.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)

Design references: [Nakama room scheduling](https://github.com/heroiclabs/nakama/blob/master/server/match_handler.go), [Mirror interpolation](https://github.com/MirrorNetworking/Mirror/blob/master/Assets/Mirror/Core/SnapshotInterpolation/SnapshotInterpolation.cs), [Colyseus state encoding](https://github.com/colyseus/schema), and [client prediction and reconciliation](https://www.gabrielgambetta.com/client-side-prediction-server-reconciliation.html). The synchronization core is independently written; those frameworks are not runtime dependencies. The internal congestion controller has separate [upstream attribution and adaptation notes](internal/bbr/README.md).

Licensed under the [MIT License](LICENSE), with [original notices and licenses](internal/bbr/README.md) retained for the internal controller.
