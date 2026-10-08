# Go StateSync

**Server-authoritative multiplayer state synchronization in Go.**

[![CI](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml/badge.svg)](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Protocol](https://img.shields.io/badge/Protocol-v2-5268e8)](PROTOCOL.md)

[中文](README.md) · [Integration guide](docs/INTEGRATION.md) · [Protocol](PROTOCOL.md) · [Session resume](RESUME.md) · [Validation](reports/release-v0.2.0/VALIDATION.md)

An engine-independent core for rooms of up to 16 players, with a standalone server, headless reference client, a 2D movement and pickup example, and weak-network tests over real encrypted connections. Detailed technical documents are currently in Chinese.

Release **v0.2.0** uses **wire protocol v2**. Library and protocol versions are separate; clients and servers must use the same wire protocol.

## Features

| Capability | Behavior |
|---|---|
| Authoritative rooms | One owner loop per room; bounded queues and fixed simulation steps |
| Prediction and reconciliation | Predict local input immediately, then restore authoritative state and replay pending input |
| Remote interpolation | A configurable 200 ms default buffer; hold the latest state when the buffer runs out |
| In-process session resume | A 60-second default grace period, immediate connection takeover, and replay of the latest action result |
| Lifecycle and fault isolation | Reliable entity changes and actions; an invalid batch or recoverable panic ends only its room |
| Bounded backpressure | Replace stale snapshots; disconnect slow reliable-stream consumers |

QUIC reliable streams carry joins, full state, lifecycle changes, actions, resynchronization, and resume. Datagrams carry redundant movement input and full motion records. Each entity record can be applied independently. Games explicitly define input and state codecs; the core does not use reflection or engine APIs.

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
go get github.com/qihai-coding/go-statesync@v0.2.0
```

Implement `Game` for server simulation and `Model` for prediction and codecs, using an independent game instance per room. Start with the [2D example](arena/arena.go), then follow the [integration guide](docs/INTEGRATION.md).

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
go run ./cmd/check -rooms 8 -clients 16 -duration 10m -resume-every 30s -report soak.json
```

Race detection requires a compatible C compiler. [CI](https://github.com/qihai-coding/go-statesync/actions/workflows/ci.yml) runs native tests and builds on Windows, Linux, and macOS, with race detection and four fuzz targets on Linux.

[Release validation](reports/release-v0.2.0/VALIDATION.md) records the 8-room, 128-client, ten-minute workload, three weak-network profiles, resume, resources, and application payload bandwidth. The room-step target is **p99 below 10 ms**, not a hard bound on every step. CPU and memory measurements include both the server and reference clients in one process; reported bandwidth excludes transport overhead.

## Boundaries

- Each session controls one dynamic entity; respawn and ownership transfer are outside this release.
- Resume works only within a running server process. Credentials expire after restart; application code supplies authentication, persistence, matchmaking, and cross-server routing.
- Game callbacks must return quickly without blocking. Recoverable panics are isolated; process termination and indefinitely blocked callbacks are not.
- Motion snapshots are full entity records. Delta compression and interest management are not implemented.

## Documentation and contributing

- [Integration guide](docs/INTEGRATION.md)
- [Protocol and golden packets](PROTOCOL.md)
- [Session resume](RESUME.md)
- [Validation report index](reports/README.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)

Design references: [Nakama room scheduling](https://github.com/heroiclabs/nakama/blob/master/server/match_handler.go), [Mirror interpolation](https://github.com/MirrorNetworking/Mirror/blob/master/Assets/Mirror/Core/SnapshotInterpolation/SnapshotInterpolation.cs), [Colyseus state encoding](https://github.com/colyseus/schema), and [client prediction and reconciliation](https://www.gabrielgambetta.com/client-side-prediction-server-reconciliation.html). This implementation is independently written; those frameworks are not runtime dependencies.

Licensed under the [MIT License](LICENSE).
