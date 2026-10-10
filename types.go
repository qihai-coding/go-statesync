// Package statesync provides authoritative rooms and a transport-independent game API.
package statesync

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	ProtocolVersion byte = 3
	ALPN                 = "statesync/3"
	MaxState             = 512
	MaxInput             = 64
	MaxAction            = 512
	MaxEntities          = 256
	MaxFrame             = 256 * 1024
	HistorySize          = 128
	inputWindow          = 256
)

var ErrClosed = errors.New("statesync: closed")
var ErrProtocol = errors.New("statesync: invalid protocol")
var ErrBusy = errors.New("statesync: queue full")
var ErrResumeRejected = errors.New("statesync: resume rejected")
var ErrSessionReplaced = errors.New("statesync: session replaced")

// ResumeToken is a bearer credential; never include its bytes in logs.
type ResumeToken [32]byte

func (ResumeToken) String() string   { return "[redacted]" }
func (ResumeToken) GoString() string { return "ResumeToken([redacted])" }

type SnapshotEncoding uint8

const (
	DeltaSnapshots SnapshotEncoding = iota
	FullSnapshots
)

type Config struct {
	TickRate          int
	SnapshotRate      int
	MaxPlayers        int
	DatagramSize      int
	ResumeGracePeriod time.Duration
	SnapshotEncoding  SnapshotEncoding
}

func DefaultConfig() Config {
	return Config{TickRate: 30, SnapshotRate: 15, MaxPlayers: 16, DatagramSize: 1000, ResumeGracePeriod: time.Minute}
}
func (c Config) validate() error {
	if c.TickRate < 1 || c.TickRate > 120 || c.SnapshotRate < 1 || c.SnapshotRate > c.TickRate || c.MaxPlayers < 1 || c.MaxPlayers > 16 || c.DatagramSize < 600 || c.DatagramSize > 1000 || c.ResumeGracePeriod < 0 || c.SnapshotEncoding > FullSnapshots {
		return fmt.Errorf("invalid config: %+v", c)
	}
	return nil
}

// State is an immutable, game-encoded value once returned to the framework.
type Entity struct {
	ID         uint32
	Generation uint32
	Owner      uint32
	Dynamic    bool
	Ack        uint64 // Filled by the server; all inputs <= Ack have been resolved.
	State      []byte
}
type Change struct {
	Delete bool
	Entity Entity
}
type PlayerInput struct {
	Player uint32
	Data   []byte
}
type Input struct {
	Sequence uint64
	Data     []byte
}

// After the initial Entities call, Game methods execute on the room's owning goroutine.
// Returned slices must not subsequently be modified by the game.
// Join/Action errors must leave game state unchanged.
type Game interface {
	Join(player uint32) ([]Change, error)
	Leave(player uint32) []Change
	ValidateInput(data []byte) error
	Step(dt float32, inputs []PlayerInput) []Change
	Action(player uint32, data []byte) ([]Change, []byte, error)
	Entities() []Entity
}

// Model supplies the same movement rules and codec to the reference client.
// Implementations must return owned byte slices and must not modify arguments.
type Model interface {
	ValidateInput([]byte) error
	ValidateState([]byte) error
	Predict(state, input []byte, dt float32) ([]byte, error)
	Interpolate(a, b []byte, fraction float32) []byte
}

type View struct {
	Tick       uint64
	Revision   uint64
	Player     uint32
	Entities   []Entity
	ServerTime time.Duration
}
type SampleInfo struct {
	SourceTime, LatestStateTime time.Duration
	Holding, Predicted          bool
}
type ClientStats struct {
	EntityUpdates, ReliableChanges, FullStates uint64
	BaselineEntities, BaselineBytes            int
}
type Metrics struct {
	Ticks                 uint64
	StepTotal             time.Duration
	StepP99               time.Duration
	StepMax               time.Duration
	InputDrops            uint64
	SnapshotDrops         uint64
	ReliableBytes         uint64
	DatagramBytes         uint64
	Players               int
	RetainedPlayers       int
	InputQueue            int
	ControlQueue          int
	ReliableQueued        int
	SnapshotQueued        int
	BaselineEntities      int
	BaselineBytes         int
	DeltaBytes            uint64
	FullRecordBytes       uint64
	SnapshotRecords       uint64
	DeltaRecords          uint64
	FullRecords           uint64
	ExpectedEntityUpdates uint64
	SentEntityUpdates     uint64
}
type inspectResult struct {
	view    View
	metrics Metrics
}
type roomCommand struct {
	peer    *peer
	kind    byte
	data    []byte
	reply   chan error
	inspect chan inspectResult
	token   ResumeToken
}

func cloneEntity(e Entity) Entity { e.State = append([]byte(nil), e.State...); return e }
func validEntity(e Entity) bool   { return e.ID != 0 && e.Generation != 0 && len(e.State) <= MaxState }
func waitError(ctx context.Context, done <-chan struct{}, reply <-chan error) error {
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrClosed
	}
}
