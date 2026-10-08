package statesync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// These rooms have no players, so only initialization and Step are called.
type releaseGame struct {
	Game
	initial func() []Entity
	step    func() []Change
}

func (g releaseGame) Entities() []Entity                   { return g.initial() }
func (g releaseGame) Step(float32, []PlayerInput) []Change { return g.step() }

func assertRoomReleased(t *testing.T, r *Room) {
	t.Helper()
	if r.game != nil || r.entities != nil || r.players != nil || r.commands != nil || r.inputs != nil {
		t.Fatal("ended room retains game, entities, sessions or queue references")
	}
	for range 100 {
		if _, _, err := r.Inspect(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatal("inspection after shutdown", err)
		}
		if r.command(roomCommand{}) {
			t.Fatal("command accepted after shutdown")
		}
		if r.enqueueInputs(inputBatch{}) {
			t.Fatal("input accepted after shutdown")
		}
	}
}

func TestRoomReleaseAfterInitializationFailure(t *testing.T) {
	for _, failure := range []string{"invalid entity", "panic"} {
		t.Run(failure, func(t *testing.T) {
			g := releaseGame{initial: func() []Entity {
				if failure == "panic" {
					panic("initialization failed")
				}
				return []Entity{{ID: 1, Generation: 1, State: []byte{1}}, {}}
			}}
			r := newRoom(context.Background(), DefaultConfig(), g)
			if err := r.start(); err == nil {
				t.Fatal("initialization unexpectedly succeeded")
			}
			select {
			case <-r.Done():
			default:
				t.Fatal("failed initialization did not finish")
			}
			if r.Err() == nil {
				t.Fatal("initialization cause lost")
			}
			assertRoomReleased(t, r)
		})
	}
}

func TestRoomReleaseWithPendingMessages(t *testing.T) {
	for _, failure := range []string{"close", "panic"} {
		t.Run(failure, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			g := releaseGame{
				initial: func() []Entity { return []Entity{{ID: 1, Generation: 1, State: []byte{1}}} },
				step: func() []Change {
					enteredOnce.Do(func() { close(entered) })
					<-release
					if failure == "panic" {
						panic("step failed")
					}
					return nil
				},
			}
			r := newRoom(context.Background(), DefaultConfig(), g)
			if err := r.start(); err != nil {
				t.Fatal(err)
			}
			defer func() { unblock(); r.Close() }()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("room did not step")
			}
			// Freeze the owner loop while filling both bounded queues.
			for range cap(r.commands) {
				if !r.command(roomCommand{inspect: make(chan inspectResult, 1), data: make([]byte, MaxAction)}) {
					t.Fatal("control queue filled early")
				}
			}
			for range cap(r.inputs) {
				if !r.enqueueInputs(inputBatch{peer: &peer{id: 999}, inputs: []Input{{Sequence: 1, Data: []byte{1}}}}) {
					t.Fatal("input admission stopped early")
				}
			}
			if !r.enqueueInputs(inputBatch{}) || r.inputDrops.Load() != 1 || len(r.inputs) != cap(r.inputs) {
				t.Fatal("full input queue must drop without growing or closing admission")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			results := make(chan error, 16)
			for range cap(results) {
				go func() { _, _, err := r.Inspect(ctx); results <- err }()
			}
			// All three admission paths can race shutdown. These peers have no
			// matching player and must be discarded before accessing transport.
			var producers sync.WaitGroup
			producers.Add(8)
			for range 8 {
				go func() {
					defer producers.Done()
					for r.enqueueInputs(inputBatch{peer: &peer{id: 999}}) {
						r.command(roomCommand{inspect: make(chan inspectResult, 1)})
					}
				}()
			}
			if failure == "close" {
				go r.Close()
				<-r.ctx.Done()
			}
			unblock()
			select {
			case <-r.Done():
			case <-ctx.Done():
				t.Fatal("shutdown blocked behind full queues or waiting inspections")
			}
			for range cap(results) {
				if err := <-results; !errors.Is(err, ErrClosed) {
					t.Fatal("pending inspection did not observe closure", err)
				}
			}
			producers.Wait()
			if failure == "close" && r.Err() != nil || failure == "panic" && (r.Err() == nil || !strings.Contains(r.Err().Error(), "step failed")) {
				t.Fatal("unexpected termination cause", r.Err())
			}
			assertRoomReleased(t, r)
		})
	}
}
