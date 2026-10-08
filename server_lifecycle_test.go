package statesync_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
)

type gatedInitialization struct {
	*arena.Game
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (g *gatedInitialization) Entities() []syncnet.Entity {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.Game.Entities()
}

type creationResult struct {
	room *syncnet.Room
	err  error
}

func beginInitialization(t *testing.T, f fixture) (<-chan creationResult, func()) {
	t.Helper()
	g := &gatedInitialization{Game: arena.New(), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(g.release) }) }
	t.Cleanup(release)
	result := make(chan creationResult, 1)
	go func() {
		r, err := f.s.CreateRoom("pending", g)
		result <- creationResult{r, err}
	}()
	select {
	case <-g.entered:
	case <-time.After(time.Second):
		release()
		t.Fatal("initialization did not start")
	}
	return result, release
}

func TestRoomInitializationDoesNotBlockOtherJoins(t *testing.T) {
	f := setup(t)
	result, release := beginInitialization(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c, err := syncnet.Dial(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{})
	release()
	created := <-result
	if created.err != nil {
		t.Fatal(created.err)
	}
	if c != nil {
		defer c.Close()
	}
	if err != nil {
		t.Fatal("unrelated room join blocked by initialization", err)
	}
}

func TestPendingRoomReservesNameAndCapacity(t *testing.T) {
	f := setup(t)
	result, release := beginInitialization(t, f)
	checked := make(chan error, 1)
	go func() {
		if _, err := f.s.CreateRoom("pending", arena.New()); err == nil {
			checked <- errors.New("duplicate initializing room accepted")
			return
		}
		for i := 0; i < 62; i++ {
			if _, err := f.s.CreateRoom(fmt.Sprintf("extra-%d", i), arena.New()); err != nil {
				checked <- err
				return
			}
		}
		if _, err := f.s.CreateRoom("overflow", arena.New()); err == nil {
			checked <- errors.New("initializing room did not count toward capacity")
			return
		}
		checked <- nil
	}()
	select {
	case err := <-checked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialization blocked registry operations")
	}
	release()
	if created := <-result; created.err != nil {
		t.Fatal(created.err)
	}
}

func TestCanceledInitializationCannotDeleteReplacement(t *testing.T) {
	f := setup(t)
	result, release := beginInitialization(t, f)
	closed := make(chan error, 1)
	go func() { closed <- f.s.CloseRoom("pending") }()
	replaced := make(chan creationResult, 1)
	go func() {
		deadline := time.Now().Add(time.Second)
		for {
			r, err := f.s.CreateRoom("pending", arena.New())
			if err == nil || time.Now().After(deadline) {
				replaced <- creationResult{r, err}
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case replacement := <-replaced:
		if replacement.err != nil {
			t.Fatal(replacement.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement blocked by canceled initializer")
	}
	select {
	case <-closed:
		t.Fatal("CloseRoom returned before initialization finished")
	default:
	}
	release()
	if created := <-result; !errors.Is(created.err, syncnet.ErrClosed) {
		t.Fatal("canceled creation succeeded", created.err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	c := connect(t, f, f.s.Addr().String(), "pending")
	if c.Err() != nil {
		t.Fatal("old initialization removed replacement", c.Err())
	}
}

func TestServerCloseWaitsForPendingInitialization(t *testing.T) {
	f := setup(t)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	result, release := beginInitialization(t, f)
	stopped := make(chan struct{})
	go func() { f.s.Close(); close(stopped) }()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("server shutdown did not close existing clients")
	}
	rejected := make(chan error, 1)
	go func() { _, err := f.s.CreateRoom("late", arena.New()); rejected <- err }()
	select {
	case err := <-rejected:
		if !errors.Is(err, syncnet.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown held registry lock while waiting")
	}
	select {
	case <-stopped:
		t.Fatal("server returned before initializer finished")
	default:
	}
	release()
	if created := <-result; !errors.Is(created.err, syncnet.ErrClosed) {
		t.Fatal("pending creation survived shutdown", created.err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("server failed to finish shutdown")
	}
}

type leaveManagementGame struct {
	*arena.Game
	onLeave func()
}

func (g *leaveManagementGame) Leave(id uint32) []syncnet.Change {
	g.onLeave()
	return g.Game.Leave(id)
}

func TestServerCloseAllowsManagementFromCleanup(t *testing.T) {
	g := &leaveManagementGame{Game: arena.New()}
	f := configured(t, syncnet.DefaultConfig(), g)
	callback := make(chan error, 1)
	g.onLeave = func() {
		result := make(chan error, 1)
		go func() { _, err := f.s.CreateRoom("after-close", arena.New()); result <- err }()
		select {
		case err := <-result:
			callback <- err
		case <-time.After(500 * time.Millisecond):
			callback <- errors.New("cleanup callback blocked on registry lock")
		}
	}
	connect(t, f, f.s.Addr().String(), "alpha")
	f.s.Close()
	if err := <-callback; !errors.Is(err, syncnet.ErrClosed) {
		t.Fatal(err)
	}
}

func TestServerShutdownReportsTerminalError(t *testing.T) {
	g := &countedGame{Game: arena.New(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	f := configured(t, syncnet.DefaultConfig(), g)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	var once sync.Once
	release := func() { once.Do(func() { close(g.release) }) }
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	action := make(chan error, 1)
	go func() { _, err := c.Action(ctx, 1, arena.Pickup(1, 1)); action <- err }()
	select {
	case <-g.entered:
	case <-ctx.Done():
		t.Fatal("action did not enter game")
	}
	// Hold the room callback so connection shutdown must work independently.
	stopped := make(chan struct{})
	go func() { f.s.Close(); close(stopped) }()
	select {
	case <-c.Done():
	case <-ctx.Done():
		t.Fatal("server did not close client")
	}
	if err := c.Err(); !errors.Is(err, syncnet.ErrClosed) {
		t.Errorf("server shutdown is not terminal: %v", err)
	}
	select {
	case err := <-action:
		if !errors.Is(err, syncnet.ErrClosed) {
			t.Errorf("pending operation lost shutdown reason: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("pending operation did not stop")
	}
	release()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("shutdown did not finish after callback returned")
	}
	if g.leaves.Load() != 1 || f.r.Err() != nil {
		t.Fatal("shutdown cleanup changed", g.leaves.Load(), f.r.Err())
	}
}
