package bbr

import (
	"testing"
	"time"

	"github.com/sagernet/quic-go/congestion"
	"github.com/sagernet/quic-go/monotime"
)

type fixedClock struct{ at monotime.Time }

func (c fixedClock) Now() monotime.Time { return c.at }

type fixedRTT struct{}

func (fixedRTT) MinRTT() time.Duration                  { return 50 * time.Millisecond }
func (fixedRTT) LatestRTT() time.Duration               { return 50 * time.Millisecond }
func (fixedRTT) SmoothedRTT() time.Duration             { return 50 * time.Millisecond }
func (fixedRTT) MeanDeviation() time.Duration           { return 5 * time.Millisecond }
func (fixedRTT) MaxAckDelay() time.Duration             { return 25 * time.Millisecond }
func (fixedRTT) PTO(bool) time.Duration                 { return 100 * time.Millisecond }
func (fixedRTT) UpdateRTT(time.Duration, time.Duration) {}
func (fixedRTT) SetMaxAckDelay(time.Duration)           {}
func (fixedRTT) SetInitialRTT(time.Duration)            {}

func callbackSender() *bbrSender {
	b := NewBbrSender(fixedClock{monotime.Time(time.Second)}, 1000, ProfileStandard)
	b.SetRTTStatsProvider(fixedRTT{})
	b.minRtt = 50 * time.Millisecond
	return b
}

func TestCallbacksPreserveReorderedPacketUntilActualCleanup(t *testing.T) {
	b := callbackSender()
	for n := congestion.PacketNumber(1); n <= 5; n++ {
		b.OnPacketSent(monotime.Time(time.Second).Add(time.Duration(n)*time.Millisecond), (congestion.ByteCount(n)-1)*1000, n, 1000, true)
	}
	b.OnCongestionEventEx(5000, monotime.Time(2*time.Second), []congestion.AckedPacketInfo{{PacketNumber: 5, BytesAcked: 1000}}, nil)
	if b.sampler.connectionStateMap.GetEntry(1) == nil {
		t.Fatal("largest ACK discarded an older outstanding packet")
	}
	b.OnPacketsLost(1)
	if b.sampler.connectionStateMap.GetEntry(1) == nil {
		t.Fatal("actual least-unacked packet must remain tracked")
	}
	// A late ACK for the genuinely outstanding packet must still produce a sample.
	b.OnCongestionEventEx(4000, monotime.Time(3*time.Second), []congestion.AckedPacketInfo{{PacketNumber: 1, BytesAcked: 1000}}, nil)
	if b.sampler.TotalBytesAcked() != 2000 {
		t.Fatalf("acked=%d, want 2000", b.sampler.TotalBytesAcked())
	}
	b.OnPacketsLost(2)
	if b.sampler.connectionStateMap.GetEntry(1) != nil || b.sampler.connectionStateMap.GetEntry(2) == nil {
		t.Fatal("cleanup must remove packets strictly below the actual boundary")
	}
}

func TestCallbacksLossNeuteringAndLateACKDoNotDoubleCount(t *testing.T) {
	b := callbackSender()
	b.OnPacketSent(monotime.Time(time.Second), 0, 1, 1000, true)
	b.OnPacketSent(monotime.Time(time.Second).Add(time.Millisecond), 1000, 2, 1000, true)
	b.OnCongestionEventEx(2000, monotime.Time(2*time.Second), nil, []congestion.LostPacketInfo{{PacketNumber: 2, BytesLost: 1000}})
	if !b.hasNoAppLimitedSample || b.sampler.connectionStateMap.GetEntry(2) == nil {
		t.Fatal("loss event did not consume its valid send-state before cleanup")
	}
	b.OnPacketNeutered(1)
	b.OnPacketNeutered(1) // Repeated notifications cannot count an absent packet.
	b.OnPacketsLost(3)
	b.OnPacketsLost(3)
	if b.sampler.TotalBytesLost() != 1000 || b.sampler.TotalBytesNeutered() != 1000 {
		t.Fatalf("lost=%d neutered=%d, want 1000 each", b.sampler.TotalBytesLost(), b.sampler.TotalBytesNeutered())
	}
	if !b.sampler.connectionStateMap.IsEmpty() || b.sampler.connectionStateMap.EntrySlotsUsed() != 0 {
		t.Fatal("no obsolete packet slots should remain")
	}
	// SagerNet does not report already-lost ACKs as a fresh congestion event.
	// The sampler must nevertheless ignore such an ACK after its state was removed.
	late := b.sampler.onPacketAcknowledged(monotime.Time(3*time.Second), 2)
	if late.stateAtSend.isValid || b.sampler.TotalBytesAcked() != 0 || !b.sampler.connectionStateMap.IsEmpty() {
		t.Fatal("late ACK recreated removed state or counted bytes")
	}
}

func TestCallbacksACKOnlyAndSustainedRetentionStayBounded(t *testing.T) {
	b := callbackSender()
	const rounds = 10000
	const livePackets = 8
	for n := congestion.PacketNumber(1); n <= rounds; n++ {
		at := monotime.Time(time.Second).Add(time.Duration(n) * time.Millisecond)
		b.OnPacketSent(at, 0, n, 40, false)
	}
	if b.sampler.TotalBytesSent() != 0 || b.sampler.connectionStateMap.EntrySlotsUsed() != 0 {
		t.Fatal("ACK-only packets retained state or contributed sampled bytes")
	}
	for n := congestion.PacketNumber(rounds + 1); n <= 2*rounds; n++ {
		at := monotime.Time(time.Second).Add(time.Duration(n) * time.Millisecond)
		b.OnPacketSent(at, 7000, n, 1000, true)
		// Model the transport retaining at most the most recent eight packets.
		b.OnPacketsLost(n - livePackets + 1)
		if got := b.sampler.connectionStateMap.EntrySlotsUsed(); got > livePackets {
			t.Fatalf("after packet %d retained %d slots, want <=%d", n, got, livePackets)
		}
	}
	b.OnPacketsLost(2*rounds + 1)
	if b.sampler.connectionStateMap.EntrySlotsUsed() != 0 {
		t.Fatal("terminal cleanup retained historical packet slots")
	}
}

func TestAppLimitedCallbackUsesTransportFlightAndMarksSamples(t *testing.T) {
	b := callbackSender()
	b.OnPacketSent(monotime.Time(time.Second), 0, 1, 1000, true)
	b.OnAppLimited(b.GetCongestionWindow())
	if b.sampler.IsAppLimited() {
		t.Fatal("a full congestion window is not application-limited")
	}
	b.OnAppLimited(1000)
	if !b.sampler.IsAppLimited() || b.sampler.EndOfAppLimitedPhase() != 1 {
		t.Fatal("empty-packer callback did not mark the sent-packet boundary")
	}
	b.OnPacketSent(monotime.Time(time.Second).Add(time.Millisecond), 1000, 2, 1000, true)
	if p := b.sampler.connectionStateMap.GetEntry(2); p == nil || !p.sendTimeState.isAppLimited {
		t.Fatal("new sample did not inherit the application-limited marker")
	}
}
