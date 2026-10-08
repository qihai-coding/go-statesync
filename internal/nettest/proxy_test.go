package nettest

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestInvalidProbabilitiesRejected(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -.01, 1.01} {
		for _, field := range []string{"loss", "duplicate", "reorder"} {
			t.Run(fmt.Sprintf("%s/%v", field, value), func(t *testing.T) {
				v := Profile{}
				switch field {
				case "loss":
					v.Loss = value
				case "duplicate":
					v.Duplicate = value
				case "reorder":
					v.Reorder = value
				}
				original := Profile{RTT: 150 * time.Millisecond, Loss: .05}
				p := &Proxy{profile: original}
				if err := p.Set(v); err == nil {
					t.Error("invalid update accepted")
				}
				if p.profile != original {
					t.Error("invalid update changed the active profile")
				}
				created, err := New("127.0.0.1:9", 1, v)
				if created != nil {
					created.Close()
				}
				if err == nil || created != nil {
					t.Error("invalid initial profile accepted")
				}
			})
		}
	}
}

func TestProfileBoundaryValues(t *testing.T) {
	p := &Proxy{}
	for _, v := range []Profile{{}, {RTT: 10 * time.Second, Jitter: time.Second, Loss: 1, Duplicate: 1, Reorder: 1}} {
		if err := p.Set(v); err != nil {
			t.Fatal("valid boundary rejected", err)
		}
		if p.profile != v {
			t.Fatal("valid update not applied")
		}
	}
}
