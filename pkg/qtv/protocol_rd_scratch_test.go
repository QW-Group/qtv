package qtv

import (
	"reflect"
	"testing"
)

// Starting a new update slice for each message must fail this regression.
func TestRDSteadyDecodeAllocations(t *testing.T) {
	q := rdTestProtocol()
	body := append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 4, 1, 2, 0, 128, 4, 2})...)
	if err := q.readMessage(body, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	body = append([]byte{86}, rdTestFrame([]byte{1, 0, 128, 4, 3, 2, 0, 128, 4, 4})...)
	allocations := testing.AllocsPerRun(100, func() {
		if err := q.readMessage(body, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("steady RD decoding allocates %g times per message", allocations)
	}
	if q.rd.current[1].modelIndex != 3 || q.rd.current[2].modelIndex != 4 {
		t.Fatal("lost decoded state")
	}
}

func TestRDScratchLifetime(t *testing.T) {
	q := rdTestProtocol()
	if e := q.readMessage(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 4, 1, 2, 0, 128, 4, 2})...), mvdMsgRead, playersMaskAll); e != nil {
		t.Fatal(e)
	}
	before := append([]rdEntityState(nil), q.rd.current...)
	present := append([]bool(nil), q.rd.present...)
	tm := q.rd.time
	// A reset and a valid first change precede a truncated second change. None commits.
	if e := q.readMessage(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 4, 7, 2, 0, 128})...), mvdMsgRead, playersMaskAll); e == nil {
		t.Fatal("accepted truncated second entity")
	}
	if !reflect.DeepEqual(before, q.rd.current) || !reflect.DeepEqual(present, q.rd.present) || q.rd.time != tm {
		t.Fatal("failed frame changed retained world")
	}
	// A smaller following update must not apply old records in scratch's unused tail.
	if e := q.readMessage(append([]byte{86}, rdTestFrame([]byte{1, 0, 128, 4, 3})...), mvdMsgRead, playersMaskAll); e != nil {
		t.Fatal(e)
	}
	if q.rd.current[1].modelIndex != 3 || q.rd.current[2].modelIndex != 2 {
		t.Fatal("scratch tail leaked into following frame")
	}
	saved := q.rd.current[1]
	if e := q.readMessage(append([]byte{86}, rdTestFrame([]byte{2, 0, 128, 4, 4})...), mvdMsgRead, playersMaskAll); e != nil {
		t.Fatal(e)
	}
	if q.rd.current[1] != saved {
		t.Fatal("later scratch reuse aliased previous state")
	}
	if cap(q.rdUpdates) == 0 {
		t.Fatal("decoder did not retain scratch")
	}
	q.protocolReaderData.reset()
	if q.rdUpdates != nil || q.rd != nil {
		t.Fatal("map reset retained old decode scratch/world")
	}
}

func TestRDIndependentFramesAndSources(t *testing.T) {
	first, err := readRDFrame(newNetMsgR(rdTestFrame([]byte{1, 0, 128, 4, 1}), false), false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := readRDFrame(newNetMsgR(rdTestFrame([]byte{1, 0, 128, 4, 2}), false), false)
	if err != nil {
		t.Fatal(err)
	}
	second.updates[0].delta.state.modelIndex = 3
	if first.updates[0].delta.state.modelIndex != 1 {
		t.Fatal("independent reader result was overwritten")
	}
	a, b := rdTestProtocol(), rdTestProtocol()
	for i, q := range []*qProtocol{a, b} {
		body := append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 4, byte(i + 1)})...)
		if err := q.readMessage(body, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
	}
	if a.rd.current[1].modelIndex != 1 || b.rd.current[1].modelIndex != 2 {
		t.Fatal("sources shared retained values")
	}
	if &a.rdUpdates[:1][0] == &b.rdUpdates[:1][0] {
		t.Fatal("sources shared decode storage")
	}
	b.reset()
	if b.rdUpdates != nil || b.rd != nil || a.rd.current[1].modelIndex != 1 {
		t.Fatal("source reconnect corrupted ownership")
	}
	// Real serverdata processing must release both the world and its scratch.
	a.us.wb = nil
	if err := a.readMessage(rdTestServerData(0, 0, 0), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if a.rdUpdates != nil || a.rd != nil {
		t.Fatal("legacy map retained RD storage")
	}
}

// Incomplete records must never leak through the returned prefix or scratch reuse.
func TestRDDecodeCompletedPrefix(t *testing.T) {
	records := []byte{0, 128, 1, 0, 128, 4, 7, 2, 0, 128, 4, 9}
	full := rdTestFrame(records)
	var scratch []rdUpdate
	for end := 11; end < len(full)-2; end++ {
		f, err := readRDFrameInto(newNetMsgR(full[:end], false), false, scratch)
		if err == nil {
			t.Fatalf("accepted truncated input at %d", end)
		}
		if len(f.updates) != 1 || f.updates[0].number != 1 || f.updates[0].delta.state.modelIndex != 7 {
			t.Fatalf("incomplete entity leaked at %d: %+v", end, f.updates)
		}
		scratch = f.updates[:0]
	}
	f, err := readRDFrameInto(newNetMsgR(rdTestFrame([]byte{0, 128, 1, 0, 0}), false), false, scratch)
	if err != nil || len(f.updates) != 1 || f.updates[0].delta.state.modelIndex != 0 {
		t.Fatal("reused decode retained stale fields", err, f)
	}
}
