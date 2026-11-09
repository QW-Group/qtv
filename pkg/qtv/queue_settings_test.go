package qtv

import (
	"fmt"
	"testing"
)

func TestQueueSettingsValidation(t *testing.T) {
	q := &QTV{}
	q.cmd = newQCmd(q)
	q.qvs = newQVarStorage(q)
	q.qvs.RegEx("queue", 32768, 0, onChangeQueueSize)
	for _, s := range []string{"0", "-1", "1.5", "NaN", "2147483648", "", "1e3"} {
		q.qvs.Set("queue", s)
		if q.qvs.Get("queue").Int != 32768 {
			t.Fatal(s)
		}
	}
	q.qvs.Set("queue", "2147483647")
	if q.qvs.Get("queue").Int != 2147483647 {
		t.Fatal("arm32 boundary")
	}
	q.qvs.Set("queue", fmt.Sprint(64))
	old := mustByteQueue(q.qvs.Get("queue").Int, 8)
	q.qvs.Set("queue", "128")
	if old.Capacity() != 64 || mustByteQueue(q.qvs.Get("queue").Int, 8).Capacity() != 128 {
		t.Fatal("mutated existing budget")
	}
}
