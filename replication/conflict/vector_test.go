package conflict

import (
	"testing"
)

func TestVectorConcurrentEventsDoNotHappenBefore(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	v1.vector[0] = 0
	v1.vector[3] = 2
	v2.vector[0] = 1

	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1 due to v1[3]")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2 due to v2[0] > v1[0]")
	}
}

func TestVectorHappensBeforeBothEmpty(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	if v2.HappensBefore(v1) {
		t.Errorf("v1 and v2 are both empty")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 and v2 are both empty")
	}
}

func TestVectorHappensBeforeEqualVectors(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	v1.vector[0] = 0
	v1.vector[1] = 1
	v2.vector[0] = 0
	v2.vector[1] = 1
	if v2.HappensBefore(v1) {
		t.Errorf("v1 and v2 are equal")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 and v2 are equal")
	}
}

func TestVectorHappensBeforeZeroAndEmptyVectors(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	v1.vector[0] = 0
	v1.vector[1] = 0
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2 since all v2 entries are 0")
	}
	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1 since v2 entries are 0")
	}
}
