package conflict

import (
	"testing"
	"time"
)

func testCreatePhysicalClockConflictResolver() *PhysicalClockConflictResolver {
	return &PhysicalClockConflictResolver{}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClock() PhysicalClock {
	return PhysicalClock{timestamp: uint64(time.Now().UnixNano())}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClockGivenTimestamp(timestamp uint64) PhysicalClock {
	return PhysicalClock{timestamp: timestamp}
}

func TestPhysicalConcurrentEventsHappenBefore(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	c1 := r.testCreatePhysicalClockGivenTimestamp(10)
	c2 := r.testCreatePhysicalClockGivenTimestamp(20)
	c3 := r.testCreatePhysicalClockGivenTimestamp(30)

	if !c1.HappensBefore(c2) {
		t.Errorf("c1 should happen before c2")
	}
	if !c1.HappensBefore(c3) {
		t.Errorf("c1 should happen before c3")
	}
	if !c2.HappensBefore(c3) {
		t.Errorf("c2 should happen before c3")
	}
}

func TestResolveConcurrentEvents(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	kv1 := KV[PhysicalClock]{
		Key:   "a",
		Value: "b",
		Clock: PhysicalClock{timestamp: 11}}
	kv2 := KV[PhysicalClock]{
		Key:   "a",
		Value: "c",
		Clock: PhysicalClock{timestamp: 11}}
	kv3 := KV[PhysicalClock]{
		Key:   "a",
		Value: "c",
		Clock: PhysicalClock{timestamp: 15}}

	res, _ := r.ResolveConcurrentEvents(&kv1, &kv2)
	if res.Value != "c" {
		t.Errorf("Should return kv2")
	}
	res, _ = r.ResolveConcurrentEvents(&kv2, &kv3)
	if res.Clock.timestamp != 15 {
		t.Errorf("Should return kv3")
	}
}
