package conflict

import (
	"fmt"
	pb "modist/proto"
	"time"
)

// PhysicalClock is the Clock that we use to implement eventual consistency. The timestamp is a
// UNIX timestamp.
type PhysicalClock struct {
	timestamp uint64
}

// Proto converts a PhysicalClock into a clock that can be sent in an RPC.
func (c PhysicalClock) Proto() *pb.Clock {
	p := &pb.Clock{
		Timestamp: c.timestamp,
	}
	return p
}

func (c PhysicalClock) String() string {
	return fmt.Sprint(c.timestamp)
}

func (c PhysicalClock) Equals(other Clock) bool {
	otherClock := other.(PhysicalClock)
	return c.timestamp == otherClock.timestamp
}

// HappensBefore determines whether the PhysicalClock c occurs *strictly* before other.
func (c PhysicalClock) HappensBefore(other Clock) bool {
	otherClock := other.(PhysicalClock)

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	if c.timestamp < otherClock.timestamp {
		return true
	}
	return false
}

// PhysicalClockConflictResolver implements a Resolver, meaning that it can generate new clocks
// (of type PhysicalClock) and that it "resolves" key-value pairs whose clocks are concurrent.
type PhysicalClockConflictResolver struct {
	// This embedded DefaultLifecycle struct provides default values for the lifecycle "hooks",
	// which are functions that are run when certain events happen on Resolver's node. These are
	// needed only for vector-based clocks, so we just provide "dummy"/default function values here.
	DefaultLifecycle[PhysicalClock]
}

// NewClock() returns a PhysicalClock using the current system timestamp, in nanoseconds.
func (c *PhysicalClockConflictResolver) NewClock() PhysicalClock {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!

	return PhysicalClock{uint64(time.Now().UnixNano())}
}

// ZeroClock returns a clock that happens before (or is concurrent with) all other clocks.
func (c *PhysicalClockConflictResolver) ZeroClock() PhysicalClock {
	return PhysicalClock{timestamp: 0}
}

// ResolveConcurrentEvents is run when we have several key-value pairs all with the same keys, all
// with clocks that are concurrent. Recall that two clocks are concurrent if neither happens
// before the other. With our PhysicalClocks, when might this happen?
//
// Even if two events have the same PhysicalClock, we want eventual consistency, so we need to
// pick a "winning" key-value pair. Here, we pick the key-value that has the lexicographic
// largest value.
//
// You should return an error if no conflicts are given.
func (c *PhysicalClockConflictResolver) ResolveConcurrentEvents(conflicts ...*KV[PhysicalClock]) (*KV[PhysicalClock], error) {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	var res *KV[PhysicalClock]
	res.Key = "A"
	res.Clock = c.ZeroClock()

	count := 0
	for _, item := range conflicts {
		count += 1
		if res.Clock.HappensBefore(item.Clock) {
			res = item
		} else if res.Clock.Equals(item.Clock) {
			if res.Key < item.Key {
				res = item
			}
		}
	}
	if count == 0 {
		var e error
		e.Error()
		return nil, e
	}

	return res, nil
}
