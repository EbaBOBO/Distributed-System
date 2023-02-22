package conflict

import (
	"errors"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// VersionVectorClock is the Clock that we use to implement causal consistency.
type VersionVectorClock struct {
	// Map from node ID to the associated counter. If a node ID isn't in the map, then its counter
	// is considered to be 0 (we don't automatically populate node IDs to save memory).
	vector map[uint64]uint64
}

// NewVersionVectorClock returns an initialized VersionVectorClock.
func NewVersionVectorClock() VersionVectorClock {
	return VersionVectorClock{vector: make(map[uint64]uint64)}
}

// Proto converts a VersionVectorClock into a clock that can be sent in an RPC.
func (v VersionVectorClock) Proto() *pb.Clock {
	p := &pb.Clock{
		Vector: v.vector,
	}
	return p
}

func (v VersionVectorClock) String() string {
	return fmt.Sprintf("%v", v.vector)
}

func (v VersionVectorClock) Equals(other Clock) bool {
	otherVector := other.(VersionVectorClock)
	return maps.Equal(v.vector, otherVector.vector)
}

// HappensBefore returns whether v happens before other. With version vectors, this happens when
// two conditions are met:
//   - For every nodeID in v, other has a counter greater than or equal to v's counter for that
//     node
//   - The vectors are not equal.
//
// Remember that nodeIDs that are not in a vector have an implicit counter of 0.
func (v VersionVectorClock) HappensBefore(other Clock) bool {
	otherVector := other.(VersionVectorClock)

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	for k, val := range v.vector {
		if otherVector.vector[k] < val {
			return false
		}
	}
	for k, val := range otherVector.vector {
		if v.vector[k] > val {
			return false
		}
	}
	return true
}

// Version vector implementation of a ConflictResolver. Might need to keep some state in here
// so that we can always give an up-to-date version vector.
type VersionVectorConflictResolver struct {
	// The node ID on which this conflict resolver is running. Used so that when a message is
	// received, vector[nodeID] can be incremented.
	nodeID uint64

	// mu guards vector
	mu sync.Mutex
	// This node's current clock
	vector map[uint64]uint64
}

// NewVersionVectorConflictResolver() returns an initialized VersionVectorConflictResolver{}
func NewVersionVectorConflictResolver() *VersionVectorConflictResolver {
	return &VersionVectorConflictResolver{vector: make(map[uint64]uint64)}
}

// ReplicatorDidStart initializes the VersionVectorConflictResolver using node metadata
func (v *VersionVectorConflictResolver) ReplicatorDidStart(node *node.Node) {
	v.nodeID = node.ID
	v.vector[v.nodeID] = 0

	log.Printf("version vector conflict resolver initializing itself")
}

// Finds the max of two ordered entities, x and y. constraints.Ordered is an alias for Integers
// and Floats.
func max[T constraints.Ordered](x T, y T) T {
	if x > y {
		return x
	}
	return y
}

// OnMessageReceive is called whenever the underlying node receives an RPC with a clock. As per
// the version-vector algorithm, this function does the following:
//   - Sets the current node's clock to be the element-wise max of itself and the given clock
//   - Increments its own nodeID in vector
//
// Remember thread-safety when modifying fields of v, since multiple messages could be received at
// the same time!
func (v *VersionVectorConflictResolver) OnMessageReceive(clock VersionVectorClock) {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	v.mu.Lock()
	var max_value uint64 = 0
	for _, val := range clock.vector {
		max_value = max(max_value, val)
	}
	v.vector[v.nodeID] = max(v.vector[v.nodeID], max_value)
	v.vector[v.nodeID] += 1
	v.mu.Unlock()
}

// OnMessageSend is called before an RPC is sent to any other node. The version vector should be
// incremented for the local node.
func (v *VersionVectorConflictResolver) OnMessageSend() {
	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	v.mu.Lock()
	v.vector[v.nodeID] += 1
	v.mu.Unlock()

}

func (v *VersionVectorConflictResolver) OnEvent() {
	panic("disregard; not yet implemented in modist")
}

// NewClock creates a new VersionVectorClock by using v's vector.
//
// Note that maps in Golang are implicit pointers, so you should deep-copy the map before
// returning it.
func (v *VersionVectorConflictResolver) NewClock() VersionVectorClock {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	var newClk VersionVectorClock = VersionVectorClock{}
	v.mu.Lock()
	for k, val := range v.vector {
		newClk.vector[k] = val
	}
	v.mu.Unlock()
	return newClk
}

// ZeroClock returns a clock that happens before (or is concurrent with) all other clocks.
func (v *VersionVectorConflictResolver) ZeroClock() VersionVectorClock {
	return VersionVectorClock{vector: map[uint64]uint64{}}
}

// ResolveConcurrentEvents is run when we have several key-value pairs with the same keys, all
// with concurrent clocks (i.e. no version vector happens before any other version vector). To
// converge to one value, we must choose a "winner" among these key-value pairs. Like in
// physical.go, we choose the key-value with the highest lexicographic value.
//
// Additionally, the returned key-value must have a clock that is higher than all the given
// key-value pairs. You can construct a new clock for the returned key by merging the clocks of
// the conflicts slice together (merging two version vectors means computing the element-wise
// max).
//
// You should return an error if no conflicts are given.
func (v *VersionVectorConflictResolver) ResolveConcurrentEvents(
	conflicts ...*KV[VersionVectorClock]) (*KV[VersionVectorClock], error) {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	if conflicts == nil || len(conflicts) == 0 {
		return nil, errors.New("No conflicts are given, ")
	}
	var winner KV[VersionVectorClock] = KV[VersionVectorClock]{
		Key:   conflicts[0].Key,
		Value: conflicts[0].Value,
		Clock: conflicts[0].Clock,
	}
	for i := 1; i < len(conflicts); i++ {
		winner.Value = max(winner.Value, conflicts[i].Value)
		for k, val := range conflicts[i].Clock.vector {
			winner.Clock.vector[k] = max(winner.Clock.vector[k], val)
		}
	}
	return &winner, nil
}
