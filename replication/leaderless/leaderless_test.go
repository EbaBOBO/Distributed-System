package leaderless

import (
	"context"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"modist/replication/conflict"
	"modist/store"
	"strconv"
	"testing"
)

func testCreatePhysicalClockArgs(node *node.Node, w, r int) Args[conflict.PhysicalClock] {
	a := Args[conflict.PhysicalClock]{
		Node:             node,
		W:                w,
		R:                r,
		ConflictResolver: &conflict.PhysicalClockConflictResolver{},
		LocalStore:       &store.Memory[*conflict.KV[conflict.PhysicalClock]]{},
	}
	return a
}

// Test that a cluster with 3 nodes can replicate a key and let us read it back
func TestBasicLeaderless(t *testing.T) {
	nodes := node.Create([]string{"localhost:1234", "localhost:1235", "localhost:1236"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](
			testCreatePhysicalClockArgs(node, 2, 2),
		)
		replicators = append(replicators, replicator)
	}

	key := "foo"
	value := "bar"

	firstReplicator := replicators[0]

	response, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: key, Value: value, Clock: &pb.Clock{Timestamp: 1}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}

	log.Printf("response clock is %v", response.GetClock())
	kv, err := firstReplicator.GetReplicatedKey(context.Background(),
		&pb.GetRequest{Key: key, Metadata: &pb.GetMetadata{Clock: response.GetClock()}})
	if err != nil {
		t.Fatalf("Error while getting key from node 1: %v", err)
	}

	if kv.GetValue() != value {
		t.Fatalf("Value mismatch: expected %v, got %v", value, kv.GetValue())
	}
}

// Read repair brings nodes that have falled behind up-to-date when we do reads. To test that read
// repair works, we can intentionally make a node x fall behind (partition it), do quorum reads
// involving x, and then read only from x. We then make sure that x is as up-to-date as any other
// replica.
func TestBasicReadRepair(t *testing.T) {
	nodes := node.Create([]string{"localhost:4001", "localhost:4002", "localhost:4003"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](testCreatePhysicalClockArgs(node, 2, 2))
		replicators = append(replicators, replicator)
	}

	// make replicators[2] fall behind by never including it in any writes
	// NOTE: This can also be done using node.StartPartition(...)
	replicators[0].replicaChooser = func(numreplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[1].node.ID}, nil
	}

	replicators[1].replicaChooser = func(numreplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[0].node.ID}, nil
	}

	// write lots of data to replicators[0] and replicators[1]
	numItrs := 10
	for i := 0; i < numItrs; i++ {
		pr := &pb.PutRequest{Key: strconv.Itoa(i), Value: strconv.Itoa(numItrs - i), Clock: &pb.Clock{Timestamp: 1}}
		replicators[i%2].ReplicateKey(context.Background(), pr)
	}

	// force the reads from replicators[0] or [1] to include replicators[2]
	replicators[1].replicaChooser = func(numReplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[2].node.ID}, nil
	}

	for i := 0; i < numItrs; i++ {
		log.Printf("requesting key")
		replicators[1].GetReplicatedKey(context.Background(),
			&pb.GetRequest{Key: strconv.Itoa(i), Metadata: &pb.GetMetadata{Clock: &pb.Clock{Timestamp: 1}}})
	}

	// finally, ensure that replicators[2] has every key itself (by setting r = 1)
	for i := 0; i < numItrs; i++ {
		got, ok := replicators[2].localStore.Get(strconv.Itoa(i))
		expected := strconv.Itoa(numItrs - i)

		if !ok {
			t.Errorf("key %d not found in local store", i)
		} else if got.Value != expected {
			t.Errorf("value mismatch: expected %s for key %d, got %s", expected, i, got)
		}
	}
}

func TestReadNonexistentKeys(t *testing.T) {
	nodes := node.Create([]string{"localhost:2234", "localhost:2235", "localhost:2236"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](
			testCreatePhysicalClockArgs(node, 2, 2),
		)
		replicators = append(replicators, replicator)
	}

	key := "foo"
	value := "bar"

	firstReplicator := replicators[0]

	response, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: key, Value: value, Clock: &pb.Clock{Timestamp: 1}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}

	log.Printf("response clock is %v", response.GetClock())
	kv, err := firstReplicator.GetReplicatedKey(context.Background(),
		&pb.GetRequest{Key: "nonexist", Metadata: &pb.GetMetadata{Clock: response.GetClock()}})
	if err == nil {
		t.Fatalf("Expected error while reading non-existent key")
	}

	if kv.GetValue() != "" {
		t.Fatalf("Value mismatch: expected empty string, got %v", kv.GetValue())
	}
}

func TestGetUpToDate(t *testing.T) {
	nodes := node.Create([]string{"localhost:3234", "localhost:3235", "localhost:3236"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](
			testCreatePhysicalClockArgs(node, 2, 2),
		)
		replicators = append(replicators, replicator)
	}

	firstReplicator := replicators[0]

	response, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: "foo", Value: "bar1", Clock: &pb.Clock{Timestamp: 10}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}
	log.Printf("response clock is %v", response.GetClock())
	response2, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: "foo", Value: "bar2", Clock: &pb.Clock{Timestamp: 20}})

	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}
	log.Printf("response clock is %v", response2.GetClock())
	//kv, err := firstReplicator.GetReplicatedKey(context.Background(),
	//	&pb.GetRequest{Key: "foo", Metadata: &pb.GetMetadata{Clock: &pb.Clock{Timestamp: uint64(time.Now().UnixNano() + 1e15)}}})

	kv, err := firstReplicator.GetReplicatedKey(context.Background(),
		&pb.GetRequest{Key: "foo", Metadata: &pb.GetMetadata{Clock: response2.GetClock()}})
	//if err != nil {
	//	t.Fatalf("Error while replicating key to node 0: %v", err)
	//}
	//log.Printf("response clock is %v", kv.GetClock())
	if kv.GetValue() != "bar2" {
		t.Fatalf("Error while replicating key")
	}

}

func TestReadRepair(t *testing.T) {
	nodes := node.Create([]string{"localhost:5234", "localhost:5235", "localhost:5236"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](
			testCreatePhysicalClockArgs(node, 2, 2),
		)
		replicators = append(replicators, replicator)
	}

	_, err := replicators[0].ReplicateKey(context.Background(), &pb.PutRequest{
		Key: "k", Value: "0", Clock: &pb.Clock{Timestamp: 10}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}
	_, err = replicators[1].ReplicateKey(context.Background(), &pb.PutRequest{
		Key: "k", Value: "1", Clock: &pb.Clock{Timestamp: 10}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 1: %v", err)
	}
	_, err = replicators[2].ReplicateKey(context.Background(), &pb.PutRequest{
		Key: "k", Value: "2", Clock: &pb.Clock{Timestamp: 10}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 2: %v", err)
	}
	kv, err := replicators[1].GetReplicatedKey(context.Background(),
		&pb.GetRequest{Key: "k", Metadata: &pb.GetMetadata{Clock: &pb.Clock{Timestamp: 1}}})
	if kv.Value != "2" {
		t.Fatalf("The latest value should be 2")
	}
}
