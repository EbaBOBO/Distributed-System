package partitioning

import (
	"encoding/binary"
	"reflect"
	"testing"

	"golang.org/x/exp/slices"
)

// checkLookup performs a lookup of key using the provided consistent hash partitioner,
// ensuring that there is no error, the returned id matches what is expected, and the
// rewritten key is a hash of the looked-up key.
func checkLookup(t *testing.T, msg string, c *ConsistentHash, key string, id uint64) {
	t.Helper()

	gotID, gotRewrittenKey, err := c.Lookup(key)
	rewrittenKey := hashToString(c.keyHash(key))
	if err != nil {
		t.Errorf("%s: Returned an error: %v", msg, err)
	} else if gotID != id {
		t.Errorf("%s: Returned the wrong shard: expected %d, got %d\nThe hashed key is %s\n Here are the virtual nodes in the assigner: %+v\n\n", msg, id, gotID, rewrittenKey, c.virtualNodes)
	} else if gotRewrittenKey != rewrittenKey {
		t.Errorf("%s: Returned the wrong rewritten key: expected %s, got %s", msg, rewrittenKey, gotRewrittenKey)
	}
}

// identityHasher returns the last 32 bytes of the input as a 32-byte array, padding with
// zeroes if necessary.
func identityHasher(b []byte) [32]byte {
	var out [32]byte

	bIndex := len(b) - 1
	for i := len(out) - 1; i >= 0; i-- {
		if bIndex < 0 {
			continue
		}
		out[i] = b[bIndex]
		bIndex--
	}
	return out
}

func newVirtualNode(c *ConsistentHash, id uint64, virtualNum int) virtualNode {
	return virtualNode{
		id:   id,
		num:  virtualNum,
		hash: c.virtualNodeHash(id, virtualNum),
	}
}

func TestConsistentHash_Lookup_SimpleIdentity(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher

	c.virtualNodes = []virtualNode{
		newVirtualNode(c, 1, 0),
		newVirtualNode(c, 1, 1),
		newVirtualNode(c, 50, 0),
		newVirtualNode(c, 50, 1),
	}
	slices.SortFunc(c.virtualNodes, virtualNodeLess)

	byteKey := make([]byte, 8)

	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 1)

	binary.BigEndian.PutUint64(byteKey, 10)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 50)
	checkLookup(t, "Lookup(50)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 51)
	checkLookup(t, "Lookup(51)", c, string(byteKey), 50)
}

func TestAddReplicaGroup(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher

	c.virtualNodes = []virtualNode{
		newVirtualNode(c, 1, 0),
		newVirtualNode(c, 1, 1),
		newVirtualNode(c, 50, 0),
		newVirtualNode(c, 50, 1),
	}
	slices.SortFunc(c.virtualNodes, virtualNodeLess)
	res := c.AddReplicaGroup(25)
	var correctRes []Reassignment

	correctRes = append(correctRes, Reassignment{
		From: 50,
		To:   25,
		Range: KeyRange{
			Start: "0000000000000000000000000000000000000000000000000000000000000003",
			End:   "0000000000000000000000000000000000000000000000000000000000000019",
		},
	})
	correctRes = append(correctRes, Reassignment{
		From: 50,
		To:   25,
		Range: KeyRange{
			Start: "000000000000000000000000000000000000000000000000000000000000001a",
			End:   "000000000000000000000000000000000000000000000000000000000000001a",
		},
	})

	equalCnt := 0
	for _, r := range res {
		for _, cr := range correctRes {
			if reflect.DeepEqual(r, cr) {
				equalCnt++
			}
		}
	}
	if equalCnt != len(correctRes) {
		t.Errorf("False range")
	}

}

func TestRemoveReplicaGroup(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher

	c.virtualNodes = []virtualNode{
		newVirtualNode(c, 1, 0),
		newVirtualNode(c, 1, 1),
		newVirtualNode(c, 20, 0),
		newVirtualNode(c, 20, 1),
		newVirtualNode(c, 50, 0),
		newVirtualNode(c, 50, 1),
	}
	slices.SortFunc(c.virtualNodes, virtualNodeLess)
	res := c.RemoveReplicaGroup(50)

	var correctRes []Reassignment

	correctRes = append(correctRes, Reassignment{
		From: 50,
		To:   1,
		Range: KeyRange{
			Start: "0000000000000000000000000000000000000000000000000000000000000016",
			End:   "0000000000000000000000000000000000000000000000000000000000000032",
		},
	})
	correctRes = append(correctRes, Reassignment{
		From: 50,
		To:   1,
		Range: KeyRange{
			Start: "0000000000000000000000000000000000000000000000000000000000000033",
			End:   "0000000000000000000000000000000000000000000000000000000000000033",
		},
	})

	equalCnt := 0
	for _, r := range res {
		for _, cr := range correctRes {
			if reflect.DeepEqual(r, cr) {
				equalCnt++
			}
		}
	}
	if equalCnt != len(correctRes) {
		t.Errorf("False range")
	}
}

func TestLookup(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher
	_, _, err := c.Lookup("test")
	if err == nil {
		t.Errorf("Should return error when there is no replica group")
	}
}
