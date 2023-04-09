/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"sort"
	"sync"
)

// RoutingTable has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table is protected by a mutex.
type RoutingTable struct {
	localId ID                 // The ID of the local tapestry node
	Rows    [DIGITS][BASE][]ID // The rows of the routing table (stores IDs of remote tapestry nodes)
	mutex   sync.Mutex         // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// NewRoutingTable creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me ID) *RoutingTable {
	t := new(RoutingTable)
	t.localId = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			t.Rows[i][j] = make([]ID, 0, SLOTSIZE)
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.Rows[i][t.localId[i]]
		t.Rows[i][t.localId[i]] = append(slot, t.localId)
	}

	return t
}

// Add adds the given node to the routing table.
//
// Note you should not add the node to preceding levels. You need to add the node
// to one specific slot in the routing table (or replace an element if the slot is full
// at SLOTSIZE).
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(remoteNodeId ID) (added bool, previous *ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	spl := SharedPrefixLength(t.localId, remoteNodeId)
	if spl == DIGITS {
		return false, nil
	}
	slotLen := len(t.Rows[spl][remoteNodeId[spl]])
	// Node already exist
	for _, it := range t.Rows[spl][remoteNodeId[spl]] {
		if it.String() == remoteNodeId.String() {
			return false, nil
		}
	}
	// Empty slot, direct add
	if slotLen == 0 {
		t.Rows[spl][remoteNodeId[spl]] = append(t.Rows[spl][remoteNodeId[spl]], remoteNodeId)
		return true, nil
	}
	t.Rows[spl][remoteNodeId[spl]] = append(t.Rows[spl][remoteNodeId[spl]], remoteNodeId)
	sort.Slice(t.Rows[spl][remoteNodeId[spl]], func(i, j int) bool {
		return t.localId.Closer(t.Rows[spl][remoteNodeId[spl]][i], t.Rows[spl][remoteNodeId[spl]][j])
	})
	// Smaller than SLOTSIZE
	if len(t.Rows[spl][remoteNodeId[spl]]) <= SLOTSIZE {
		return true, nil
	}
	// remoteNodeId is less closer than node in slot
	if t.Rows[spl][remoteNodeId[spl]][len(t.Rows[spl][remoteNodeId[spl]])-1].String() == remoteNodeId.String() {
		return false, nil
	}
	// Need to overwrite
	previous = &t.Rows[spl][remoteNodeId[spl]][len(t.Rows[spl][remoteNodeId[spl]])-1]
	t.Rows[spl][remoteNodeId[spl]] = t.Rows[spl][remoteNodeId[spl]][0:SLOTSIZE]
	return true, previous
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
// Return false if a node tries to remove itself from the table.
func (t *RoutingTable) Remove(remoteNodeId ID) (wasRemoved bool) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!

	// Node try to remove itself
	if remoteNodeId.String() == t.localId.String() {
		return false
	}
	spl := SharedPrefixLength(t.localId, remoteNodeId)
	slotLen := len(t.Rows[spl][remoteNodeId[spl]])
	idx := 0
	for ; idx < slotLen; idx++ {
		if t.Rows[spl][remoteNodeId[spl]][idx].String() == remoteNodeId.String() {
			break
		}
	}
	// Node does not exist
	if idx >= slotLen {
		return false
	}
	tmp := make([]ID, slotLen)
	copy(tmp, t.Rows[spl][remoteNodeId[spl]])
	t.Rows[spl][remoteNodeId[spl]] = t.Rows[spl][remoteNodeId[spl]][0:idx]
	t.Rows[spl][remoteNodeId[spl]] = append(t.Rows[spl][remoteNodeId[spl]], tmp[idx+1:slotLen]...)
	return true
}

// GetLevel gets ALL nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodeIds []ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	for _, slt := range t.Rows[level] {
		for _, nd := range slt {
			if nd.String() != t.localId.String() {
				nodeIds = append(nodeIds, nd)
			}
		}
	}
	return
}

// FindNextHop searches the table for the closest next-hop node for the provided ID starting at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) ID {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	if level == DIGITS-1 {
		return t.localId
	}
	col := id[level]
	for level < DIGITS-1 {
		for len(t.Rows[level][col]) == 0 {
			col = (col + 1) % BASE
		}
		if t.Rows[level][col][0].String() != t.localId.String() {
			return t.Rows[level][col][0]
		}
		level += 1
		col = id[level]
	}
	return t.localId
}
