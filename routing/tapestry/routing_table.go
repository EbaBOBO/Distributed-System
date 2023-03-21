/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
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
	return
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
// Return false if a node tries to remove itself from the table.
func (t *RoutingTable) Remove(remoteNodeId ID) (wasRemoved bool) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	return
}

// GetLevel gets ALL nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodeIds []ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	return
}

// FindNextHop searches the table for the closest next-hop node for the provided ID starting at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) ID {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	return ID{}
}
