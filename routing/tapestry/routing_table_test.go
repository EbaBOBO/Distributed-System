package tapestry

import (
	"sort"
	"testing"
)

func TestAdd(t *testing.T) {
	table := NewRoutingTable(MakeIDFromHexString("2A"))
	if added, previous := table.Add(MakeIDFromHexString("12")); added != true || previous != nil {
		t.Errorf("Should be true, nil")
	}
	if added, previous := table.Add(MakeIDFromHexString("12")); added != false || previous != nil {
		t.Errorf("Should be false, nil")
	}
	if added, previous := table.Add(MakeIDFromHexString("13")); added != true || previous != nil {
		t.Errorf("Should be true, nil")
	}
	if added, previous := table.Add(MakeIDFromHexString("14")); added != true || previous != nil {
		t.Errorf("Should be true, nil")
	}
	if added, previous := table.Add(MakeIDFromHexString("15")); added != true || previous.String() != MakeIDFromHexString("12").String() {
		t.Errorf("Should be true, 12")
	}
	if added, previous := table.Add(MakeIDFromHexString("11")); added != false || previous != nil {
		t.Errorf("Should be false, nil")
	}
	if !sort.SliceIsSorted(table.Rows[0][1], func(i, j int) bool {
		return table.localId.Closer(table.Rows[0][1][i], table.Rows[0][1][j])
	}) {
		t.Errorf("Slot should be sorted")
	}
}

func TestRemove(t *testing.T) {
	table := NewRoutingTable(MakeIDFromHexString("2A"))
	table.Add(MakeIDFromHexString("12"))
	table.Add(MakeIDFromHexString("13"))
	table.Add(MakeIDFromHexString("14"))
	if wasRemoved := table.Remove(MakeIDFromHexString("2A")); wasRemoved != false {
		t.Errorf("Should be false")
	}
	if wasRemoved := table.Remove(MakeIDFromHexString("15")); wasRemoved != false {
		t.Errorf("Should be false")
	}
	if wasRemoved := table.Remove(MakeIDFromHexString("13")); wasRemoved != true {
		t.Errorf("Should be true")
	}
	if len(table.Rows[0][1]) != 2 ||
		table.Rows[0][1][0].String() != MakeIDFromHexString("14").String() ||
		table.Rows[0][1][1].String() != MakeIDFromHexString("12").String() {
		t.Errorf("Corrupted removal")
	}
}

func TestGetLevel(t *testing.T) {
	table := NewRoutingTable(MakeIDFromHexString("2A"))
	table.Add(MakeIDFromHexString("31"))
	res := table.GetLevel(0)
	if res[0].String() != MakeIDFromHexString("31").String() {
		t.Errorf("Incorrect result")
	}
}

func TestFindNextHop(t *testing.T) {
	table := NewRoutingTable(MakeIDFromHexString("2A"))
	table.Add(MakeIDFromHexString("31"))
	table.Add(MakeIDFromHexString("41"))
	res := table.FindNextHop(MakeIDFromHexString("42"), 0)
	if res.String() != MakeIDFromHexString("41").String() {
		t.Errorf("Incorrect result")
	}
}
