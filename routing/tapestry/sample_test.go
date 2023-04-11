package tapestry

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSampleTapestrySetup(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "3", "5", "7") //Make a tapestry with these ids
	fmt.Printf("length of tap %d\n", len(tap))
	KillTapestries(tap[1], tap[2]) //Kill off two of them.
	resp, _ := tap[0].FindRoot(
		context.Background(),
		CreateIDMsg("2", 0),
	) //After killing 3 and 5, this should route to 7
	if resp.Next != tap[3].Id.String() {
		t.Errorf("Failed to kill successfully")
	}

}

func TestSampleTapestrySearch(t *testing.T) {
	tap, _ := MakeTapestries(true, "100", "456", "1234") //make a sample tap
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, err := tap[0].Get("look at this lad") //Store a KV pair and try to fetch it
	fmt.Println(err)
	if !bytes.Equal(result, []byte("an absolute unit")) { //Ensure we correctly get our KV
		t.Errorf("Get failed")
	}
}

func TestSampleTapestryAddNodes(t *testing.T) {
	// Need to use this so that gRPC connections are set up correctly
	tap, delayNodes, _ := MakeTapestriesDelayConnecting(
		true,
		[]string{"1", "5", "9"},
		[]string{"8", "12"},
	)

	// Add some tap nodes after the initial construction
	for _, delayNode := range delayNodes {
		args := Args{
			Node:      delayNode,
			Join:      true,
			ConnectTo: tap[0].RetrieveID(tap[0].Id),
		}
		tn := Configure(args).tapestryNode
		tap = append(tap, tn)
		time.Sleep(1000 * time.Millisecond) //Wait for availability
	}

	resp, _ := tap[1].FindRoot(context.Background(), CreateIDMsg("7", 0))
	if resp.Next != tap[3].Id.String() {
		t.Errorf("Addition of node failed")
	}
}

func TestGetError(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "10") //Make a tapestry with these ids
	blob := []byte{1, 2, 3}
	tap[0].Store("9", blob)
	tap[0].Remove("9")
	_, err := tap[1].Get("9")
	if err == nil {
		t.Errorf("Should get error from un-exist key")
	}
}

func TestLeave(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "3", "5") //Make a tapestry with these ids
	fmt.Printf("length of tap %d\n", len(tap))
	tap[1].Leave()
	time.Sleep(time.Second)
	resp, _ := tap[0].FindRoot(
		context.Background(),
		CreateIDMsg("2", 0),
	) //After killing 3 and 5, this should route to 7
	if resp.Next != tap[2].Id.String() {
		t.Errorf("Failed to kill successfully")
	}

}

func TestOffline(t *testing.T) {
	tap, delayNodes, _ := MakeTapestriesDelayConnecting(
		true,
		[]string{"1", "5", "9", "A"},
		[]string{"8", "12"},
	)
	tap[3].Kill()
	// Add some tap nodes after the initial construction
	for _, delayNode := range delayNodes {
		args := Args{
			Node:      delayNode,
			Join:      true,
			ConnectTo: tap[0].RetrieveID(tap[0].Id),
		}
		tn := Configure(args).tapestryNode
		tap = append(tap, tn)
		time.Sleep(1000 * time.Millisecond) //Wait for availability
	}
	tap[0].Store("1", []byte("info"))
	res, err := tap[1].Get("1")
	if !bytes.Equal(res, []byte("info")) || err != nil {
		t.Errorf("Failed to store after node offline")
	}
	tap[0].Kill()
	_, err = tap[1].Get("1")
	if err == nil {
		t.Errorf("Should be a failure")
	}
}
