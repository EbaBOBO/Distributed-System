/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

import (
	"context"
	"errors"
	"fmt"
	pb "modist/proto"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *TapestryNode) Store(key string, value []byte) (err error) {
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	local.blobstore.Put(key, value, done)
	return nil
}

// Get looks up a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *TapestryNode) Get(key string) ([]byte, error) {
	// Lookup the key
	routerIds, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(routerIds) == 0 {
		return nil, fmt.Errorf("No routers returned for key %v", key)
	}

	// Contact router
	keyMsg := &pb.TapestryKey{
		Key: key,
	}

	var errs []error
	for _, routerId := range routerIds {
		conn := local.Node.PeerConns[local.RetrieveID(routerId)]
		router := pb.NewTapestryRPCClient(conn)
		resp, err := router.BlobStoreFetch(context.Background(), keyMsg)
		if err != nil {
			errs = append(errs, err)
		} else if resp.Data != nil {
			return resp.Data, nil
		}
	}

	return nil, fmt.Errorf("Error contacting routers, %v: %v", routerIds, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *TapestryNode) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// Publishes the key in tapestry.
//
// - Start periodically publishing the key. At each publishing:
//   - Find the root node for the key
//   - Register the local node on the root
//   - if anything failed, retry; until RETRIES has been reached.
//
// - Return a channel for cancelling the publish
//   - if receiving from the channel, stop republishing
//
// Some note about publishing behavior:
//   - The first publishing attempt should attempt to retry at most RETRIES times if there is a failure.
//     i.e. if RETRIES = 3 and FindRoot errored or returned false after all 3 times, consider this publishing
//     attempt as failed. The error returned for Publish should be the error message associated with the final
//     retry.
//   - If any of these attempts succeed, you do not need to retry.
//   - In addition to the initial publishing attempt, you should repeat this entire publishing workflow at the
//     appropriate interval. i.e. every 5 seconds we attempt to publish, and THIS publishing attempt can either
//     succeed, or fail after at most RETRIES times.
//   - Keep trying to republish regardless of how the last attempt went
func (local *TapestryNode) Publish(key string) (chan bool, error) {
	// TODO(students): [Tapestry] Implement me!
	publishChan := make(chan bool)
	var publishErr error
	publishErr = nil

	////////// initial publish function
	publishWorkFlow := func() {
		//   - Find the root node for the key
		for i := 0; i < RETRIES; i++ {
			rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
			if err != nil {
				publishErr = errors.New("Publish FindRoot failed")
				continue
			}
			rootId, err := ParseID(rootMsg.Next)
			if err != nil {
				publishErr = errors.New("Publish ParseID failed")
				continue
			}

			//   - Register the local node on the root
			// if anything failed, retry; until RETRIES has been reached.
			conn := local.Node.PeerConns[local.RetrieveID(rootId)]
			rootNode := pb.NewTapestryRPCClient(conn)
			isRegister, err := rootNode.Register(context.Background(), &pb.Registration{FromNode: local.Id.String(), Key: key})
			if err != nil || !isRegister.Ok {
				publishErr = errors.New("Publish Register failed")
				continue
			}

			publishErr = nil
			return
		}

	}

	// first publish
	publishWorkFlow()
	if publishErr != nil {
		return publishChan, publishErr
	}

	// - Start periodically publishing the key. At each publishing:
	timer := time.NewTicker(REPUBLISH)
	go func() {
		select {
		case <-timer.C:
			publishWorkFlow()
		case <-publishChan:
			return
		}
	}()

	return publishChan, publishErr
}

// Lookup look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the routers (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *TapestryNode) Lookup(key string) ([]ID, error) {
	// TODO(students): [Tapestry] Implement me!

	for i := 0; i < RETRIES; i++ {
		msg, _ := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
		root, _ := ParseID(msg.Next)
		conn := local.Node.PeerConns[local.RetrieveID(root)]
		rootNode := pb.NewTapestryRPCClient(conn)
		loc, err := rootNode.Fetch(context.Background(), &pb.TapestryKey{Key: key})

		if err != nil {
			_, _ = local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{root.String()}})
			_, _ = local.RemoveBackpointer(context.Background(), &pb.NodeMsg{Id: root.String()})
		} else {
			var res []ID
			for _, it := range loc.Values {
				id, _ := ParseID(it)
				res = append(res, id)
			}
			return res, nil
		}
	}
	return nil, errors.New("too many retries")
}

// FindRoot returns the root for the id in idMsg by recursive RPC calls on the next hop found in our routing table
//   - find the next hop from our routing table
//   - call FindRoot on nextHop
//   - if failed, add nextHop to toRemove, remove them from local routing table, retry
func (local *TapestryNode) FindRoot(ctx context.Context, idMsg *pb.IdMsg) (*pb.RootMsg, error) {
	id, err := ParseID(idMsg.Id)
	if err != nil {
		return nil, err
	}
	level := idMsg.Level

	// TODO(students): [Tapestry] Implement me!
	var toRemove []string
s1:
	nextHop := local.Table.FindNextHop(id, level)
	if nextHop.String() == local.String() {
		return &pb.RootMsg{Next: nextHop.String(), ToRemove: toRemove}, nil
	}
	idMsg.Level = level + 1
	conn := local.Node.PeerConns[local.RetrieveID(nextHop)]
	nextNode := pb.NewTapestryRPCClient(conn)
	rootMsg, err := nextNode.FindRoot(ctx, idMsg)
	if err != nil {
		toRemove = append(toRemove, rootMsg.ToRemove...)
		local.RemoveBadNodes(ctx, &pb.Neighbors{Neighbors: toRemove})
		goto s1
	}
	local.RemoveBadNodes(ctx, &pb.Neighbors{Neighbors: toRemove})
	return rootMsg, nil
}

// The node that stores some data with key is registering themselves to us as an advertiser of the key.
// - Check that we are the root node for the key, return true in pb.Ok if we are
// - Add the node to the location map (local.locationsByKey.Register)
//   - local.locationsByKey.Register kicks off a timer to remove the node if it's not advertised again
//     after TIMEOUT
func (local *TapestryNode) Register(
	ctx context.Context,
	registration *pb.Registration,
) (*pb.Ok, error) {
	from, err := ParseID(registration.FromNode)
	if err != nil {
		return nil, err
	}
	key := registration.Key

	// TODO(students): [Tapestry] Implement me!
	local.LocationsByKey.Register(key, from, TIMEOUT)
	res := pb.Ok{
		Ok: local.Table.FindNextHop(Hash(key), 0).String() == local.String(),
	}
	return &res, nil
}

// Fetch checks that we are the root node for the requested key and
// return all nodes that are registered in the local location map for this key
func (local *TapestryNode) Fetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.FetchedLocations, error) {
	// TODO(students): [Tapestry] Implement me!
	keyStr := key.GetKey()
	id := Hash(keyStr)
	root := local.Table.FindNextHop(id, 0)
	var value []string
	for _, it := range local.LocationsByKey.Get(keyStr) {
		value = append(value, it.String())
	}
	return &pb.FetchedLocations{
		IsRoot: root.String() == local.String(),
		Values: value,
	}, nil
}

// Retrieves the blob corresponding to a key
func (local *TapestryNode) BlobStoreFetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.DataBlob, error) {
	data, isOk := local.blobstore.Get(key.Key)

	var err error
	if !isOk {
		err = errors.New("Key not found")
	}

	return &pb.DataBlob{
		Key:  key.Key,
		Data: data,
	}, err
}

// Transfer registers all of the provided objects in the local location map. (local.locationsByKey.RegisterAll)
// If appropriate, add the from node to our local routing table
func (local *TapestryNode) Transfer(
	ctx context.Context,
	transferData *pb.TransferData,
) (*pb.Ok, error) {
	from, err := ParseID(transferData.From)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string][]ID)
	for key, set := range transferData.Data {
		nodeMap[key], err = stringSliceToIds(set.Neighbors)
		if err != nil {
			return nil, err
		}
	}

	// TODO(students): [Tapestry] Implement me!
	local.LocationsByKey.RegisterAll(nodeMap, TIMEOUT)
	if err := local.AddRoute(from); err != nil {
		return &pb.Ok{Ok: false}, err
	}

	return &pb.Ok{Ok: true}, nil
}

// calls FindRoot on a remote node to find the root of the given id
func (local *TapestryNode) FindRootOnRemoteNode(remoteNodeId ID, id ID) (*ID, error) {
	// TODO(students): [Tapestry] Implement me!
	conn := local.Node.PeerConns[local.RetrieveID(remoteNodeId)]
	remote := pb.NewTapestryRPCClient(conn)
	msg, err := remote.FindRoot(context.Background(), &pb.IdMsg{Id: id.String(), Level: 0})
	if err != nil {
		return nil, err
	}
	res, err := ParseID(msg.Next)
	return &res, err
}
