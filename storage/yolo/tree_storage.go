// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package yolo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/google/trillian"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"github.com/google/trillian/storage/storagepb"
	"github.com/tsuna/gohbase"
)

const degree = 8

func subtreeKey(treeID, rev int64, nodeID storage.NodeID) *kv {
	return &kv{k: fmt.Sprintf("/%d/subtree/%s/%d", treeID, nodeID.String(), rev)}
}

// tree stores all data for a given treeID
type tree struct {
	mu          sync.RWMutex
	store       *hbaseClient
	currentSTH  int64 // currentSTH is the timestamp of the current STH.
	meta        *trillian.Tree
	kafkaOffset int64 // TODO(filippo): probably belongs on a tx?
	//TODO(gtank): kafkaOffset should be persisted in proto if it's going to be here
}

func (t *tree) Lock() {
	t.mu.Lock()
}

func (t *tree) Unlock() {
	t.mu.Unlock()
}

func (t *tree) RLock() {
	t.mu.RLock()
}

func (t *tree) RUnlock() {
	t.mu.RUnlock()
}

// Dump ascends the tree, logging the items contained.
func Dump(t *btree.BTree) {
	t.Ascend(func(i btree.Item) bool {
		glog.Infof("%#v", i)
		return true
	})
}

type commitTreeStorage struct {
	mu sync.RWMutex
	// trees     map[int64]*tree
	kafkaProd sarama.SyncProducer
	kafkaCons sarama.Consumer
	kafka     sarama.Client
	hbase     *hbaseClient
}

func newTreeStorage(kafka sarama.Client, client gohbase.Client) *commitTreeStorage {
	kafkaProd, err := sarama.NewSyncProducerFromClient(kafka)
	if err != nil {
		glog.Exit("NewSyncProducerFromClient", err)
	}
	kafkaCons, err := sarama.NewConsumerFromClient(kafka)
	if err != nil {
		glog.Exit("NewConsumerFromClient", err)
	}
	return &commitTreeStorage{
		// trees:     make(map[int64]*tree),
		kafkaProd: kafkaProd,
		kafkaCons: kafkaCons,
		kafka:     kafka,
		hbase:     newHBaseClient(client),
	}
}

// getTree returns the tree associated with id, or nil if no such tree exists.
func (m *commitTreeStorage) getTree(id int64) (*tree, error) {
	// m.mu.RLock()
	// defer m.mu.RUnlock()

	treekey := metaKey(id, "tree").(*kv).k
	treeprotobytes, err := m.hbase.QualifiedGet("subtrees", treekey, "raw", "bytes")
	if err != nil {
		return nil, err
	}
	var meta trillian.Tree
	if err := proto.Unmarshal(treeprotobytes.v.([]byte), &meta); err != nil {
		return nil, err
	}

	var offset int64
	offsetMetaKey := metaKey(id, "offset").(*kv).k
	offsetResult, err := m.hbase.QualifiedGet("subtrees", offsetMetaKey, "raw", "bytes")
	if err != nil {
		if err == ErrDoesNotExist {
			offsetResult = &kv{k: offsetMetaKey, v: []byte("0")}
		} else {
			return nil, err
		}
	}
	if err := json.Unmarshal(offsetResult.v.([]byte), &offset); err != nil {
		return nil, err
	}

	var currentSTH int64
	sthMetaKey := metaKey(id, "currentSTH").(*kv).k
	sthResult, err := m.hbase.QualifiedGet("subtrees", sthMetaKey, "raw", "bytes")
	if err != nil {
		if err == ErrDoesNotExist {
			sthResult = &kv{k: sthMetaKey, v: []byte("0")}
		} else {
			return nil, err
		}
	}
	if err := json.Unmarshal(sthResult.v.([]byte), &currentSTH); err != nil {
		return nil, err
	}

	ret := &tree{
		store:       m.hbase,
		meta:        &meta,
		kafkaOffset: offset,
		currentSTH:  currentSTH,
	}

	return ret, nil
}

func (m *commitTreeStorage) getKafkaOffset(id int64) (int64, error) {
	// m.mu.RLock()
	// defer m.mu.RUnlock()
	tree, err := m.getTree(id)
	if err != nil {
		return -1, err
	}
	return tree.kafkaOffset, nil
}

func (m *commitTreeStorage) getCurrentSTH(id int64) (int64, error) {
	// m.mu.RLock()
	// defer m.mu.RUnlock()
	tree, err := m.getTree(id)
	if err != nil {
		return -1, err
	}
	return tree.currentSTH, nil
}

// kv is a simple key->value type which implements btree's Item interface.
type kv struct {
	k string
	v interface{}
}

// Less than by k's string key
func (a kv) Less(b btree.Item) bool {
	return strings.Compare(a.k, b.(*kv).k) < 0
}

// newTree creates and initializes a tree struct.
func (cts *commitTreeStorage) newTree(t trillian.Tree) *tree {
	ret := &tree{
		store: cts.hbase,
		meta:  &t,
	}

	// we don't need to initialize these
	// k := unseqKey(t.TreeId)
	// k.(*kv).v = list.New()
	// ret.store.BufferedPut("subtrees", k.(*kv).k, "raw", "bytes",

	k := hashToSeqKey(t.TreeId)
	k.(*kv).v = make(map[string][]int64, 1)

	var treeIDMapBytes []byte
	treeIDMapBytes, _ = json.Marshal(k.(*kv).v)

	ret.store.BufferedPut("subtrees", k.(*kv).k, "raw", "bytes", treeIDMapBytes)
	ret.store.Flush()

	return ret
}

func (m *commitTreeStorage) beginTreeTX(ctx context.Context, readonly bool, treeID int64, hashSizeBytes int, cache cache.SubtreeCache) (treeTX, error) {
	tree, err := m.getTree(treeID)
	if err != nil {
		return treeTX{}, err
	}
	// Lock the tree for the duration of the TX.
	// It will be unlocked by a call to Commit or Rollback.
	var unlock func()
	if readonly {
		tree.RLock()
		unlock = tree.RUnlock
	} else {
		tree.Lock()
		unlock = tree.Unlock
	}
	return treeTX{
		ts: m,
		tx: tree.store,
		// tree:          tree,
		treeID:        treeID,
		hashSizeBytes: hashSizeBytes,
		subtreeCache:  cache,
		writeRevision: -1,
		unlock:        unlock,
	}, nil
}

type treeTX struct {
	closed bool
	tx     *hbaseClient
	ts     *commitTreeStorage
	// tree          *tree
	treeID        int64
	hashSizeBytes int
	subtreeCache  cache.SubtreeCache
	writeRevision int64
	unlock        func()
}

func (t *treeTX) getSubtree(ctx context.Context, treeRevision int64, nodeID storage.NodeID) (*storagepb.SubtreeProto, error) {
	s, err := t.getSubtrees(ctx, treeRevision, []storage.NodeID{nodeID})
	if err != nil {
		return nil, err
	}
	switch len(s) {
	case 0:
		return nil, nil
	case 1:
		return s[0], nil
	default:
		return nil, fmt.Errorf("got %d subtrees, but expected 1", len(s))
	}
}

func (t *treeTX) getSubtrees(ctx context.Context, treeRevision int64, nodeIDs []storage.NodeID) ([]*storagepb.SubtreeProto, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	ret := make([]*storagepb.SubtreeProto, 0, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		if nodeID.PrefixLenBits%8 != 0 {
			return nil, fmt.Errorf("invalid subtree ID - not multiple of 8: %d", nodeID.PrefixLenBits)
		}

		// Look for a nodeID at or below treeRevision:
		for r := treeRevision; r >= 0; r-- {
			key := subtreeKey(t.treeID, r, nodeID)
			subtreeKV, err := t.tx.QualifiedGet("subtrees", key.k, "raw", "bytes")
			if err != nil {
				continue
			}
			var subtree storagepb.SubtreeProto
			if err := proto.Unmarshal(subtreeKV.v.([]byte), &subtree); err != nil {
				continue
			}
			ret = append(ret, &subtree)
			break
		}
	}

	// The InternalNodes cache is possibly nil here, but the SubtreeCache (which called
	// this method) will re-populate it.
	return ret, nil
}

func (t *treeTX) storeSubtrees(ctx context.Context, subtrees []*storagepb.SubtreeProto) error {
	if len(subtrees) == 0 {
		glog.Warning("attempted to store 0 subtrees...")
		return nil
	}

	for _, s := range subtrees {
		s := s
		if s.Prefix == nil {
			panic(fmt.Errorf("nil prefix on %v", s))
		}
		key := subtreeKey(t.treeID, t.writeRevision, storage.NewNodeIDFromHash(s.Prefix))
		// k.(*kv).v = s
		subtreeBytes, err := proto.Marshal(s)
		if err != nil {
			return err
		}
		t.tx.BufferedPut("subtrees", key.k, "raw", "bytes", subtreeBytes)
		// if err := t.tx.Flush(); err != nil {
		// 	return nil
		// }
	}
	return nil
}

// getSubtreesAtRev returns a GetSubtreesFunc which reads at the passed in rev.
func (t *treeTX) getSubtreesAtRev(ctx context.Context, rev int64) cache.GetSubtreesFunc {
	return func(ids []storage.NodeID) ([]*storagepb.SubtreeProto, error) {
		return t.getSubtrees(ctx, rev, ids)
	}
}

// GetMerkleNodes returns the requests nodes at (or below) the passed in treeRevision.
func (t *treeTX) GetMerkleNodes(ctx context.Context, treeRevision int64, nodeIDs []storage.NodeID) ([]storage.Node, error) {
	return t.subtreeCache.GetNodes(nodeIDs, t.getSubtreesAtRev(ctx, treeRevision))
}

func (t *treeTX) SetMerkleNodes(ctx context.Context, nodes []storage.Node) error {
	for _, n := range nodes {
		err := t.subtreeCache.SetNodeHash(n.NodeID, n.Hash,
			func(nID storage.NodeID) (*storagepb.SubtreeProto, error) {
				return t.getSubtree(ctx, t.writeRevision, nID)
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *treeTX) Commit() error {
	defer t.unlock()

	if t.writeRevision > -1 {
		if err := t.subtreeCache.Flush(func(st []*storagepb.SubtreeProto) error {
			flushErr := t.storeSubtrees(context.TODO(), st)
			if flushErr != nil {
				return flushErr
			}
			t.tx.Flush()
			return nil
		}); err != nil {
			glog.Warningf("TX commit flush error: %v", err)
			return err
		}
	}
	t.closed = true
	// update the shared view of the tree post TX:
	// t.tree.store = t.tx
	return nil
}

func (t *treeTX) Rollback() error {
	defer t.unlock()

	t.closed = true
	return nil
}

func (t *treeTX) Close() error {
	if !t.closed {
		err := t.Rollback()
		if err != nil {
			glog.Warningf("Rollback error on Close(): %v", err)
		}
		return err
	}
	return nil
}

func (t *treeTX) IsOpen() bool {
	return !t.closed
}
