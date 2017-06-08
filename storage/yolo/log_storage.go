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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/google/trillian"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/monitoring/prometheus"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"github.com/google/trillian/trees"
	"github.com/tsuna/gohbase"
)

const logIDLabel = "logid"

var (
	defaultLogStrata = []int{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}

	once            sync.Once
	queuedCounter   monitoring.Counter
	dequeuedCounter monitoring.Counter
)

func createMetrics(mf monitoring.MetricFactory) {
	queuedCounter = mf.NewCounter("commit_queued_leaves", "Number of leaves queued", logIDLabel)
	dequeuedCounter = mf.NewCounter("commit_dequeued_leaves", "Number of leaves dequeued", logIDLabel)
}

func labelForTX(t *logTreeTX) string {
	return strconv.FormatInt(t.treeID, 10)
}

func unseqKey(treeID int64) btree.Item {
	return &kv{k: fmt.Sprintf("/%d/unseq", treeID)}
}

func seqLeafKey(treeID, seq int64) btree.Item {
	return &kv{k: fmt.Sprintf("/%d/seq/%020d", treeID, seq)}
}

func hashToSeqKey(treeID int64) btree.Item {
	return &kv{k: fmt.Sprintf("/%d/h2s", treeID)}
}

func sthKey(treeID, timestamp int64) btree.Item {
	return &kv{k: fmt.Sprintf("/%d/sth/%020d", treeID, timestamp)}
}

type commitLogStorage struct {
	*commitTreeStorage
	admin storage.AdminStorage
}

// NewLogStorage creates a commit log LogStorage instance.
func NewLogStorage(kafkaProd sarama.SyncProducer, kafkaCons sarama.Consumer, client gohbase.Client) storage.LogStorage {
	ret := &commitLogStorage{
		commitTreeStorage: newTreeStorage(kafkaProd, kafkaCons, client),
	}
	ret.admin = NewAdminStorage(ret)
	return ret
}

func (m *commitLogStorage) CheckDatabaseAccessible(ctx context.Context) error {
	return nil
}

type readOnlyLogTX struct {
	ms *commitTreeStorage
}

func (m *commitLogStorage) Snapshot(ctx context.Context) (storage.ReadOnlyLogTX, error) {
	return &readOnlyLogTX{m.commitTreeStorage}, nil
}

func (t *readOnlyLogTX) Commit() error {
	return nil
}

func (t *readOnlyLogTX) Rollback() error {
	return nil
}

func (t *readOnlyLogTX) Close() error {
	return nil
}

func (t *readOnlyLogTX) GetActiveLogIDs(ctx context.Context) ([]int64, error) {
	t.ms.mu.RLock()
	defer t.ms.mu.RUnlock()

	ret := make([]int64, 0, len(t.ms.trees))
	for k := range t.ms.trees {
		ret = append(ret, k)
	}
	return ret, nil
}

func (t *readOnlyLogTX) GetActiveLogIDsWithPendingWork(ctx context.Context) ([]int64, error) {
	// just return all trees for now
	return t.GetActiveLogIDs(ctx)
}

func (m *commitLogStorage) beginInternal(ctx context.Context, treeID int64, readonly bool) (storage.LogTreeTX, error) {
	once.Do(func() {
		// TODO(drysdale): this should come from the registry rather than hard-coding use of Prometheus
		createMetrics(prometheus.MetricFactory{})
	})
	tree, err := trees.GetTree(
		ctx,
		m.admin,
		treeID,
		trees.GetOpts{TreeType: trillian.TreeType_LOG, Readonly: readonly})
	if err != nil {
		return nil, err
	}
	hasher, err := trees.Hasher(tree)
	if err != nil {
		return nil, err
	}

	stCache := cache.NewSubtreeCache(defaultLogStrata, cache.PopulateLogSubtreeNodes(hasher), cache.PrepareLogSubtreeWrite())
	ttx, err := m.commitTreeStorage.beginTreeTX(ctx, readonly, treeID, hasher.Size(), stCache)
	if err != nil {
		return nil, err
	}

	ltx := &logTreeTX{
		treeTX: ttx,
		ls:     m,
	}

	ltx.root, err = ltx.fetchLatestRoot(ctx)
	if err != nil {
		ttx.Rollback()
		return nil, err
	}
	ltx.treeTX.writeRevision = ltx.root.TreeRevision + 1

	return ltx, nil
}

func (m *commitLogStorage) BeginForTree(ctx context.Context, treeID int64) (storage.LogTreeTX, error) {
	return m.beginInternal(ctx, treeID, false /* readonly */)
}

func (m *commitLogStorage) SnapshotForTree(ctx context.Context, treeID int64) (storage.ReadOnlyLogTreeTX, error) {
	tx, err := m.beginInternal(ctx, treeID, true /* readonly */)
	if err != nil {
		return nil, err
	}
	return tx.(storage.ReadOnlyLogTreeTX), err
}

type logTreeTX struct {
	treeTX
	ls   *commitLogStorage
	root trillian.SignedLogRoot
}

func (t *logTreeTX) ReadRevision() int64 {
	return t.root.TreeRevision
}

func (t *logTreeTX) WriteRevision() int64 {
	return t.treeTX.writeRevision
}

func (t *logTreeTX) DequeueLeaves(ctx context.Context, limit int, cutoffTime time.Time) ([]*trillian.LogLeaf, error) {
	leaves := make([]*trillian.LogLeaf, 0, limit)

	c, err := t.ls.kafkaCons.ConsumePartition(strconv.FormatInt(t.treeID, 10), 0, t.tree.kafkaOffset)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	p := c.Messages()
	for i := 0; i < limit; i++ {
		// TODO(filippo): consider cutoffTime
		msg, ok := <-p
		if !ok {
			break
		}
		leaf := &trillian.LogLeaf{}
		err = proto.Unmarshal(msg.Value, leaf)
		if err != nil {
			return nil, err
		}
		leaves = append(leaves, leaf)
	}

	dequeuedCounter.Add(float64(len(leaves)), labelForTX(t))
	return leaves, nil
}

func (t *logTreeTX) QueueLeaves(ctx context.Context, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.LogLeaf, error) {
	// Don't accept batches if any of the leaves are invalid.
	for _, leaf := range leaves {
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return nil, fmt.Errorf("queued leaf must have a leaf ID hash of length %d", t.hashSizeBytes)
		}
	}
	queuedCounter.Add(float64(len(leaves)), labelForTX(t))
	// No deduping in this storage!
	for _, l := range leaves {
		encoded, err := proto.Marshal(l)
		if err != nil {
			return nil, err
		}
		// TODO(filippo): batch send, annotate errors
		_, _, err = t.ls.kafkaProd.SendMessage(&sarama.ProducerMessage{
			Topic: strconv.FormatInt(t.treeID, 10),
			Value: sarama.ByteEncoder(encoded),
		})
		if err != nil {
			return nil, err
		}
	}
	return []*trillian.LogLeaf{}, nil
}

func (t *logTreeTX) GetSequencedLeafCount(ctx context.Context) (int64, error) {
	// var sequencedLeafCount int64

	// t.tx.DescendRange(seqLeafKey(t.treeID, math.MaxInt64), seqLeafKey(t.treeID, 0), func(i btree.Item) bool {
	// 	sequencedLeafCount = i.(*kv).v.(*trillian.LogLeaf).LeafIndex + 1
	// 	return false
	// })
	// return sequencedLeafCount, nil
	return t.ls.trees[t.treeID].kafkaOffset, nil
}

func (t *logTreeTX) GetLeavesByIndex(ctx context.Context, leaves []int64) ([]*trillian.LogLeaf, error) {
	ret := make([]*trillian.LogLeaf, 0, len(leaves))
	for _, seq := range leaves {
		leafBytes, err := t.tx.QualifiedGet("subtrees", seqLeafKey(t.treeID, seq).(*kv).k, "raw", "bytes")
		if err != nil {
			return nil, err
		}
		var leaf trillian.LogLeaf
		if err := proto.Unmarshal(leafBytes.v.([]byte), &leaf); err != nil {
			return nil, err
		}
		ret = append(ret, &leaf)
	}
	return ret, nil
}

func (t *logTreeTX) GetLeavesByHash(ctx context.Context, leafHashes [][]byte, orderBySequence bool) ([]*trillian.LogLeaf, error) {
	treeIDJsonBytes, err := t.tx.QualifiedGet("subtrees", hashToSeqKey(t.treeID).(*kv).k, "raw", "bytes")
	if err != nil {
		return nil, err
	}
	var treeIDMap map[string][]int64
	if err := json.Unmarshal(treeIDJsonBytes.v.([]byte), &treeIDMap); err != nil {
		return nil, err
	}

	ret := make([]*trillian.LogLeaf, 0, len(leafHashes))
	for hash := range leafHashes {
		seq, ok := treeIDMap[string(hash)]
		if !ok {
			continue
		}
		for _, s := range seq {
			l, err := t.tx.QualifiedGet("subtrees", seqLeafKey(t.treeID, s).(*kv).k, "raw", "bytes")
			if err != nil {
				continue
			}
			ret = append(ret, l.v.(*trillian.LogLeaf))
		}
	}
	return ret, nil
}

func (t *logTreeTX) LatestSignedLogRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	return t.root, nil
}

// fetchLatestRoot reads the latest SignedLogRoot from the DB and returns it.
func (t *logTreeTX) fetchLatestRoot(ctx context.Context) (trillian.SignedLogRoot, error) {
	r, err := t.tx.QualifiedGet("subtrees", sthKey(t.treeID, t.tree.currentSTH).(*kv).k, "raw", "bytes")
	if err != nil {
		// TODO YOLO
		return trillian.SignedLogRoot{RootHash: []byte("EmptyRoot")}, nil
	}

	var root trillian.SignedLogRoot
	err = proto.Unmarshal(r.v.([]byte), &root)
	if err != nil {
		// TODO YOLO
		return trillian.SignedLogRoot{RootHash: []byte("EmptyRoot")}, nil
	}

	return root, nil
}

func (t *logTreeTX) StoreSignedLogRoot(ctx context.Context, root trillian.SignedLogRoot) error {
	k := sthKey(t.treeID, root.TimestampNanos)
	// k.(*kv).v = root
	encoded, err := proto.Marshal(&root)
	if err != nil {
		return err
	}

	t.tx.BufferedPut("subtrees", k.(*kv).k, "raw", "bytes", encoded)

	// TODO(alcutter): this breaks the transactional model
	if root.TimestampNanos > t.tree.currentSTH {
		t.tree.currentSTH = root.TimestampNanos
	}
	return nil
}

func (t *logTreeTX) UpdateSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf) error {
	countByMerkleHash := make(map[string]int)
	for _, leaf := range leaves {
		// This should fail on insert but catch it early
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return errors.New("Sequenced leaf has incorrect hash size")
		}
		mh := string(leaf.MerkleLeafHash)
		countByMerkleHash[mh]++
		// insert sequenced leaf:
		k := seqLeafKey(t.treeID, leaf.LeafIndex)
		// k.(*kv).v = leaf
		encoded, err := proto.Marshal(leaf)
		if err != nil {
			return err
		}
		t.tx.BufferedPut("subtrees", k.(*kv).k, "raw", "bytes", encoded)

		// update merkle-to-seq mapping:
		key := hashToSeqKey(t.treeID).(*kv).k
		m, err := t.tx.QualifiedGet("subtrees", key, "raw", "bytes")
		if err != nil {
			return err
		}

		var treeIDMap map[string][]int64
		if err := json.Unmarshal(m.v.([]byte), &treeIDMap); err != nil {
			return err
		}

		l := treeIDMap[string(leaf.MerkleLeafHash)]
		l = append(l, leaf.LeafIndex)
		treeIDMap[string(leaf.MerkleLeafHash)] = l

		var treeIDMapBytes []byte
		treeIDMapBytes, err = json.Marshal(treeIDMap)
		if err != nil {
			return err
		}

		t.tx.BufferedPut("subtrees", key, "raw", "bytes", treeIDMapBytes)
	}

	c, err := t.ls.kafkaCons.ConsumePartition(strconv.FormatInt(t.treeID, 10), 0, t.tree.kafkaOffset)
	defer c.Close()
	if err != nil {
		return err
	}
	for msg := range c.Messages() {
		leaf := &trillian.LogLeaf{}
		err = proto.Unmarshal(msg.Value, leaf)
		if err != nil {
			return err
		}
		mh := string(leaf.MerkleLeafHash)
		if countByMerkleHash[mh] == 0 {
			panic("flag")
			return errors.New("tried to dequeue non-contiguous leaves")
		}
		countByMerkleHash[mh]--
		if countByMerkleHash[mh] == 0 {
			delete(countByMerkleHash, mh)
		}
		if len(countByMerkleHash) == 0 {
			break
		}
	}

	if unknown := len(countByMerkleHash); unknown != 0 {
		panic("flag")
		return fmt.Errorf("attempted to update %d unknown leaves: %x", unknown, countByMerkleHash)
	}

	t.tree.kafkaOffset += int64(len(leaves))
	return nil
}

func (t *logTreeTX) getActiveLogIDs(ctx context.Context) ([]int64, error) {
	var ret []int64
	for k := range t.ts.trees {
		ret = append(ret, k)
	}
	return ret, nil
}

// GetActiveLogIDs returns a list of the IDs of all configured logs
func (t *logTreeTX) GetActiveLogIDs(ctx context.Context) ([]int64, error) {
	return t.getActiveLogIDs(ctx)
}

// GetActiveLogIDsWithPendingWork returns a list of the IDs of all configured logs
// that have queued unsequenced leaves that need to be integrated
func (t *logTreeTX) GetActiveLogIDsWithPendingWork(ctx context.Context) ([]int64, error) {
	// TODO(alcutter): only return trees with work to do
	return t.getActiveLogIDs(ctx)
}

// byLeafIdentityHash allows sorting of leaves by their identity hash, so DB
// operations always happen in a consistent order.
type byLeafIdentityHash []*trillian.LogLeaf

func (l byLeafIdentityHash) Len() int {
	return len(l)
}
func (l byLeafIdentityHash) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l byLeafIdentityHash) Less(i, j int) bool {
	return bytes.Compare(l[i].LeafIdentityHash, l[j].LeafIdentityHash) == -1
}

// leafAndPosition records original position before sort.
type leafAndPosition struct {
	leaf *trillian.LogLeaf
	idx  int
}

// byLeafIdentityHashWithPosition allows sorting (as above), but where we need
// to remember the original position
type byLeafIdentityHashWithPosition []leafAndPosition

func (l byLeafIdentityHashWithPosition) Len() int {
	return len(l)
}
func (l byLeafIdentityHashWithPosition) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l byLeafIdentityHashWithPosition) Less(i, j int) bool {
	return bytes.Compare(l[i].leaf.LeafIdentityHash, l[j].leaf.LeafIdentityHash) == -1
}
