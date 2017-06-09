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
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/trillian"
	"github.com/google/trillian/storage"
)

// NewAdminStorage returns a storage.AdminStorage implementation backed by
// commitTreeStorage.
func NewAdminStorage(ms storage.LogStorage) storage.AdminStorage {
	return &commitAdminStorage{ms.(*commitLogStorage).commitTreeStorage}
}

// commitAdminStorage implements storage.AdminStorage
type commitAdminStorage struct {
	ms *commitTreeStorage
}

func (s *commitAdminStorage) Snapshot(ctx context.Context) (storage.ReadOnlyAdminTX, error) {
	return s.Begin(ctx)
}

func (s *commitAdminStorage) Begin(ctx context.Context) (storage.AdminTX, error) {
	return &adminTX{ms: s.ms}, nil
}

type adminTX struct {
	ms *commitTreeStorage
	// mu guards reads/writes on closed, which happen only on
	// Commit/Rollback/IsClosed/Close methods.
	// We don't check closed on *all* methods (apart from the ones above),
	// as we trust tx to keep tabs on its state (and consequently fail to do
	// queries after closed).
	mu     sync.RWMutex
	closed bool
}

func (t *adminTX) Commit() error {
	// TODO(al): The admin implementation isn't transactional
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *adminTX) Rollback() error {
	// TODO(al): The admin implementation isn't transactional
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *adminTX) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

func (t *adminTX) Close() error {
	// Acquire and release read lock manually, without defer, as if the txn
	// is not closed Rollback() will attempt to acquire the rw lock.
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if !closed {
		err := t.Rollback()
		if err != nil {
			glog.Warningf("Rollback error on Close(): %v", err)
		}
		return err
	}
	return nil
}

func (t *adminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	treeKey := metaKey(treeID, "tree").(*kv).k
	treeProtoBytes, err := t.ms.hbase.QualifiedGet("subtrees", treeKey, "raw", "bytes")
	if err != nil {
		return nil, err
	}
	var tree trillian.Tree
	if err := proto.Unmarshal(treeProtoBytes.v.([]byte), &tree); err != nil {
		return nil, err
	}
	return &tree, nil
}

func (t *adminTX) ListTreeIDs(ctx context.Context) ([]int64, error) {
	t.ms.mu.RLock()
	defer t.ms.mu.RUnlock()

	var treeListJsonBytes *kv
	treeListJsonBytes, err := t.ms.hbase.QualifiedGet("subtrees", "/meta/tree_list", "raw", "bytes")
	if err != nil {
		if err == ErrDoesNotExist {
			treeListJsonBytes.v = []byte("[]")
		}
		return nil, err
	}
	var treeIDList []int64
	if err := json.Unmarshal(treeListJsonBytes.v.([]byte), &treeIDList); err != nil {
		return nil, err
	}
	return treeIDList, nil
}

func (t *adminTX) ListTrees(ctx context.Context) ([]*trillian.Tree, error) {
	t.ms.mu.RLock()
	defer t.ms.mu.RUnlock()

	treeIDList, err := t.ListTreeIDs(ctx)
	if err != nil {
		return nil, err
	}
	treeList := make([]*trillian.Tree, len(treeIDList))
	for i := 0; i < len(treeIDList); i++ {
		tree, err := t.GetTree(ctx, treeIDList[i])
		if err != nil {
			glog.Warningf("tried to get tree with id %d, got err: %v", treeIDList[i], err)
			continue
		}
		treeList[i] = tree
	}
	return treeList, nil
}

func (t *adminTX) CreateTree(ctx context.Context, tr *trillian.Tree) (*trillian.Tree, error) {
	if err := storage.ValidateTreeForCreation(tr); err != nil {
		return nil, err
	}
	if err := validateStorageSettings(tr); err != nil {
		return nil, err
	}

	id, err := storage.NewTreeID()
	if err != nil {
		return nil, err
	}

	nowMillis := toMillisSinceEpoch(time.Now())

	meta := *tr
	meta.TreeId = id
	meta.CreateTimeMillisSinceEpoch = nowMillis
	meta.UpdateTimeMillisSinceEpoch = nowMillis

	t.ms.mu.Lock()
	defer t.ms.mu.Unlock()

	newTree := t.ms.newTree(meta)
	newTreeKey := metaKey(meta.TreeId, "tree")
	encoded, err := proto.Marshal(&meta)
	if err != nil {
		return nil, err
	}
	newTree.store.BufferedPut("subtrees", newTreeKey.(*kv).k, "raw", "bytes", encoded)

	// add new tree to the index
	var treeListJsonBytes *kv
	treeListJsonBytes, err = newTree.store.QualifiedGet("subtrees", "/meta/tree_list", "raw", "bytes")
	if err != nil {
		if err == ErrDoesNotExist {
			treeListJsonBytes = &kv{k: "/meta/tree_list", v: []byte("[]")}
		} else {
			return nil, err
		}
	}
	var treeIDList []int64
	if err := json.Unmarshal(treeListJsonBytes.v.([]byte), &treeIDList); err != nil {
		return nil, err
	}
	treeIDList = append(treeIDList, meta.TreeId)
	treeListJsonBytes.v, err = json.Marshal(treeIDList)
	if err != nil {
		return nil, err
	}
	newTree.store.BufferedPut("subtrees", "/meta/tree_list", "raw", "bytes", treeListJsonBytes.v.([]byte))

	// persist the new tree before returning
	if err := newTree.store.Flush(); err != nil {
		return nil, err
	}

	glog.Infof("tree: %v", meta)

	return &meta, nil
}

func (t *adminTX) UpdateTree(ctx context.Context, treeID int64, updateFunc func(*trillian.Tree)) (*trillian.Tree, error) {
	mTree, err := t.ms.getTree(treeID)
	if err != nil {
		return nil, err
	}
	mTree.mu.Lock()
	defer mTree.mu.Unlock()

	tree := mTree.meta
	beforeUpdate := *tree
	updateFunc(tree)
	if err := storage.ValidateTreeForUpdate(&beforeUpdate, tree); err != nil {
		return nil, err
	}
	if err := validateStorageSettings(tree); err != nil {
		return nil, err
	}

	tree.UpdateTimeMillisSinceEpoch = toMillisSinceEpoch(time.Now())
	return tree, nil
}

func toMillisSinceEpoch(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func validateStorageSettings(tree *trillian.Tree) error {
	if tree.StorageSettings != nil {
		return fmt.Errorf("storage_settings not supported, but got %v", tree.StorageSettings)
	}
	return nil
}
