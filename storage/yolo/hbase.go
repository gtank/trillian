package yolo

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// hbaseClient wraps a gohbase.Client to provide transaction semantics
type hbaseClient struct {
	sync.RWMutex
	client gohbase.Client
	buffer []hbaseWriteOp
}

type hbaseWriteOp struct {
	table  string
	row    string
	values map[string]map[string][]byte
}

func newHBaseClient(client gohbase.Client) *hbaseClient {
	return &hbaseClient{
		client: client,
		buffer: make([]hbaseWriteOp, 0),
	}
}

func (t *hbaseClient) bufferWrite(table, row string, values map[string]map[string][]byte) {
	t.Lock()
	defer t.Unlock()

	newWriteOp := hbaseWriteOp{
		table:  table,
		row:    row,
		values: values,
	}

	t.buffer = append(t.buffer, newWriteOp)
}

func (t *hbaseClient) Flush() error {
	t.RLock()
	for _, op := range t.buffer {
		req, err := hrpc.NewPutStr(context.Background(), op.table, op.row, op.values)
		if err != nil {
			t.RUnlock()
			return err
		}
		_, err = t.client.Put(req)
		if err != nil {
			t.RUnlock()
			return err
		}
	}
	t.RUnlock()

	t.Reset()

	return nil
}

func (t *hbaseClient) Reset() {
	t.Lock()
	t.buffer = t.buffer[:0]
	t.Unlock()
}

func (c *hbaseClient) QualifiedGet(table, row, family, qualifier string) (*kv, error) {
	req, err := hrpc.NewGet(context.Background(), []byte(table), []byte(row))
	if err != nil {
		return nil, err
	}
	res, err := c.client.Get(req)
	if err != nil {
		return nil, err
	}

	for _, cell := range res.Cells {
		if string(cell.Family) != family {
			continue
		}
		if string(cell.Qualifier) != qualifier {
			continue
		}
		kv := &kv{k: row, v: cell.Value}
		return kv, nil
	}
	errorMsg := fmt.Sprintf("row has no %s:%s", family, qualifier)
	return nil, errors.New(errorMsg)
}

func (t *hbaseClient) BufferedPut(table, row, family, qualifier string, value []byte) {
	values := make(map[string]map[string][]byte)
	values[family][qualifier] = value
	t.bufferWrite(table, row, values)
}

// mockClient implements gohbase.Client for testing
type mockClient struct {
	sync.RWMutex
	tables   map[string]map[string]hrpc.Result
	zkquorum string
	options  []gohbase.Option
}

func newMockClient(zkquorum string, options ...gohbase.Option) mockClient {
	return mockClient{
		tables:   make(map[string]map[string]hrpc.Result),
		zkquorum: zkquorum,
		options:  options,
	}
}

func (m *mockClient) Scan(s *hrpc.Scan) hrpc.Scanner              { panic("not implemented") }
func (m *mockClient) Get(g *hrpc.Get) (*hrpc.Result, error)       { panic("not implemented") }
func (m *mockClient) Put(p *hrpc.Mutate) (*hrpc.Result, error)    { panic("not implemented") }
func (m *mockClient) Delete(d *hrpc.Mutate) (*hrpc.Result, error) { panic("not implemented") }
func (m *mockClient) Append(a *hrpc.Mutate) (*hrpc.Result, error) { panic("not implemented") }
func (m *mockClient) Increment(i *hrpc.Mutate) (int64, error)     { panic("not implemented") }
func (m *mockClient) CheckAndPut(p *hrpc.Mutate, family string, qualifier string, expectedValue []byte) (bool, error) {
	panic("not implemented")
}
func (m *mockClient) Close() { return }
