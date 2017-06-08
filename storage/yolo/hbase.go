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

// the RPC format makes it hard to mock the HBase client. let's cheat
var yolo_values map[string]map[string][]byte

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
		//TODO(gtank): remove this awful debug hack
		yolo_values = op.values
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
	// fmt.Printf("Get: %s[%s] -> %s:%s\n", table, row, family, qualifier)
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
	// fmt.Printf("Put: %s[%s] -> %s:%s\n", table, row, family, qualifier)
	values := make(map[string]map[string][]byte)
	values[family] = make(map[string][]byte)
	values[family][qualifier] = value
	t.bufferWrite(table, row, values)
}

// mockClient implements gohbase.Client for testing
type MockClient struct {
	sync.RWMutex
	tables   map[string]map[string]*hrpc.Result
	zkquorum string
	options  []gohbase.Option
}

func NewMockClient(zkquorum string, options ...gohbase.Option) MockClient {
	return MockClient{
		tables: make(map[string]map[string]*hrpc.Result, 1),
	}
}

func (m MockClient) Get(g *hrpc.Get) (*hrpc.Result, error) {
	table := string(g.Table())
	row := string(g.Key())
	m.RLock()
	if m.tables[table] == nil {
		m.tables[table] = make(map[string]*hrpc.Result)
	}
	result := m.tables[table][row]
	m.RUnlock()
	if result != nil {
		return result, nil
	}
	return nil, errors.New("key does not exist: " + row)
}

func (m MockClient) Put(p *hrpc.Mutate) (*hrpc.Result, error) {
	table := string(p.Table())
	row := string(p.Key())

	// TODO mock general requests
	cell := &hrpc.Cell{
		Row:       p.Key(),
		Family:    []byte("raw"),
		Qualifier: []byte("bytes"),
		Value:     yolo_values["raw"]["bytes"],
	}
	result := &hrpc.Result{
		Cells: []*hrpc.Cell{cell},
	}

	m.Lock()
	if m.tables[table] == nil {
		m.tables[table] = make(map[string]*hrpc.Result)
	}
	m.tables[table][row] = result
	m.Unlock()

	return result, nil
}

func (m MockClient) Scan(s *hrpc.Scan) hrpc.Scanner              { panic("Scan not implemented") }
func (m MockClient) Delete(d *hrpc.Mutate) (*hrpc.Result, error) { panic("Delete not implemented") }
func (m MockClient) Append(a *hrpc.Mutate) (*hrpc.Result, error) { panic("Append not implemented") }
func (m MockClient) Increment(i *hrpc.Mutate) (int64, error)     { panic("Incrememt not implemented") }
func (m MockClient) CheckAndPut(p *hrpc.Mutate, family string, qualifier string, expectedValue []byte) (bool, error) {
	panic("CheckAndPut not implemented")
}
func (m MockClient) Close() { return }
