package common

import "sync"

const (
	TraceStatusReady    = 0
	TraceStatusWrongSet = 1
	TraceStatusSkip     = 2
	TraceStatusSended   = 3
	TraceStatusDone     = 4
)

func (m *TraceData) AddSpan(newSpans [][]byte) {
	m.Lock()
	m.Sb = append(m.Sb, newSpans...)
	m.Unlock()
}

type SpanData struct {
	StartTime string
	Tags      []byte
}

// traceId：全局唯一的Id，用作整个链路的唯一标识与组装
//startTime：调用的开始时间
//spanId: 调用链中某条数据(span)的id
//parentSpanId: 调用链中某条数据(span)的父亲id，头节点的span的parantSpanId为0
//duration：调用耗时
//serviceName：调用的服务名
//spanName：调用的埋点名
//host：机器标识，比如ip，机器名
//tags: 链路信息中tag信息，存在多个tag的key和value信息。格式为key1=val1&key2=val2&key3=val3 比如 http.status_code=200&error=1
type Spans []*SpanData

func (s Spans) Len() int           { return len(s) }
func (s Spans) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Spans) Less(i, j int) bool { return s[i].StartTime < s[j].StartTime }

type TData struct {
	Wrong  bool
	Status uint8
	Sbi    []int
	//sync.Mutex
}

//func (m *TData) AddSpani(newSpans []int) {
//	m.Lock()
//	m.Sbi = append(m.Sbi, newSpans...)
//	m.Unlock()
//}

type TDataMapShard struct {
	mu    sync.RWMutex
	tdMap map[string]*TData
}

func (t *TDataMapShard) LoadOrStore(id string, val *TData) (*TData, bool) {
	t.mu.Lock()
	if v, ok := t.tdMap[id]; ok {
		t.mu.Unlock()
		return v, true
	} else {
		t.tdMap[id] = val
		t.mu.Unlock()
		return val, false
	}
}

func (t *TDataMapShard) Load(id string) (*TData, bool) {
	t.mu.RLock()
	if v, ok := t.tdMap[id]; ok {
		t.mu.RUnlock()
		return v, true
	} else {
		t.mu.RUnlock()
		return nil, false
	}
}

const shardNum = 128

func NewTDataMap() *TDataMap {
	m := &TDataMap{shards: make([]*TDataMapShard, shardNum)}
	for i := 0; i < shardNum; i++ {
		m.shards[i] = &TDataMapShard{
			tdMap: make(map[string]*TData, 8000),
			mu:    sync.RWMutex{},
		}
	}
	return m
}

type TDataMap struct {
	shards []*TDataMapShard
}

func (t *TDataMap) Load(id string) (*TData, bool) {
	shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	return shard.Load(id)
}

func (t *TDataMap) LoadOrStore(id string, val *TData) (*TData, bool) {
	shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	return shard.LoadOrStore(id, val)
}

const prime32 = uint32(16777619)

// FNV hash
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
