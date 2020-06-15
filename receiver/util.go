package receiver

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"sync"
	"time"
)

var (
	c = &fasthttp.Client{
		MaxConnsPerHost:     20000,
		MaxIdleConnDuration: 10 * time.Second,
		ReadTimeout:         500 * time.Millisecond,
		WriteTimeout:        500 * time.Millisecond,
	}
	//reqPool *ants.Pool
)

//func init() {
//	reqPool, _ = ants.NewPool(1000, ants.WithPreAlloc(true))
//}

func (r *Receiver) SendWrongRequest(id string, td *TData, over string) {

	if over == "1" {
		r.overWg.Add(1)
		defer r.overWg.Done()
	}

	rtd := &common.TraceData{
		Id:     id,
		Source: r.HttpPort,
		Sb:     make([][]byte, len(td.Sbi)),
	}
	for i, val := range td.Sbi {
		start := val >> 16
		llen := val & 0xffff
		rtd.Sb[i] = r.consumer.lineBlock[start : start+llen]
	}

	b, _ := rtd.Marshal()
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(r.CompactorSetWrongUrl + fmt.Sprintf("?over=%s", over))
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		fmt.Printf("set wrong fail id[%v] err[%v] \n", id, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("set wrong fail[%v] code[%v]\n", id, resp.StatusCode())
		return
	}

}

func (r *Receiver) notifyFIN() {
	notifyUrl := fmt.Sprintf("http://127.0.0.1:%s/fn?port=%s", r.CompactorPort, r.HttpPort)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(notifyUrl)
	req.Header.SetMethod("GET")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		r.logger.Info("send notify fin",
			zap.Error(err),
		)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		r.logger.Info("send notify fin",
			zap.Int("code", resp.StatusCode()),
		)
		return
	}
}

func (r *Receiver) warmUp(wg sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	notifyUrl := fmt.Sprintf("http://127.0.0.1:%s/warmup?port=%s", r.CompactorPort, r.HttpPort)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(notifyUrl)
	req.Header.SetMethod("GET")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		r.logger.Info("send notify fin",
			zap.Error(err),
		)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		r.logger.Info("send notify fin",
			zap.Int("code", resp.StatusCode()),
		)
		return
	}
}

type TData struct {
	Wrong  bool
	Status uint8
	Sbi    []int
	//sync.Mutex
}

func NewTData() *TData {
	return &TData{
		Sbi: make([]int, 0, 60),
	}
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
const shardNum1 = shardNum - 1

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
	//shard := t.shards[fnv64(id)&shardNum1]
	shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	return shard.Load(id)
}

func (t *TDataMap) LoadOrStore(id string, val *TData) (*TData, bool) {
	//shard := t.shards[fnv64(id)&shardNum1]
	shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	return shard.LoadOrStore(id, val)
}

//
const prime32 = uint32(16777619)
const offset = uint32(2166136261)

// FNV hash
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

const (
	// offset64 FNVa offset basis. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	offset64 = 14695981039346656037
	// prime64 FNVa prime value. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	prime64 = 1099511628211
)

// Sum64 gets the string and returns its uint64 hash value.
func fnv64(key string) uint {
	var hash uint = offset64
	for i := 0; i < len(key); i++ {
		hash ^= uint(key[i])
		hash *= prime64
	}
	return hash
}
