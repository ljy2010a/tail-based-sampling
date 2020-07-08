package receiver

import (
	"bytes"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"strings"
	"sync"
)

type TData struct {
	Wrong  bool
	Status uint8
	n      uint8
	id     string
	Sbi    []int
}

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
			tdMap: make(map[string]*TData, 7000),
			mu:    sync.RWMutex{},
		}
	}
	return m
}

type TDataMap struct {
	shards []*TDataMapShard
}

func (t *TDataMap) Load(id string) (*TData, bool) {
	shard := t.shards[uint(fnv32(id))&shardNum1]
	//shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	//shard := t.shards[uint(id[0])&shardNum1]
	return shard.Load(id)
}

func (t *TDataMap) LoadOrStore(id string, val *TData) (*TData, bool) {
	shard := t.shards[uint(fnv32(id))&shardNum1]
	//shard := t.shards[uint(fnv32(id))%uint(shardNum)]
	//shard := t.shards[uint(id[0])&shardNum1]
	return shard.LoadOrStore(id, val)
}

//
const prime32 = uint32(16777619)
const offset = uint32(2166136261)

// FNV hash
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < 5; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func fnvi64(key string) int64 {
	hash := int64(2166136261)
	for i := 0; i < 7; i++ {
		hash *= prime64
		hash ^= int64(key[i])
	}
	return hash
	//hash := uint32(2166136261)
	//for i := 0; i < len(key); i++ {
	//	hash *= prime32
	//	hash ^= uint32(key[i])
	//}
	//return int64(hash)
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

var (
	FCode    = []byte("http.status_code=")
	FCode200 = []byte("http.status_code=200")
	Ferr1    = []byte("error=1")
)

func GetTraceIdWrongByByte(l []byte) (string, bool) {
	//firstIdx := bytes.IndexByte(l, '|')
	//id := common.BytesToString(l[:firstIdx])
	//if bytes.Contains(l, Ferr1) {
	//	return id, true
	//}
	//if bytes.Contains(l, FCode) && !bytes.Contains(l, FCode200) {
	//	return id, true
	//}
	//return id, false
	ls := common.BytesToString(l)
	tpos := strings.IndexByte(ls, '|')
	id := ls[:tpos]

	tagPos := tpos + 1
	httpHit := false
	for {
		p := bytes.IndexByte(l[tagPos:], '&')
		if p == -1 {
			//http.status_code=200
			//error=1
			llen := len(l)
			if (tagPos+7) < llen &&
				l[tagPos+0] == 'e' &&
				l[tagPos+1] == 'r' &&
				l[tagPos+2] == 'r' &&
				l[tagPos+3] == 'o' &&
				l[tagPos+4] == 'r' &&
				l[tagPos+5] == '=' &&
				l[tagPos+6] == '1' {
				return id, true
			}
			//fmt.Println(string(l[tagPos:]))
			//if bytes.Equal(l[tagPos:], Ferr1) {
			//	return id, true
			//}
			// http.status_code=
			if tagPos+17 < llen &&
				l[tagPos+0] == 'h' &&
				l[tagPos+1] == 't' &&
				l[tagPos+2] == 't' &&
				l[tagPos+3] == 'p' &&
				l[tagPos+4] == '.' &&
				l[tagPos+5] == 's' &&
				l[tagPos+6] == 't' &&
				l[tagPos+7] == 'a' &&
				l[tagPos+8] == 't' &&
				l[tagPos+9] == 'u' &&
				l[tagPos+10] == 's' &&
				l[tagPos+11] == '_' &&
				l[tagPos+12] == 'c' &&
				l[tagPos+13] == 'o' &&
				l[tagPos+14] == 'd' &&
				l[tagPos+15] == 'e' &&
				l[tagPos+16] == '=' && !httpHit {
				httpHit = true
				if tagPos+19 < llen && (l[tagPos+17] != '2' || l[tagPos+18] != '0' || l[tagPos+19] != '0') {
					return id, true
				}
			}
			return id, false
		} else {
			tagPos += p + 1
			//error=1
			//fmt.Println(string(l[tagPos-8:]))
			if l[tagPos-8] == 'e' &&
				l[tagPos-7] == 'r' &&
				l[tagPos-6] == 'r' &&
				l[tagPos-5] == 'o' &&
				l[tagPos-4] == 'r' &&
				l[tagPos-3] == '=' &&
				l[tagPos-2] == '1' {
				return id, true
			}
			//if bytes.Equal(l[tagPos-8:tagPos-1], Ferr1) {
			//	return id, true
			//}

			// http.status_code=
			//fmt.Println(string(l[tagPos-21:tagPos-4]), string(l[tagPos-21]), string(l[tagPos-4]), string(l[tagPos-3]), string(l[tagPos-2]))
			if l[tagPos-21] == 'h' &&
				l[tagPos-20] == 't' &&
				l[tagPos-19] == 't' &&
				l[tagPos-18] == 'p' &&
				l[tagPos-17] == '.' &&
				l[tagPos-16] == 's' &&
				l[tagPos-15] == 't' &&
				l[tagPos-14] == 'a' &&
				l[tagPos-13] == 't' &&
				l[tagPos-12] == 'u' &&
				l[tagPos-11] == 's' &&
				l[tagPos-10] == '_' &&
				l[tagPos-9] == 'c' &&
				l[tagPos-8] == 'o' &&
				l[tagPos-7] == 'd' &&
				l[tagPos-6] == 'e' &&
				l[tagPos-5] == '=' && !httpHit {
				if l[tagPos-4] == '2' &&
					l[tagPos-3] == '0' &&
					l[tagPos-2] == '0' {
					return id, false
				}
				if l[tagPos-4] != '2' ||
					l[tagPos-3] != '0' ||
					l[tagPos-2] != '0' {
					return id, true
				}
				httpHit = true
			}
			//fmt.Println(string(l[tagPos-21:tagPos-4]), string(l[tagPos-4]), string(l[tagPos-3]), string(l[tagPos-2]))
			//if bytes.Equal(l[tagPos-21:tagPos-4], FCode) && !httpHit {
			//	httpHit = true
			//	if l[tagPos-4] != '2' && l[tagPos-3] != '0' && l[tagPos-2] != '0' {
			//		return id, true
			//	}
			//}
		}
	}

}

func GetTraceIdByString(line []byte) string {
	//l := common.BytesToString(line)
	return common.BytesToString(line[:bytes.IndexByte(line, '|')])
}

func IfSpanWrongString(l []byte) bool {
	//if bytes.Contains(l, Ferr1) {
	//	return true
	//}
	//if bytes.Contains(l, FCode) && !bytes.Contains(l, FCode200) {
	//	return true
	//}
	//return false

	tagPos := 32
	httpHit := false
	//bytes.IndexByte(l[64:], '&')
	//bytes.IndexByte(l[165:], '&')
	//tagPos = 192
	for {
		p := bytes.IndexByte(l[tagPos:], '&')
		if p == -1 {
			//error=1
			llen := len(l)
			if (tagPos+7) < llen &&
				l[tagPos+0] == 'e' &&
				l[tagPos+1] == 'r' &&
				l[tagPos+2] == 'r' &&
				l[tagPos+3] == 'o' &&
				l[tagPos+4] == 'r' &&
				l[tagPos+5] == '=' &&
				l[tagPos+6] == '1' {
				return true
			}
			// http.status_code=
			if tagPos+17 < llen &&
				l[tagPos+0] == 'h' &&
				l[tagPos+1] == 't' &&
				l[tagPos+2] == 't' &&
				l[tagPos+3] == 'p' &&
				l[tagPos+4] == '.' &&
				l[tagPos+5] == 's' &&
				l[tagPos+6] == 't' &&
				l[tagPos+7] == 'a' &&
				l[tagPos+8] == 't' &&
				l[tagPos+9] == 'u' &&
				l[tagPos+10] == 's' &&
				l[tagPos+11] == '_' &&
				l[tagPos+12] == 'c' &&
				l[tagPos+13] == 'o' &&
				l[tagPos+14] == 'd' &&
				l[tagPos+15] == 'e' &&
				l[tagPos+16] == '=' && !httpHit {
				httpHit = true
				if tagPos+19 < llen && (l[tagPos+17] != '2' || l[tagPos+18] != '0' || l[tagPos+19] != '0') {
					return true
				}
			}
			return false
		} else {
			//if tagPos != 64 && p != 20 && p != 7 {
			//	//fmt.Println("s",tagPos,p)
			//	tagPos += p + 1
			//	continue
			//}
			//fmt.Println(tagPos,p)
			tagPos += p + 1
			// http.status_code=
			//fmt.Println(string(l[tagPos-21:tagPos-4]), string(l[tagPos-21]), string(l[tagPos-4]), string(l[tagPos-3]), string(l[tagPos-2]))
			if l[tagPos-21] == 'h' &&
				l[tagPos-20] == 't' &&
				l[tagPos-19] == 't' &&
				l[tagPos-18] == 'p' &&
				l[tagPos-17] == '.' &&
				l[tagPos-16] == 's' &&
				l[tagPos-15] == 't' &&
				l[tagPos-14] == 'a' &&
				l[tagPos-13] == 't' &&
				l[tagPos-12] == 'u' &&
				l[tagPos-11] == 's' &&
				l[tagPos-10] == '_' &&
				l[tagPos-9] == 'c' &&
				l[tagPos-8] == 'o' &&
				l[tagPos-7] == 'd' &&
				l[tagPos-6] == 'e' &&
				l[tagPos-5] == '=' &&
				!httpHit {
				if l[tagPos-4] == '2' &&
					l[tagPos-3] == '0' &&
					l[tagPos-2] == '0' {
					return false
				}
				if l[tagPos-4] != '2' ||
					l[tagPos-3] != '0' ||
					l[tagPos-2] != '0' {
					return true
				}
				httpHit = true
			}

			//error=1
			//fmt.Println(string(l[tagPos-8:]))
			if l[tagPos-8] == 'e' &&
				l[tagPos-7] == 'r' &&
				l[tagPos-6] == 'r' &&
				l[tagPos-5] == 'o' &&
				l[tagPos-4] == 'r' &&
				l[tagPos-3] == '=' &&
				l[tagPos-2] == '1' {
				return true
			}
		}
	}
}
