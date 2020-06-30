package receiver

import (
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type Receiver struct {
	HttpPort             string // 8000,8001
	DataPort             string // 8081
	CompactorPort        string // 8002
	CompactorSetWrongUrl string
	idToTrace            *TDataMap

	dropIdQueue  chan string
	finishSingle chan interface{}

	overWg sync.WaitGroup

	traceNums   int64
	maxSpanNums int
	minSpanNums int
	mapMaxSize  int
	mapMinSize  int
	traceMiss   int
	traceSkip   int
	wrongHit    int

	//tdCache    []*TData
	tdCachePos int64
	//tdCacheLimit int64

	//tdSendSlice      []*common.TraceData
	//tdSendSlicePos   int64
	//tdSendSliceLimit int64

	idMapCache chan *Map

	linesQueue chan []int
	//linesBatchNum int
	readBufSize int
	doneWg      sync.WaitGroup
	//linesBufLen   int
	//linesBuf      []byte
	//lines        []int
	linesCache chan []int
	readChan   chan PP
}

func NewTData() *TData {
	return &TData{
		Sbi: make([]int, 110),
	}
}

func NewIdMapCache() map[string]*TData {
	return make(map[string]*TData, 10000)
}

const (
	linesBufLen = int(2.5 * 1024 * 1024 * 1024)

	linesBatchNum = 20_0000
	batchNum      = 130

	tdCacheLimit = 85_0000

	workNum = 2

	extDownloader = 2

	downloadStepSize = 512 * 1024 * 1024
)

var (
	linesBuf = make([]byte, linesBufLen)
	tdCache  = make([]*TData, tdCacheLimit)
)

func (r *Receiver) Run() {
	//go func() {
	//	i := 0
	//	for {
	//		if i > 10 {
	//			return
	//		}
	//		b2Mb := func(b uint64) uint64 {
	//			return b / 1024 / 1024
	//		}
	//		var m runtime.MemStats
	//		runtime.ReadMemStats(&m)
	//
	//		logger.Info("MEM STAT",
	//			zap.Int("times", i),
	//			zap.Uint64("Alloc", b2Mb(m.Alloc)),
	//			zap.Uint64("TotalAlloc", b2Mb(m.TotalAlloc)),
	//			zap.Uint64("HeapInuse", b2Mb(m.HeapInuse)),
	//			zap.Uint64("HeapAlloc", b2Mb(m.HeapAlloc)),
	//			zap.Uint64("Sys", b2Mb(m.Sys)),
	//			zap.Uint32("NumGC", m.NumGC),
	//		)
	//		i++
	//		time.Sleep(1 * time.Second)
	//	}
	//}()

	//r.tdSendSliceLimit = 1_2000
	//r.tdSendSlice = make([]*common.TraceData, r.tdSendSliceLimit)
	//for i := int64(0); i < r.tdSendSliceLimit; i++ {
	//	r.tdSendSlice[i] = &common.TraceData{
	//		Source: r.HttpPort,
	//		Sb:     make([][]byte, 0, 60),
	//	}
	//}
	for i := int64(0); i < tdCacheLimit; i++ {
		tdCache[i] = NewTData()
	}

	r.readBufSize = 64 * 1024 * 1024

	r.idMapCache = make(chan *Map, batchNum)
	for i := 0; i < batchNum; i++ {
		r.idMapCache <- New(10000, 0.99)
	}

	r.linesQueue = make(chan []int, batchNum)
	r.linesCache = make(chan []int, batchNum)
	for i := 0; i < batchNum; i++ {
		r.linesCache <- make([]int, linesBatchNum)
	}

	r.idToTrace = NewTDataMap()

	r.dropIdQueue = make(chan string, 6000)
	r.finishSingle = make(chan interface{})

	//r.linesBufLen = blockLen
	//r.linesBuf = make([]byte, blockLen)
	r.readChan = make(chan PP, 1024)

	go r.readIndex()
	for i := 0; i < workNum; i++ {
		go r.readLines()
	}
	go r.finish()

	r.RunHttpServer()
}

//func (r *Receiver) ConsumeByte(lines []int, idToSpans2 map[string]*TData, tdCachePos *int64, tdCache []*TData) {
func (r *Receiver) ConsumeByte(lines []int) {
	var idToSpans *Map
	select {
	case idToSpans = <-r.idMapCache:
	default:
		idToSpans = New(10000, 0.99)
	}
	//idToSpans := make(map[string]*TData, 10000)
	for i, val := range lines {
		start := val >> 16
		llen := val & 0xffff
		line := linesBuf[start : start+llen]
		//GetTraceIdByString(line)
		//IfSpanWrongString(line)
		//continue
		id := GetTraceIdByString(line)
		idh := fnvi64(id)
		if etdp, ok := idToSpans.Get(idh); !ok {
			var td *TData
			nowPos := atomic.AddInt64(&r.tdCachePos, 1)
			if nowPos < tdCacheLimit {
				td = tdCache[nowPos]
			} else {
				td = NewTData()
			}
			idToSpans.Put(idh, nowPos)
			td.Wrong = IfSpanWrongString(line)
			td.id = id
			if i > 2_0000 && i < linesBatchNum-2_0000 {
				td.Status = common.TraceStatusSkip
			}
			td.Sbi[0] = val
			td.n++
			//td.Sbi = append(td.Sbi, val)
			//idToSpans[id] = nowPos
		} else {
			etd := tdCache[etdp]
			if !etd.Wrong && IfSpanWrongString(line) {
				etd.Wrong = true
			}
			//etd.Sbi = append(etd.Sbi, val)
			etd.Sbi[etd.n] = val
			etd.n++
		}
	}

	//mapSize := len(idToSpans)
	//if mapSize > r.mapMaxSize {
	//	r.mapMaxSize = mapSize
	//}

	for i := 0; i < len(idToSpans.data); i += 2 {
		if idToSpans.data[i] == FREE_KEY {
			continue
		}
		etdp := idToSpans.data[i+1]
		etd := tdCache[etdp]
		id := etd.id
		if etd.Status == common.TraceStatusSkip && etd.Wrong {
			//r.traceSkip++
			r.dropTrace(id, etd, "0")
			continue
		}
		td, exist := r.idToTrace.LoadOrStore(id, etd)
		if exist {
			// 已存在
			//r.traceMiss++
			//td.Sbi = append(td.Sbi, etd.Sbi...)
			copy(td.Sbi[td.n:], etd.Sbi[:etd.n])
			td.n += etd.n
			if !td.Wrong && etd.Wrong {
				td.Wrong = true
			}
			if td.Status == common.TraceStatusWrongSet {
				goto SET_AND_DROP
			}
			continue
		}
		if td.Status == common.TraceStatusSkip {
			//r.traceSkip++
			r.dropTrace(id, td, "0")
			continue
		}
	SET_AND_DROP:
		postDeletion := false
		for !postDeletion {
			select {
			case r.dropIdQueue <- id:
				postDeletion = true
			default:
				//<-r.dropIdQueue
				dropId, ok := <-r.dropIdQueue
				if ok {
					r.dropTraceById(dropId, "0")
				}
			}
		}
	}
}

func (r *Receiver) dropTraceById(id string, over string) {
	td, ok := r.idToTrace.Load(id)
	if !ok {
		logger.Info("drop id not exist", zap.String("id", id))
		return
	}
	r.dropTrace(id, td, over)
}

func (r *Receiver) dropTrace(id string, td *TData, over string) {
	//atomic.AddInt64(&r.traceNums, 1)
	//spLen := len(td.Sbi)
	//if r.maxSpanNums < spLen {
	//	r.maxSpanNums = spLen
	//}
	//if r.minSpanNums > spLen || r.minSpanNums == 0 {
	//	r.minSpanNums = spLen
	//}
	wrong := td.Wrong
	if wrong && td.Status != common.TraceStatusSended {
		td.Status = common.TraceStatusSended
		go r.SendWrongRequest(id, td, over)
		return
	} else {
		td.Status = common.TraceStatusDone
	}
}

func (r *Receiver) finish() {
	btime := time.Now()
	ftime := time.Second
	fwg := sync.WaitGroup{}
	fonce := sync.Once{}
	for i := 0; i < 2; i++ {
		fwg.Add(1)
		go func() {
			defer fwg.Done()
			<-r.finishSingle
			fonce.Do(func() {
				logger.Info("finish start")
				btime = time.Now()
			})
			for {
				select {
				case dropId := <-r.dropIdQueue:
					r.dropTraceById(dropId, "1")
				default:
					ftime = time.Since(btime)
					return
				}
			}

		}()
	}
	fwg.Wait()
	r.overWg.Wait()
	logger.Info("finish over",
		zap.Duration("finish cost", ftime),
		zap.Duration("total cost", time.Since(btime)),
		zap.Int64("traceNum", r.traceNums),
		zap.Int("maxSpLen", r.maxSpanNums),
		zap.Int("minSpanNums", r.minSpanNums),
		zap.Int("mapMaxSize", r.mapMaxSize),
		zap.Int64("tdCachePos", r.tdCachePos),
		zap.Int("traceMiss", r.traceMiss),
		zap.Int("traceSkip", r.traceSkip),
		zap.Int("wrongHit", r.wrongHit),
	)
	//for i := range r.idToTrace.shards {
	//	logger.Info("shard",
	//		zap.Int("i", len(r.idToTrace.shards[i].tdMap)),
	//	)
	//}
	r.notifyFIN()
}
