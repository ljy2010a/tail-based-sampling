package receiver

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/smallnest/goreq"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"time"
)

type Receiver struct {
	HttpPort       string // 8000,8001
	DataPort       string // 8081
	CompactorPort  string // 8002
	logger         *zap.Logger
	idToTrace      sync.Map
	maxNumTraces   uint64
	numTracesOnMap uint64

	deleteChan chan string
	finishChan chan interface{}
	gzipLen    int
	closeTimes int64
}

var (
	compactorReq = goreq.New()
)

func (r *Receiver) Run() {
	// localhost:{8000,8001,8002}/ready
	// localhost:{8000,8001,8002}/setParameter
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()

	r.deleteChan = make(chan string, 100000)
	r.finishChan = make(chan interface{})
	go r.finish()

	router := gin.Default()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.GET("/qw", r.QueryWrongHandler)
	router.Run(fmt.Sprintf(":%s", r.HttpPort))
}

func (r *Receiver) ReadyHandler(c *gin.Context) {
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) SetParamHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "8081")
	r.DataPort = port
	// 暂时用一个
	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", port)
		r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl))
		go r.ReadHttp(dataUrl)

		dataUrl = fmt.Sprintf("http://127.0.0.1:%s/trace2.data", port)
		r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl))
		go r.ReadHttp(dataUrl)
	}
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) QueryWrongHandler(c *gin.Context) {
	traceId := c.DefaultQuery("id", "")
	tdi, exist := r.idToTrace.Load(traceId)
	if exist {
		c.AbortWithStatus(404)
		return
	}
	td := tdi.(*common.TraceData)
	c.JSON(http.StatusOK, gin.H{
		"td": td,
	})
	return
}

func (r *Receiver) ConsumeTraceData(spans []*common.SpanData) {

	idToSpans := make(map[string][]*common.SpanData)
	for _, traceInfo := range spans {
		//id := common.BytesToString(traceInfo.TraceId)
		id := traceInfo.TraceId
		idToSpans[id] = append(idToSpans[id], traceInfo)
	}

	for id, spans := range idToSpans {
		initialTraceData := &common.TraceData{
			Sd:     spans,
			Source: r.HttpPort,
		}
		d, loaded := r.idToTrace.LoadOrStore(id, initialTraceData)
		if loaded {
			// 已存在
			r.logger.Debug("exist id", zap.String("id", id))
			td := d.(*common.TraceData)
			td.Add(spans)
		} else {
			postDeletion := false
			currTime := time.Now()
			// 淘汰一个
			for !postDeletion {
				select {
				case r.deleteChan <- id:
					postDeletion = true
				default:
					dropId, ok := <-r.deleteChan
					if ok {
						r.dropTrace(dropId, currTime, false)
					}
				}
			}
			//r.logger.Info(id)
		}
	}
}

func (r *Receiver) dropTrace(id string, duration time.Time, keep bool) {
	d, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Error("drop id not exist", zap.String("id", id))
		return
	}
	td := d.(*common.TraceData)
	if !keep {
		r.idToTrace.Delete(id)
	}

	// check status
	wrong := false
	for _, span := range td.Sd {
		if span.Wrong {
			wrong = true
		}
	}

	if wrong {
		// send2compactor no keep
		//r.logger.Info("send wrong id", zap.String("id", id))
		//_, body, err := compactorReq.Post(fmt.Sprintf("http://127.0.0.1:%s/sw", r.CompactorPort)).SendStruct(td).End()
		//r.logger.Info("report checksum",
		//	zap.String("body", body),
		//	zap.Errors("err", err),
		//)
		compactorReq.Post(fmt.Sprintf("http://127.0.0.1:%s/sw", r.CompactorPort)).SendStruct(td).End()
		return
	}
	// keep in cache tty 30s

	//ret, err := common.GzipTd(td)
	//if err != nil {
	//	r.logger.Error("GzipTd err", zap.Error(err))
	//	return
	//}
	//r.gzipLen += len(ret)
}

func (r *Receiver) finish() {
	<-r.finishChan
	r.logger.Info("start clear less")
	for {
		select {
		case id := <-r.deleteChan:
			r.dropTrace(id, time.Now(), true)
		default:
			r.logger.Info("clear less succ")
			r.notifyFIN()
			return
		}
	}
}

func (r *Receiver) notifyFIN() {
	// send fin_noti
	_, body, err := compactorReq.Get(fmt.Sprintf("http://127.0.0.1:%s/fn", r.CompactorPort)).End()
	r.logger.Info("notify FIN",
		zap.String("body", body),
		zap.Errors("err", err),
	)
}
