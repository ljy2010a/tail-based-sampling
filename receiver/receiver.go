package receiver

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/smallnest/goreq"
	"go.uber.org/zap"
	"net/http"
	"os"
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
	sync.Mutex
}

var (
	compactorReq = goreq.New()
)

func (r *Receiver) Run() {
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()
	go func() {
		i := 0
		for {
			if i > 4 {
				r.logger.Info("too long to stop")
				time.Sleep(10 * time.Second)
				os.Exit(0)
			}
			i++
			r.logger.Info("sleep",
				zap.String("port", r.HttpPort),
				zap.Int("i", i),
			)
			time.Sleep(1 * time.Minute)
		}
	}()

	go func() {
		time.Sleep(30 * time.Second)
		if r.DataPort != "" {
			r.logger.Info("has dataport")
			return
		}
		r.logger.Info("try to detect")

		port := 8000
		for i := 0; i < 1000; i++ {
			port++
			dataUrl := fmt.Sprintf("http://127.0.0.1:%d/trace1.data", port)
			resp, err := http.Get(dataUrl)
			if err != nil {
				continue
			}
			resp.Body.Close()
			r.logger.Info("detect port",
				zap.Int("port", port),
				zap.Int("code", resp.StatusCode),
			)

			if resp.StatusCode == 200 && r.HttpPort == "8000" {
				r.DataPort = fmt.Sprintf("%d", port)

				dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
				go r.ReadHttp(dataUrl)

				dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
				go r.ReadHttp(dataUrl2)
				return
			}
		}
	}()

	r.deleteChan = make(chan string, 100000)
	r.finishChan = make(chan interface{})
	go r.finish()

	router := gin.New()
	//router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.GET("/qw", r.QueryWrongHandler)
	err := router.Run(fmt.Sprintf(":%s", r.HttpPort))
	if err != nil {
		r.logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Receiver) ReadyHandler(c *gin.Context) {
	r.logger.Info("ready", zap.String("port", r.HttpPort))
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) SetParamHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "")
	r.DataPort = port
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)
	// 暂时用一个
	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
		//r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl))
		go r.ReadHttp(dataUrl)

		//dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		////r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl2))
		//go r.ReadHttp(dataUrl2)
	}

	if r.HttpPort == "8001" {
		dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		//r.logger.Info("gen dataUrl", zap.String("dataUrl", dataUrl2))
		go r.ReadHttp(dataUrl2)
	}
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Receiver) QueryWrongHandler(c *gin.Context) {
	id := c.DefaultQuery("id", "")
	tdi, exist := r.idToTrace.Load(id)
	if !exist {
		c.AbortWithStatus(404)
		return
	}
	td := tdi.(*common.TraceData)
	if len(td.Sd) == 0 {
		r.logger.Info("span expire",
			zap.String("id", id),
		)
	}
	c.JSON(http.StatusOK, td)
	return
}

func (r *Receiver) ConsumeTraceData(spans []*common.SpanData) {

	idToSpans := make(map[string][]*common.SpanData)
	for _, span := range spans {
		//id := common.BytesToString(span.TraceId)
		id := span.TraceId
		idToSpans[id] = append(idToSpans[id], span)
		//span.TraceId = ""
	}

	for id, spans := range idToSpans {
		initialTraceData := &common.TraceData{
			Sd:     spans,
			Id:     id,
			Source: r.HttpPort,
		}
		d, loaded := r.idToTrace.LoadOrStore(id, initialTraceData)
		if loaded {
			// 已存在
			//r.logger.Debug("exist id", zap.String("id", id))
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
	r.Lock()
	defer r.Unlock()
	d, ok := r.idToTrace.Load(id)
	if !ok {
		r.logger.Error("drop id not exist", zap.String("id", id))
		return
	}
	td := d.(*common.TraceData)
	if !keep {
		//r.idToTrace.Delete(id)
		td.Sd = common.Spans{}
	}

	wrong := false
	for _, span := range td.Sd {
		if span.Wrong {
			wrong = true
		}
	}

	if wrong {
		// send2compactor no keep
		//r.logger.Info("send wrong id", zap.String("id", id))
		swUrl := fmt.Sprintf("http://127.0.0.1:%s/sw", r.CompactorPort)
		compactorReq.Post(swUrl).SendStruct(td).End()
		//b, _ := json.Marshal(td)
		//go func(_b []byte) {
		//	compactorReq.Post(swUrl).SendRawBytes(_b).End()
		//}(b)
		return
	}
	// keep in cache tty 5s

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
	// send fin
	_, body, err := compactorReq.Get(fmt.Sprintf("http://127.0.0.1:%s/fn?port=%s", r.CompactorPort, r.HttpPort)).End()
	r.logger.Info("send notify fin",
		zap.String("body", body),
		zap.Errors("err", err),
	)
	r.logger.Info("shutdown", zap.String("port", r.HttpPort))
	//time.Sleep(10 * time.Second)
	//os.Exit(0)
}
