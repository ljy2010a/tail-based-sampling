package compactor

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/smallnest/goreq"
	"go.uber.org/zap"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Processor struct {
}

type Compactor struct {
	HttpPort string // 8003
	DataPort string // 8081
	logger   *zap.Logger

	finishChan  chan interface{}
	gzipLen     int
	checkSumMap map[string]string
	closeTimes  int64
	resultChan  chan string
	idToTrace   sync.Map
}

func (r *Compactor) Run() {
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()
	go func() {
		i := 0
		for {
			if i > 4 {
				r.logger.Info("too long to stop")
				close(r.finishChan)
				r.finish()
				return
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
			if resp.StatusCode == 200 {
				r.DataPort = fmt.Sprintf("%d", port)
				return
			}
		}
	}()

	r.resultChan = make(chan string, 50000)
	r.finishChan = make(chan interface{})
	r.checkSumMap = make(map[string]string)
	go r.finish()

	router := gin.New()
	//router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.POST("/sw", r.SetWrongHandler)
	router.GET("/fn", r.FinishNotifyHandler)
	err := router.Run(fmt.Sprintf(":%s", r.HttpPort))
	if err != nil {
		r.logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Compactor) ReadyHandler(c *gin.Context) {
	r.logger.Info("ready", zap.String("port", r.HttpPort))
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Compactor) SetParamHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "")
	r.DataPort = port
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Compactor) SetWrongHandler(c *gin.Context) {
	td := &common.TraceData{}
	err := c.BindJSON(&td)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	tdi, exist := r.idToTrace.LoadOrStore(td.Id, td)
	if !exist {
		r.resultChan <- td.Id
		// notify another
		anotherPort := "8000"
		if td.Source == "8000" {
			anotherPort = "8001"
		}
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/qw?id=%s", anotherPort, td.Id)
		resp, err := http.Get(dataUrl)
		if err != nil {
			r.logger.Info("get another wrong fail",
				zap.String("id", td.Id),
				zap.Error(err),
			)
		} else {
			resp.Body.Close()
		}
	} else {
		otd := tdi.(*common.TraceData)
		otd.Add(td.Sd)
	}

	//
	//// query another wrong
	//if td.Id == "c074d0a90cd607b" {
	//	r.logger.Info("example",
	//		zap.Int("len", td.Sd.Len()),
	//	)
	//}
	//
	//sort.Sort(td.Sd)
	//checkSum := CompactMd5(td)
	//r.mu.Lock()
	//r.checkSumMap[td.Id] = checkSum
	//r.mu.Unlock()
	c.AbortWithStatus(http.StatusOK)
	return
}

func (r *Compactor) FinishNotifyHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "")
	r.logger.Info("got notify fin ", zap.String("port", port))
	times := atomic.AddInt64(&r.closeTimes, 1)
	if times == 2 {
		close(r.finishChan)
	}
	return
}

func (r *Compactor) finish() {
	for {
		select {
		case <-r.finishChan:
			btime := time.Now()
			for {
				select {
				case id := <-r.resultChan:
					tdi, exist := r.idToTrace.Load(id)
					if !exist {

					} else {
						td := tdi.(*common.TraceData)
						if td.Id == "c074d0a90cd607b" {
							r.logger.Info("example",
								zap.Int("len", td.Sd.Len()),
							)
						}
						sort.Sort(td.Sd)
						checkSum := CompactMd5(td)
						r.checkSumMap[td.Id] = checkSum
					}
				default:
					r.logger.Info("gen checksum",
						zap.Int("len", len(r.checkSumMap)),
						zap.Duration("cost", time.Since(btime)),
					)
					goto FINAL
				}
			}

		FINAL:
			btime = time.Now()
			r.logger.Info("example checksum",
				zap.String("c074d0a90cd607b = C0BC243E017EF22CE16E1CA728EB98F5 ", r.checkSumMap["c074d0a90cd607b"]),
			)
			b, _ := json.Marshal(r.checkSumMap)
			content := "result=" + string(b)
			_, body, err := goreq.New().Post(fmt.Sprintf("http://127.0.0.1:%s/api/finished", r.DataPort)).SendMapString(content).End()
			r.logger.Info("report checksum",
				zap.Int("len", len(r.checkSumMap)),
				zap.Duration("cost", time.Since(btime)),
				zap.String("body", body),
				zap.Errors("err", err),
			)
			r.logger.Info("shutdown", zap.String("port", r.HttpPort))
			return
		}
	}
}

func CompactMd5(td *common.TraceData) string {
	//sb := strings.Builder{}
	h := md5.New()
	for _, span := range td.Sd {
		//sb.WriteString(span.Tags)
		//sb.WriteString("\n")
		h.Write([]byte(span.Tags))
		h.Write([]byte("\n"))
		//if td.Id == "c074d0a90cd607b" {
		//	fmt.Println(span.Tags)
		//}
	}
	//h.Write([]byte(sb.String()))
	//if td.Id == "c074d0a90cd607b" {
	//	fmt.Println(sb.String())
	//	fmt.Println(strings.ToUpper(hex.EncodeToString(h.Sum(nil))))
	//}
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}
