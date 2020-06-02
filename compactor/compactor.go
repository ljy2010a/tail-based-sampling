package compactor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Compactor struct {
	HttpPort  string // 8003
	DataPort  string // 8081
	ReportUrl string
	logger    *zap.Logger

	finishChan  chan interface{}
	checkSumMap map[string]string
	closeTimes  int64
	//resultChan  chan string
	idToTrace   sync.Map
	startTime   time.Time
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
				r.ReportUrl = fmt.Sprintf("http://127.0.0.1:%s/api/finished", r.DataPort)
				r.startTime = time.Now()
				return
			}
		}
	}()

	//r.resultChan = make(chan string, 10000)
	r.finishChan = make(chan interface{})
	r.checkSumMap = make(map[string]string)
	go r.finish()

	router := gin.New()
	//router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.POST("/sw", r.SetWrongHandler)
	router.GET("/fn", r.NotifyFinishHandler)
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
	r.ReportUrl = fmt.Sprintf("http://127.0.0.1:%s/api/finished", r.DataPort)
	r.startTime = time.Now()
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Compactor) SetWrongHandler(c *gin.Context) {
	over := c.DefaultQuery("over", "0")
	td := &common.TraceData{}
	//err := c.BindJSON(&td)
	//if err != nil {
	//	c.AbortWithStatus(http.StatusBadRequest)
	//	return
	//}

	body, err := c.GetRawData()
	if err != nil {
		r.logger.Info("td get body fail", zap.Error(err))
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	c.Request.Body.Close()
	err = td.Unmarshal(body)
	if err != nil {
		r.logger.Info("td Unmarshal fail", zap.Error(err))
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	tdi, exist := r.idToTrace.LoadOrStore(td.Id, td)
	if !exist {
		//r.resultChan <- td.Id
		// notify another
		anotherPort := "8000"
		if td.Source == "8000" {
			anotherPort = "8001"
		}
		reportUrl := fmt.Sprintf("http://127.0.0.1:%s/qw?id=%s&over=%s", anotherPort, td.Id, over)
		NotifyAnotherWrong(reportUrl)
	} else {
		otd := tdi.(*common.TraceData)

		//if td.GetStatusL() == common.TraceStatusInit {
		//	r.logger.Info("td already exist ", zap.String("id", otd.Id))
		//	c.AbortWithStatus(http.StatusOK)
		//	return
		//}
		if otd.Md5 != "" {
			r.logger.Info("md5 already exist ", zap.String("id", otd.Id))
			c.AbortWithStatus(http.StatusOK)
			return
		}
		if otd.Source == td.Source {
			r.logger.Info("source already exist ", zap.String("id", otd.Id))
			c.AbortWithStatus(http.StatusOK)
			return
		}
		otd.Add(td.Sd)
		otd.Md5 = CompactMd5(otd)

		//times := atomic.AddInt64(&otd.Status, 1)
		//if times == 1 {
		//	otd.Add(td.Sd)
		//} else if times == 2 {
		//	otd.Add(td.Sd)
		//	otd.Md5 = CompactMd5(otd)
		//} else {
		//	r.logger.Info("request more 2 ",
		//		zap.String("id", otd.Id),
		//	)
		//}
	}
	c.AbortWithStatus(http.StatusOK)
	return
}

func (r *Compactor) NotifyFinishHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "")
	r.logger.Info("got notify fin ", zap.String("port", port))
	times := atomic.AddInt64(&r.closeTimes, 1)
	if times == 2 {
		close(r.finishChan)
	}
	return
}

func (r *Compactor) finish() {
	<-r.finishChan

	btime := time.Now()
	sb := strings.Builder{}
	sb.WriteString("result={")
	start := true
	i := 0
	calTimes := 0
	r.idToTrace.Range(func(key, value interface{}) bool {
		i++
		td := value.(*common.TraceData)
		if td.Md5 == "" {
			calTimes++
			td.Md5 = CompactMd5(td)
		}
		if !start {
			sb.WriteString(",\"")
		} else {
			sb.WriteString("\"")
			start = false
		}
		sb.WriteString(td.Id)
		sb.WriteString("\":\"")
		sb.WriteString(td.Md5)
		sb.WriteString("\"")
		//if td.Id == "c074d0a90cd607b" {
		//	r.logger.Info("example checksum",
		//		zap.String("c074d0a90cd607b = C0BC243E017EF22CE16E1CA728EB98F5 ", td.Md5),
		//		zap.Int("splen", len(td.Sd)),
		//	)
		//}
		return true
	})
	r.logger.Info("gen checksum",
		zap.Int("len", i),
		zap.Int("calTimes", calTimes),
		zap.Duration("cost", time.Since(btime)),
	)

	sb.WriteString("}")
	btime = time.Now()
	//r.logger.Info(sb.String())
	//ReportCheckSum(r.checkSumMap, r.ReportUrl)
	ReportCheckSumString(sb.String(), r.ReportUrl)
	r.logger.Info("report checksum",
		zap.Int("len", i),
		zap.Duration("cost", time.Since(btime)),
		zap.Duration("total cost", time.Since(r.startTime)),
	)
	return
}

func CompactMd5(td *common.TraceData) string {
	sort.Sort(common.Spans(td.Sd))
	h := md5.New()
	for _, span := range td.Sd {
		h.Write(span.Tags)
		h.Write([]byte("\n"))
		//if td.Id == "c074d0a90cd607b" {
		//	fmt.Println(span.Tags)
		//}
	}
	//if td.Id == "c074d0a90cd607b" {
	//	fmt.Println(strings.ToUpper(hex.EncodeToString(h.Sum(nil))))
	//}
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}
