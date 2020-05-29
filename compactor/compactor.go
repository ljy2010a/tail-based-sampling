package compactor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/smallnest/goreq"
	"go.uber.org/zap"
	"net/http"
	"os"
	"sort"
	"strings"
)

type Processor struct {
}

type Compactor struct {
	HttpPort       string // 8003
	DataPort       string // 8081
	logger         *zap.Logger
	maxNumTraces   uint64
	numTracesOnMap uint64
	deleteChan     chan string
	finishChan     chan interface{}
	gzipLen        int
	checkSumMap    map[string]string
}

func (r *Compactor) Run() {
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()

	r.deleteChan = make(chan string, 100000)
	r.finishChan = make(chan interface{})
	r.checkSumMap = make(map[string]string)
	go r.finish()

	router := gin.Default()
	//router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.GET("/ready", r.ReadyHandler)
	router.GET("/setParameter", r.SetParamHandler)
	router.POST("/sw", r.SetWrongHandler)
	router.GET("/fn", r.FinishNotifyHandler)
	router.Run(fmt.Sprintf("0.0.0.0:%s", r.HttpPort))
}

func (r *Compactor) ReadyHandler(c *gin.Context) {
	c.JSON(http.StatusOK, "ok")
	return
}

func (r *Compactor) SetParamHandler(c *gin.Context) {
	port := c.DefaultQuery("port", "8081")
	r.DataPort = port
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
	// query another wrong
	if td.Sd[0].TraceId == "c074d0a90cd607b" {
		//b, _ := json.Marshal(td)
		r.logger.Info("example ",
			//zap.String("", string(b)),
			zap.Int("len ", td.Sd.Len()),
		)
	}
	sort.Sort(td.Sd)
	checkSum := CompactMd5(td.Sd)
	//r.checkSumMap[common.BytesToString(td.Sd[0].TraceId)] = checkSum
	r.checkSumMap[td.Sd[0].TraceId] = checkSum
	return
}

func (r *Compactor) FinishNotifyHandler(c *gin.Context) {
	c.DefaultQuery("port", "")
	close(r.finishChan)
	return
}

func (r *Compactor) finish() {
	// send result
	for {
		select {
		case <-r.finishChan:
			r.logger.Info("report checksum",
				zap.Int("checksum len", len(r.checkSumMap)),
			)
			r.logger.Info("example checksum",
				zap.String("c074d0a90cd607b, C0BC243E017EF22CE16E1CA728EB98F5 should ", r.checkSumMap["c074d0a90cd607b"]),
			)

			_, body, err := goreq.New().Post(fmt.Sprintf("http://127.0.0.1:%s/api/finished", r.DataPort)).SendStruct(r.checkSumMap).End()
			r.logger.Info("report checksum",
				zap.String("body", body),
				zap.Errors("err", err),
			)
			r.logger.Info("shutdown", zap.String("port", r.HttpPort))
			os.Exit(0)
			return
		}
	}
}

func CompactMd5(spans common.Spans) string {
	sb := strings.Builder{}
	for _, span := range spans {
		sb.WriteString(span.Tags)
		sb.WriteString("\n")
		//if spans[0].TraceId == "c074d0a90cd607b" {
		//	fmt.Println(span.Tags)
		//}
	}
	h := md5.New()
	h.Write([]byte(sb.String()))
	//if spans[0].TraceId == "c074d0a90cd607b" {
	//	fmt.Println(sb.String())
	//	fmt.Println(strings.ToUpper(hex.EncodeToString(h.Sum(nil))))
	//}
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}
