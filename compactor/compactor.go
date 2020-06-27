package compactor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/http"
	"os"
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
	doneWg      sync.WaitGroup
	idToTrace   sync.Map
	startTime   time.Time
}

func (r *Compactor) Run() {
	//defer func() {
	//	err := recover()
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//}()
	r.logger, _ = zap.NewProduction()
	defer r.logger.Sync()
	go func() {
		i := 0
		for {
			if i > 2 {
				r.logger.Info("too long to stop")
				close(r.finishChan)
				r.finish()
			}
			if i > 3 {
				os.Exit(-1)
			}

			i++
			r.logger.Info("sleep",
				zap.String("port", r.HttpPort),
				zap.Int("i", i),
			)
			time.Sleep(1 * time.Minute)
		}
	}()

	r.finishChan = make(chan interface{})
	r.checkSumMap = make(map[string]string)
	go r.finish()

	frouter := fasthttprouter.New()
	frouter.GET("/ready", func(ctx *fasthttp.RequestCtx) {
		btime := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < 500; i++ {
			go WarmUp("8000", wg)
		}
		for i := 0; i < 500; i++ {
			go WarmUp("8001", wg)
		}
		wg.Wait()
		r.logger.Info("ReadyHandler done",
			zap.String("port", r.HttpPort),
			zap.Duration("cost", time.Since(btime)))
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/warmup", func(ctx *fasthttp.RequestCtx) {
		time.Sleep(1 * time.Millisecond)
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/setParameter", r.SetParamHandler)
	frouter.POST("/sw", r.SetWrongHandler)
	frouter.GET("/fn", r.NotifyFinishHandler)
	if err := fasthttp.ListenAndServe(fmt.Sprintf(":%s", r.HttpPort), frouter.Handler); err != nil {
		r.logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Compactor) SetParamHandler(ctx *fasthttp.RequestCtx) {
	port := string(ctx.QueryArgs().Peek("port"))
	r.DataPort = port
	r.ReportUrl = fmt.Sprintf("http://127.0.0.1:%s/api/finished", r.DataPort)
	r.startTime = time.Now()
	r.logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Compactor) SetWrongHandler(ctx *fasthttp.RequestCtx) {
	over := string(ctx.QueryArgs().Peek("over"))
	td := &common.TraceData{}
	body := ctx.PostBody()
	err := td.Unmarshal(body)
	if err != nil {
		r.logger.Info("td Unmarshal fail", zap.Error(err))
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}

	tdi, exist := r.idToTrace.LoadOrStore(td.Id, td)
	if !exist {
		// notify another
		anotherPort := "8000"
		if td.Source == "8000" {
			anotherPort = "8001"
		}
		reportUrl := fmt.Sprintf("http://127.0.0.1:%s/qw?id=%s&over=%s", anotherPort, td.Id, over)
		r.doneWg.Add(1)
		//if over == "1" {
		//	NotifyAnotherWrong(reportUrl, &r.doneWg)
		//} else {
		go NotifyAnotherWrong(reportUrl, &r.doneWg)
		//}
	} else {
		otd := tdi.(*common.TraceData)
		if otd.Md5 != "" {
			r.logger.Info("md5 already exist ", zap.String("id", otd.Id))
			ctx.SetStatusCode(http.StatusOK)
			return
		}
		if otd.Source == td.Source {
			r.logger.Info("source already exist ", zap.String("id", otd.Id))
			ctx.SetStatusCode(http.StatusOK)
			return
		}
		//otd.Sb = append(otd.Sb, td.Sb...)
		//if over == "1" {
		//	otd.Md5 = CompactMd5(otd)
		//} else {
		r.doneWg.Add(1)
		go func() {
			otd.AddSpan(td.Sb)
			otd.Md5 = CompactMd5(otd, &r.doneWg)
		}()
		//}
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Compactor) NotifyFinishHandler(ctx *fasthttp.RequestCtx) {
	port := string(ctx.QueryArgs().Peek("port"))
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
	r.doneWg.Wait()
	donwWaitTime := time.Since(btime)
	btime = time.Now()
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
			td.Md5 = CompactMd5(td, nil)
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
		//sb.WriteString("-")
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
		zap.Duration("wait", donwWaitTime),
		zap.Duration("cost", time.Since(btime)),
	)

	sb.WriteString("}")
	btime = time.Now()
	//r.logger.Info(sb.String())
	ReportCheckSumString(sb.String(), r.ReportUrl)
	r.logger.Info("report checksum",
		zap.Int("len", i),
		zap.Duration("cost", time.Since(btime)),
		zap.Duration("total cost", time.Since(r.startTime)),
	)
	return
}

func CompactMd5(td *common.TraceData, wg *sync.WaitGroup) string {
	if wg != nil {
		defer wg.Done()
	}
	//spans := make(common.Spans, len(td.Sb))
	//for i, sb := range td.Sb {
	//	spans[i] = ParseSpanData(sb)
	//}
	spans := common.Spans{}
	for _, sb := range td.Sb {
		spans = append(spans, ParseSpanData(sb))
	}
	sort.Sort(spans)
	h := md5.New()
	for _, span := range spans {
		h.Write(span.Tags)
		//h.Write([]byte("\n"))
		//if td.Id == "c074d0a90cd607b" {
		//	fmt.Println(span.Tags)
		//}
	}
	//if td.Id == "c074d0a90cd607b" {
	//	fmt.Println(strings.ToUpper(hex.EncodeToString(h.Sum(nil))))
	//}
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}

func ParseSpanData(line []byte) *common.SpanData {
	spanData := &common.SpanData{}
	l := common.BytesToString(line)
	firstIdx := strings.IndexByte(l, '|')
	secondIdx := strings.IndexByte(l[firstIdx+1:], '|')
	spanData.StartTime = l[firstIdx+1 : firstIdx+1+secondIdx]
	spanData.Tags = line
	return spanData
}
