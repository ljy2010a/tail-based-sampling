package receiver

import (
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func (r *Receiver) RunHttpServer() {
	frouter := fasthttprouter.New()
	frouter.GET("/ready", func(ctx *fasthttp.RequestCtx) {
		btime := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < 500; i++ {
			go r.warmUp(wg)
		}
		wg.Wait()
		logger.Info("ReadyHandler done",
			zap.String("port", r.HttpPort),
			zap.Duration("cost", time.Since(btime)))
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/warmup", func(ctx *fasthttp.RequestCtx) {
		time.Sleep(1 * time.Millisecond)
		ctx.SetStatusCode(http.StatusOK)
	})
	frouter.GET("/setParameter", r.SetParamHandler)
	frouter.GET("/qw", r.QueryWrongHandler)
	if err := fasthttp.ListenAndServe(fmt.Sprintf(":%s", r.HttpPort), frouter.Handler); err != nil {
		logger.Info("r.HttpPort fail", zap.Error(err))
	}
}

func (r *Receiver) SetParamHandler(ctx *fasthttp.RequestCtx) {
	port := string(ctx.QueryArgs().Peek("port"))

	if r.DataPort != "" {
		logger.Info("SetParamHandler already has",
			zap.String("port", r.HttpPort),
			zap.String("set", r.DataPort),
		)
		ctx.SetStatusCode(http.StatusOK)
		return
	}

	r.DataPort = port
	logger.Info("SetParamHandler",
		zap.String("port", r.HttpPort),
		zap.String("set", r.DataPort),
	)
	go r.notifyDataPort()

	if r.HttpPort == "8000" {
		dataUrl := fmt.Sprintf("http://127.0.0.1:%s/trace1.data", r.DataPort)
		go r.Read(dataUrl)
	}

	if r.HttpPort == "8001" {
		dataUrl2 := fmt.Sprintf("http://127.0.0.1:%s/trace2.data", r.DataPort)
		go r.Read(dataUrl2)
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

func (r *Receiver) notifyDataPort() {
	notifyUrl := fmt.Sprintf("http://127.0.0.1:%s/setParameter?port=%s&hport=%s", r.CompactorPort, r.DataPort, r.HttpPort)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(notifyUrl)
	req.Header.SetMethod("GET")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		logger.Info("send setParameter to compactor",
			zap.Error(err),
		)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		logger.Info("send setParameter to compactor",
			zap.Int("code", resp.StatusCode()),
		)
		return
	}
}

func (r *Receiver) QueryWrongHandler(ctx *fasthttp.RequestCtx) {
	id := string(ctx.QueryArgs().Peek("id"))
	over := string(ctx.QueryArgs().Peek("over"))
	//r.wrongIdMap.Store(id, true)
	//if id == "c074d0a90cd607b" {
	//	logger.Info("got wrong example notify",
	//		zap.String("id", id),
	//	)
	//}
	var td *TData
	if nowPos := atomic.AddInt64(&r.tdCachePos, 1); nowPos < tdCacheLimit {
		td = tdCache[nowPos]
	} else {
		td = NewTData()
	}
	td.Wrong = true
	td.Status = common.TraceStatusWrongSet
	ltd, lexist := r.idToTrace.LoadOrStore(id, td)
	if lexist {
		ltd.Wrong = true
		if ltd.Status == common.TraceStatusDone {
			ltd.Status = common.TraceStatusSended
			r.overWg.Add(1)
			if over == "1" {
				r.SendWrongRequest(id, ltd, "")
			} else {
				go r.SendWrongRequest(id, ltd, "")
			}
		}
	}
	ctx.SetStatusCode(http.StatusOK)
	return
}

var (
	c = &fasthttp.Client{
		MaxConnsPerHost:     10000,
		MaxIdleConnDuration: 5 * time.Second,
		ReadTimeout:         500 * time.Millisecond,
		WriteTimeout:        500 * time.Millisecond,
	}
	//reqPool *ants.Pool
)

//func init() {
//	reqPool, _ = ants.NewPool(1000, ants.WithPreAlloc(true))
//}

func (r *Receiver) SendWrongRequest(id string, td *TData, over string) {
	defer r.overWg.Done()

	//rtd := &common.TraceData{
	//	Id:     id,
	//	Source: r.HttpPort,
	//	Sb:     make([][]byte, td.n),
	//}
	//for i := uint8(0); i < td.n; i++ {
	//	val := td.Sbi[i]
	//	start := val >> 16
	//	llen := val & 0xffff
	//	rtd.Sb[i] = linesBuf[start : start+llen]
	//}

	rtd := &common.TraceData{
		Id:     id,
		Source: r.HttpPort,
		Sb:     make([][]byte, len(td.Sbi)),
	}
	for i, val := range td.Sbi {
		start := val >> 16
		llen := val & 0xffff
		rtd.Sb[i] = linesBuf[start : start+llen]
	}

	//var rtd *common.TraceData
	//if nowPos := atomic.AddInt64(&r.tdSendSlicePos, 1); nowPos < r.tdSendSliceLimit {
	//	rtd = r.tdSendSlice[nowPos]
	//} else {
	//	rtd = &common.TraceData{
	//		Source: r.HttpPort,
	//		Sb:     make([][]byte, len(td.Sbi)),
	//	}
	//}
	//
	//rtd.Id = id
	//for _, val := range td.Sbi {
	//	start := val >> 16
	//	llen := val & 0xffff
	//	rtd.Sb = append(rtd.Sb, r.consumer.linesBuf[start:start+llen])
	//}

	b, _ := rtd.Marshal()
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(r.CompactorSetWrongUrl + fmt.Sprintf("?over=%s", over))
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	//return
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
		logger.Info("send notify fin",
			zap.Error(err),
		)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		logger.Info("send notify fin",
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
		logger.Info("send warm up",
			zap.Error(err),
		)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		logger.Info("send notify fin",
			zap.Int("code", resp.StatusCode()),
		)
		return
	}
}
