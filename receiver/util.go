package receiver

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
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

func (r *Receiver) SendWrongRequest(id string, td *common.TData, reqUrl string, over string) {

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

	req.SetRequestURI(reqUrl + fmt.Sprintf("?over=%s", over))
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		fmt.Printf("set wrong fail id[%v] reqUrl[%v], err[%v] \n", id, reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("set wrong fail[%v] reqUrl[%v] code[%v]\n", id, reqUrl, resp.StatusCode())
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
