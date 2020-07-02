package compactor

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"sync"
	"time"
)

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

func WarmUp(port string, wg sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	notifyUrl := fmt.Sprintf("http://127.0.0.1:%s/warmup", port)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(notifyUrl)
	req.Header.SetMethod("GET")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return
	}
}

func NotifyAnotherWrong(reqUrl string, wg *sync.WaitGroup) {
	defer wg.Done()
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("GET")
	req.Header.SetContentType("application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		fmt.Printf("notify wrong fail reqUrl[%v] err[%v] \n", reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("notify wrong fail reqUrl[%v] code[%v]\n", reqUrl, resp.StatusCode())
		return
	}
}

func ReportCheckSumString(checkSumMap string, reqUrl string) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("POST")
	req.SetBodyString(checkSumMap)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := c.Do(req, resp); err != nil {
		fmt.Printf("report checkSum fail reqUrl[%v] err[%v] \n", reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("report checkSum fail reqUrl[%v] code[%v]\n", reqUrl, resp.StatusCode())
		return
	}
}
