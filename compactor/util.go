package compactor

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"strings"
)

func NotifyAnotherWrong(reqUrl string) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("GET")
	req.Header.SetContentType("application/json")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		fmt.Printf("notify wrong fail reqUrl[%v] err[%v] \n", reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("notify wrong fail reqUrl[%v] code[%v]\n", reqUrl, resp.StatusCode())
		return
	}
}

func ReportCheckSum(checkSumMap map[string]string, reqUrl string) {
	b, _ := json.Marshal(checkSumMap)
	sb := strings.Builder{}
	sb.WriteString("result=")
	sb.Write(b)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("POST")
	req.SetBodyString(sb.String())

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		fmt.Printf("report checkSum fail reqUrl[%v] err[%v] \n", reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("report checkSum fail reqUrl[%v] code[%v]\n", reqUrl, resp.StatusCode())
		return
	}
}
