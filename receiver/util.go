package receiver

import (
	"encoding/json"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"github.com/valyala/fasthttp"
)

func SendWrongRequest(td *common.TraceData, reqUrl string) {

	b, _ := json.Marshal(td)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqUrl)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		fmt.Printf("set wrong fail id[%v] reqUrl[%v], err[%v] \n", td.Id, reqUrl, err)
		return
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		fmt.Printf("set wrong fail[%v] reqUrl[%v] code[%v]\n", td.Id, reqUrl, resp.StatusCode())
		return
	}

}
