package receiver

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

var (
	rr Receiver
)

func init() {
	rand.Seed(time.Now().Unix())
	rr = Receiver{
		HttpPort:             "8000",
		DataPort:             "8081",
		CompactorPort:        "8002",
		CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
	}
	go rr.Run()
}

// go test -bench=".*" -cpuprofile=cpu.profile
// go tool pprof receiver.test cpu.profile
// go tool pprof --web receiver.test  cpu.profile
func Benchmark_ConsumeTraceData(b *testing.B) {
	// total = 11375660
	// batch = 11375660/500 = 2.2w
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := 25 //rand.Intn(25)
		nums := 20  //rand.Intn(25)
		spans := make([]*common.SpanData, nums*batch)
		for i := 0; i < batch; i++ {
			id := RandStringBytesMaskImprSrc(16)
			startTime := fmt.Sprintf("%d", time.Now().UnixNano())
			for j := 0; j < nums; j++ {
				spans[i*nums+j] = &common.SpanData{
					TraceId:   id,
					StartTime: startTime,
					Tags:      "1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET",
					Wrong:     false,
				}
			}
		}
		for i := range spans {
			j := rand.Intn(i + 1)
			spans[i], spans[j] = spans[j], spans[i]
		}
		b.StartTimer()
		rr.ConsumeTraceData(spans)
	}
}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxMask = 1<<6 - 1 // All 1-bits, as many as 6
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for 10 characters!
	for i, cache, remain := n-1, src.Int63(), 10; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), 10
		}
		b[i] = letterBytes[int(cache&letterIdxMask)%len(letterBytes)]
		i--
		cache >>= 6
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}
