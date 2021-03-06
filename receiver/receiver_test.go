package receiver

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var (
	rr Receiver
)

func init() {
	rand.Seed(time.Now().Unix())
	//rr = Receiver{
	//	HttpPort:             "8000",
	//	DataPort:             "8081",
	//	CompactorPort:        "8002",
	//	CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
	//}
	//go rr.Run()
	//time.Sleep(1 * time.Second)
}

// go test -bench=. -benchmem -memprofile memprofile.out -cpuprofile cpuprofile.out -benchtime=30000x
// go tool pprof -http=":8088" receiver.test cpuprofile.out
// go tool pprof -http=":8088" receiver.test memprofile.out
//func Benchmark_ConsumeTraceData(b *testing.B) {
//	// total = 11375660
//	// batch = 11375660/500 = 2.2w
//	batch := 250 //rand.Intn(25)
//	nums := 20   //rand.Intn(25)
//
//	//spans := make([]*common.SpanData, nums*batch)
//	spans := make([][]byte, nums*batch)
//	s := []byte("|")
//	startTime := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
//	tag := []byte("|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET")
//	for i := 0; i < b.N; i++ {
//		b.StopTimer()
//		for i := 0; i < batch; i++ {
//			id := RandStringBytesMaskImprSrc(16)
//			for j := 0; j < nums; j++ {
//				//bb := bytebufferpool.Get()
//				//bb.Write(id)
//				//b := rr.p300.Get().([]byte)
//				b := make([]byte, 300)
//				idx := 0
//				copy(b[idx:], id)
//				idx += len(id)
//				copy(b[idx:], s)
//				idx += len(s)
//				copy(b[idx:], startTime)
//				idx += len(startTime)
//				copy(b[idx:], tag)
//				//bb := bytes.NewBuffer(b)
//				//bb.Write(id)
//				//bb.WriteString("|")
//				//bb.Write(startTime)
//				//bb.Write(tag)
//
//				//span := &common.SpanData{
//				//	TraceId:   id,
//				//	StartTime: startTime,
//				//	Tags:      []byte("1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET"),
//				//	Wrong:     false,
//				//}
//				//spani := rr.spanPool.Get()
//				//span := spani.(*common.SpanData)
//				//span.TraceId = id
//				//span.StartTime = startTime
//				//span.Tags = "1d37a8b17db8568b|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET"
//				//span.Wrong = false
//				//fmt.Printf("%s\n",bb.Bytes())
//				//spans[i*nums+j] = bb.Bytes()
//				//fmt.Printf("%s\n", b)
//				spans[i*nums+j] = b
//			}
//		}
//		for i := range spans {
//			j := rand.Intn(i + 1)
//			spans[i], spans[j] = spans[j], spans[i]
//		}
//		b.StartTimer()
//		rr.ConsumeByte(spans)
//	}
//}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxMask = 1<<6 - 1 // All 1-bits, as many as 6
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) []byte {
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
	return b
}

var (
	s  = []byte("11be9afcc016ee02|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.status_code=200&http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET")
	s1 = []byte("11be9afcc016ee02|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&http.status_code=200&span.kind=server&http.method=GET")
	s2 = []byte("11be9afcc016ee02|1589285985482007|3d1e7e1147c1895d|1d37a8b17db8568b|1259|InventoryCenter|/api/traces|192.168.0.2|http.url=http://tracing.console.aliyun.com/getOrder&component=java-web-servlet&span.kind=server&http.method=GET&http.status_code=200")

	e1 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|error=1&db.instance=db&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082")
	e2 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|db.instance=db&error=1&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082")
	e3 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|db.instance=db&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082&error=1\n")

	he1 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|http.status_code=201&db.instance=db&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082")
	he2 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|db.instance=db&http.status_code=201&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082")
	he3 = []byte("11be9afcc016ee02|1589285988534901|7068da59f571fbb9|710dfde81d01f3c2|928|PromotionCenter|DoCheckApplicationExist|192.168.91.202|db.instance=db&component=java-jdbc&db.type=h2&span.kind=client&__sql_id=1x7lx2l&peer.address=localhost:8082&http.status_code=201")

	hh = []byte("80f2481e92da39a|1592840906715039|1284072d6e59970f|3d89709e6af8cdcf|1376|Frontend|checkAndRefresh|192.168.50.207|&component=java-web-servlet&span.kind=server&http.url=http://tracing.console.aliyun.com/updateInventory&bizErr=1-failUpdateInventory&http.method=GET&&error=1\n")
)

func Test_aa(t *testing.T) {
	//fmt.Println([]byte("http.status_code="))
	//fmt.Println(GetTraceIdWrongByString(s))
	//fmt.Println(GetTraceIdWrongByString(s2))

	//fmt.Println(GetTraceIdWrongByByte(hh))

	//fmt.Println(GetTraceIdWrongByByte(s))
	//fmt.Println(GetTraceIdWrongByByte(s1))
	//fmt.Println(GetTraceIdWrongByByte(s2))
	//
	//fmt.Println(GetTraceIdWrongByByte(e1))
	//fmt.Println(GetTraceIdWrongByByte(e2))
	//fmt.Println(GetTraceIdWrongByByte(e3))
	//
	//fmt.Println(GetTraceIdWrongByByte(he1))
	//fmt.Println(GetTraceIdWrongByByte(he2))
	//fmt.Println(GetTraceIdWrongByByte(he3))

	//fmt.Println(IfSpanWrongString(s))
	fmt.Println(IfSpanWrongString(s1))
	//fmt.Println(IfSpanWrongString(s2))

	//fmt.Println(IfSpanWrongString(e1))
	//fmt.Println(IfSpanWrongString(e2))
	//fmt.Println(IfSpanWrongString(e3))
	//
	//fmt.Println(IfSpanWrongString(he1))
	//fmt.Println(IfSpanWrongString(he2))
	//fmt.Println(IfSpanWrongString(he3))
}

//func BenchmarkGetTraceIdWrongByString(b *testing.B) {
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		GetTraceIdWrongByString(s)
//	}
//}

func BenchmarkGetTraceIdWrongByByte(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetTraceIdWrongByByte(s1)
	}
}

func BenchmarkGroupString(b *testing.B) {
	r := s1
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		GetTraceIdByString(r)
		IfSpanWrongString(r)
	}
}

//func BenchmarkGroupByte(b *testing.B) {
//	r := s1
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		GetTraceIdByByte(r)
//		//IfSpanWrongByte(r)
//	}
//}

//
//func BenchmarkREGroupByte(b *testing.B) {
//	r := s1
//	b.ResetTimer()
//	b.ReportAllocs()
//	for n := 0; n < b.N; n++ {
//		GetTraceIdByString(r)
//		//IfSpanWrongString(r)
//	}
//}
