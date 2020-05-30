package common

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// traceId：全局唯一的Id，用作整个链路的唯一标识与组装
//startTime：调用的开始时间
//spanId: 调用链中某条数据(span)的id
//parentSpanId: 调用链中某条数据(span)的父亲id，头节点的span的parantSpanId为0
//duration：调用耗时
//serviceName：调用的服务名
//spanName：调用的埋点名
//host：机器标识，比如ip，机器名
//tags: 链路信息中tag信息，存在多个tag的key和value信息。格式为key1=val1&key2=val2&key3=val3 比如 http.status_code=200&error=1
type SpanData struct {
	TraceId   string `json:"-"`
	StartTime int64  `json:"s"`
	Tags      string `json:"t"`
	Wrong     bool   `json:"-"`
}

var (
	FCode    = []byte("http.status_code=")
	FCode200 = []byte("http.status_code=200")
	Ferr1    = []byte("error=1")
	S1       = []byte("|")
)

func ParseSpanData(line []byte) *SpanData {
	spanData := &SpanData{}

	lineStr := string(line)
	//fmt.Println(lineStr)
	words := strings.Split(lineStr, "|")
	if len(words) < 3 {
		return nil
	}
	spanData.TraceId = words[0]
	st, err := strconv.ParseInt(words[1], 10, 64)
	if err != nil {
		fmt.Printf("timestamp to int64 fail %v", words[1])
		return nil
	}
	spanData.StartTime = st
	//firstIdx := bytes.Index(line, S1)
	//spanData.TraceId = line[:firstIdx]
	//secondIdx := bytes.Index(line[firstIdx:],S1)
	//spanData.StartTime = 0
	spanData.Tags = lineStr
	if (bytes.Contains(line, FCode) && !bytes.Contains(line, FCode200)) || bytes.Contains(line, Ferr1) {
		spanData.Wrong = true
	}
	return spanData
}

type TraceData struct {
	Sd     Spans
	Id     string
	Source string `json:"s"`
	sync.Mutex
}

type Spans []*SpanData

func (s Spans) Len() int           { return len(s) }
func (s Spans) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Spans) Less(i, j int) bool { return s[i].StartTime < s[j].StartTime }

func (b *TraceData) Add(newTD []*SpanData) {
	b.Lock()
	b.Sd = append(b.Sd, newTD...)
	b.Unlock()
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func GzipEncode(in []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err := writer.Write(in)
	if err != nil {
		writer.Close()
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

//func GzipTd(td *TraceData) ([]byte, error) {
//	var buffer bytes.Buffer
//	//writer := gzip.NewWriter(&buffer)
//	writer, _ := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
//	for _, span := range td.Sd {
//		_, err := writer.Write(span.Tags)
//		if err != nil {
//			writer.Close()
//			return nil, err
//		}
//	}
//	err := writer.Close()
//	if err != nil {
//		return nil, err
//	}
//
//	return buffer.Bytes(), nil
//}
