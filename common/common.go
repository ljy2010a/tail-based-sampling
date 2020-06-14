package common

const (
	TraceStatusReady    = 0
	TraceStatusWrongSet = 1
	TraceStatusSkip     = 2
	TraceStatusSended   = 3
	TraceStatusDone     = 4
)

func (m *TraceData) AddSpan(newSpans [][]byte) {
	m.Lock()
	m.Sb = append(m.Sb, newSpans...)
	m.Unlock()
}

type SpanData struct {
	StartTime string
	Tags      []byte
}

// traceId：全局唯一的Id，用作整个链路的唯一标识与组装
//startTime：调用的开始时间
//spanId: 调用链中某条数据(span)的id
//parentSpanId: 调用链中某条数据(span)的父亲id，头节点的span的parantSpanId为0
//duration：调用耗时
//serviceName：调用的服务名
//spanName：调用的埋点名
//host：机器标识，比如ip，机器名
//tags: 链路信息中tag信息，存在多个tag的key和value信息。格式为key1=val1&key2=val2&key3=val3 比如 http.status_code=200&error=1
type Spans []*SpanData

func (s Spans) Len() int           { return len(s) }
func (s Spans) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Spans) Less(i, j int) bool { return s[i].StartTime < s[j].StartTime }
