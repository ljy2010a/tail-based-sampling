package common

const (
	//TraceStatusInit   = 0
	TraceStatusSended = 1
	TraceStatusReady  = 2
	TraceStatusDone   = 3
)

func (m *TraceData) Clear() {
	m.Sb = m.Sb[:0]
	m.Id = ""
	m.Md5 = ""
	m.Status = TraceStatusReady
	m.Wrong = false
}
//
//func (m *TraceData) Add(newTd []*SpanData) {
//	m.Lock()
//	m.Sd = append(m.Sd, newTd...)
//	m.Unlock()
//}

func (m *TraceData) AddSpan(newTd [][]byte) {
	m.Lock()
	m.Sb = append(m.Sb, newTd...)
	m.Unlock()
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
