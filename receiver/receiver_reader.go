package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

func (r *Receiver) ReadMem(fileName string) {
	btime := time.Now()
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		r.logger.Error("file get err", zap.Error(err))
		return
	}
	r.logger.Info("read big file done ",
		zap.Duration("cost", time.Since(btime)),
	)
	r.Read(bytes.NewReader(b))
	return
}

func (r *Receiver) ReadFile(fileName string) {
	fi, err := os.Open(fileName)
	if err != nil {
		r.logger.Error("file get err", zap.Error(err))
		return
	}
	defer fi.Close()
	r.Read(fi)
	return
}

func (r *Receiver) ReadHttp(fileUrl string) {
	r.logger.Info("dataUrl", zap.String("dataUrl", fileUrl))
	resp, err := http.Get(fileUrl)
	if err != nil {
		r.logger.Error("http get err", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	r.Read(resp.Body)
	return
}

func (r *Receiver) Read(rd io.Reader) {
	r.logger.Info("read read")
	defer func() {
		err := recover()
		if err != nil {
			r.logger.Error("", zap.String("err", fmt.Sprintf("%v", err)))
		}
	}()
	btime := time.Now()
	br := bufio.NewReaderSize(rd, 4096)
	size := 0
	total := 0
	wrong := 0
	groupNum := 500
	spanDatas := make([]*common.SpanData, groupNum)
	i := 0
	go func() {
		r.logger.Info("read stat",
			zap.Int("total", total),
			zap.Int("wrong", wrong),
		)
		time.Sleep(10 * time.Second)
	}()
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		size += len(line)
		total++
		spanData := common.ParseSpanData(line)
		if spanData == nil {
			//fmt.Printf("nil : %s\n", string(line))
			continue
		}
		if spanData.Wrong {
			//fmt.Printf("err : %s\n", string(line))
			wrong++
		}
		if i < groupNum {
			spanDatas[i] = spanData
		}
		if i == groupNum-1 {
			r.ConsumeTraceData(spanDatas)
			i = 0
			continue
		}
		i++
	}
	if i != 0 {
		r.ConsumeTraceData(spanDatas[:i])
	}
	r.logger.Info("read file done ",
		zap.Int("total", total),
		zap.Int("wrong", wrong),
		zap.Int("sourceSize", size),
		zap.Int("gzipSize", r.gzipLen),
		zap.Duration("cost", time.Since(btime)),
	)
	//times := atomic.AddInt64(&r.closeTimes, 1)
	//if times == 2 {
		close(r.finishChan)
	//}
}
