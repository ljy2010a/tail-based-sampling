package receiver

import (
	"bytes"
	"go.uber.org/zap"
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
	r.consumer.Read(bytes.NewReader(b))
	return
}

func (r *Receiver) ReadFile(fileName string) {
	fi, err := os.Open(fileName)
	if err != nil {
		r.logger.Error("file get err", zap.Error(err))
		return
	}
	defer fi.Close()
	r.consumer.Read(fi)
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
	r.consumer.Read(resp.Body)
	return
}
