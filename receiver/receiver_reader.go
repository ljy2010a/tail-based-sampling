package receiver

import (
	"go.uber.org/zap"
	"net/http"
)

func (r *Receiver) ReadHttp(fileUrl string) {
	r.logger.Info("dataUrl", zap.String("dataUrl", fileUrl))
	resp, err := http.Get(fileUrl)
	if err != nil {
		r.logger.Error("http get err", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	//r.consumer.Read(resp.Body)
	return
}
