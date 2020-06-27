package receiver

import (
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type HttpBlock struct {
	Seq       int
	HttpStart int
	HttpEnd   int
	BufStart  int
	BufEnd    int

	rd                io.Reader
	readSignal        chan interface{}
	exitRead          bool
	buf               []byte
	r, w, readBufSize int
	err               error
	wg                sync.WaitGroup
}

func GenRange(dataUrl string, bufSize int) []*HttpBlock {
	btime := time.Now()
	logger.Info("dataUrl", zap.String("dataUrl", dataUrl))
	client := &http.Client{}
	req, err := http.NewRequest("HEAD", dataUrl, nil)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return nil
	}
	req.Header.Add("Range", "bytes=0-")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return nil
	}

	var length int
	switch resp.StatusCode {
	case http.StatusPartialContent:
		contentLength := resp.Header.Get("Content-Length")
		length64, _ := strconv.ParseInt(contentLength, 10, 0)
		length = int(length64)
	default:
		length = 0
	}

	logger.Info("http file info",
		zap.String("fileUrl", dataUrl),
		zap.Duration("head cost", time.Since(btime)),
		zap.Int("len", length),
	)

	stepSize := 512 * 1024 * 1024
	hbs := make([]*HttpBlock, 0, length/stepSize+1)
	bufStart := 0
	bufEnd := 0
	httpStart := 0
	httpEnd := 0
	seq := 0
	for {
		if httpEnd == length {
			break
		}

		tmpStepSize := stepSize
		if bufEnd == bufSize {
			bufStart = 20 * 1024 * 1024
			bufEnd = bufStart + tmpStepSize
		} else {
			if bufStart+tmpStepSize > bufSize {
				tmpStepSize = bufSize - bufStart
				bufEnd = bufSize
			} else {
				bufEnd = bufStart + tmpStepSize
			}
		}

		httpEnd = httpStart + tmpStepSize - 1
		if httpEnd >= length {
			httpEnd = length
		}

		rd, err := httpGet(dataUrl, httpStart, httpEnd)
		if err != nil {
			logger.Error("http get err",
				zap.Error(err),
			)
			return nil
		}
		logger.Info("hb",
			zap.Int("seq", seq),
			zap.Int("httpStart", httpStart),
			zap.Int("httpEnd", httpEnd),
			zap.Int("bufStart", bufStart),
			zap.Int("bufEnd", bufEnd),
		)
		hbs = append(hbs, &HttpBlock{
			Seq:        seq,
			HttpStart:  httpStart,
			HttpEnd:    httpEnd,
			BufStart:   bufStart,
			BufEnd:     bufEnd,
			rd:         rd,
			r:          bufStart,
			w:          bufStart,
			readSignal: make(chan interface{}),
		})
		bufStart = bufEnd
		httpStart = httpEnd + 1
		seq++
	}
	return hbs
}

func httpGet(url string, start, end int) (io.Reader, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
