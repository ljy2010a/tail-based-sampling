package receiver

import (
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type HttpRing struct {
	buf           []byte
	blen          int
	contentLength int
	readPos       int
	httpBlock     []*HttpBlock
}

type HttpRange struct {
	Seq   int
	Start int
	End   int
}

func (h *HttpRing) GenRange(dataUrl string) {
	logger, _ = zap.NewProduction()
	logger.Info("dataUrl", zap.String("dataUrl", dataUrl))
	client := &http.Client{}
	req, err := http.NewRequest("HEAD", dataUrl, nil)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return
	}
	req.Header.Add("Range", "bytes=0-")
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("http get err", zap.Error(err))
		return
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		contentLength := resp.Header.Get("Content-Length")
		length64, _ := strconv.ParseInt(contentLength, 10, 0)
		h.contentLength = int(length64)
	default:
		h.contentLength = 0
	}

	logger.Info("http file info",
		zap.String("fileUrl", dataUrl),
		zap.Int("len", h.contentLength),
	)

	wg := sync.WaitGroup{}
	taskChan := make(chan *HttpRange, 10)
	for i := 0; i < 2; i++ {
		go func() {
			for hr := range taskChan {
				logger.Info("http start",
					zap.Int("t", hr.Seq),
					zap.Int("s", hr.Start),
					zap.Int("e", hr.End),
				)
				btime := time.Now()
				rd, err := httpGet(dataUrl, hr.Start, hr.End)
				if err != nil {
					logger.Error("http get err",
						zap.Duration("cost", time.Since(btime)),
						zap.Error(err),
					)
					return
				}
				blockLen := 128 * 1024 * 1024
				sPos := hr.Start
				for {
					readbyte, readLen := h.ApplyBuf(sPos, blockLen)
					llen, err := io.ReadAtLeast(rd, readbyte, readLen)
					if err != nil {
						logger.Info("http cost done",
							zap.Int("t", hr.Seq),
							zap.Int("s", hr.Start),
							zap.Int("e", hr.End),
							zap.Duration("cost", time.Since(btime)),
							zap.Error(err),
						)
						break
					}
					sPos += llen
					//h.httpBlock[hr.Seq].writePos = sPos
					//rpos := 0
					//for {
					//	n := bytes.IndexByte(readbyte[rpos:], '\n')
					//	if n == -1 {
					//		break
					//	}
					//	if n < 200 {
					//
					//	} else {
					//		GetTraceIdByString(readbyte[rpos : rpos+n+1])
					//		IfSpanWrongString(readbyte[rpos : rpos+n+1])
					//	}
					//	rpos += n + 1
					//}
					//rpos = 0
				}
				wg.Done()
			}
		}()
	}

	var parts = 4
	var pos int
	h.httpBlock = make([]*HttpBlock, parts)
	step := h.contentLength / parts
	for i := 0; i < parts; i++ {
		s := pos
		pos = pos + step
		e := pos
		if i == parts-1 {
			e = h.contentLength
		}
		h.httpBlock[i] = &HttpBlock{Seq: i, HttpStart: s, HttpEnd: e}
		wg.Add(1)
		taskChan <- &HttpRange{Seq: i, Start: s, End: e}
	}
	close(taskChan)
	wg.Wait()
}

func (h *HttpRing) ApplyBuf(apos, alen int) ([]byte, int) {
	if apos+alen < h.blen {
		return h.buf[apos : apos+alen], alen
	}

	if apos+alen > h.blen && apos >= h.blen {
		return h.buf[apos-h.blen : apos-h.blen+alen], alen
	}

	if apos+alen > h.blen && apos < h.blen {
		l := h.blen - apos
		return h.buf[apos:h.blen], l
	}

	return nil, -1
}