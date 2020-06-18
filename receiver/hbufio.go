package receiver

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bufio implements buffered I/O. It wraps an io.HReader or io.Writer
// object, creating another object (HReader or Writer) that also implements
// the interface but provides buffering and some help for textual I/O.

import (
	"github.com/ljy2010a/tailf-based-sampling/common"
	"go.uber.org/zap"
	"io"
	"strings"
	"sync"
	"time"
)

type HReader struct {
	buf                        []byte
	rd                         io.Reader // reader provided by the client
	r, w, bufSize, readBufSize int       // buf read and write positions
	err                        error
}

func (b *HReader) fill() {
	n, err := b.rd.Read(b.buf[b.w:])
	//n, err := io.ReadAtLeast(b.rd, b.buf[b.w:], b.readBufSize)
	if n < 0 {
		panic(errNegativeRead)
	}
	b.w += n
	if err != nil {
		b.err = err
		//fmt.Printf("read err=%v n=%d \n", err, n)
		return
	}
	if n > 0 {
		//fmt.Printf("read n=%d \n", n)
		return
	}
	b.err = io.ErrNoProgress
}

func (b *HReader) asyncfill(signal chan interface{}, wg *sync.WaitGroup, seq int) {
	btime := time.Now()
	defer func() {
		//logger.Info("async done", zap.Int("seq", seq))
		wg.Done()
	}()
	//block := make([]byte, 16*1024*1024)
	for {
		select {
		case <-signal:
			//logger.Info("async read exit", zap.Int("seq", seq))
			return
		default:
			//logger.Info("async read")
			n, err := b.rd.Read(b.buf[b.w:])
			//n, err := b.rd.Read(block)
			if n < 0 {
				panic(errNegativeRead)
			}
			//copy(b.buf[b.w:], block[:n])
			b.w += n
			if err != nil {
				b.err = err
				logger.Info("rush done",
					zap.Error(err),
					zap.Int("seq", seq),
					//zap.Int("n", n),
					zap.Duration("cost", time.Since(btime)),
				)
				return
			}
			if n > 0 {
				//logger.Info("read ", zap.Int("n", n))
				continue
			}
			//b.err = io.ErrNoProgress
		}

	}
}

func (b *HReader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *HReader) ReadSlicePos() (start, llen int, err error) {
	for {
		//if b.r+128 > b.w {
		if i := strings.IndexByte(common.BytesToString(b.buf[b.r:b.w]), '\n'); i >= 0 {
			start = b.r
			llen = i + 1
			b.r += i + 1
			break
		}
		//} else {
		//	if i := strings.IndexByte(common.BytesToString(b.buf[b.r+128:b.w]), '\n'); i >= 0 {
		//		i += 128
		//		start = b.r
		//		llen = i + 1
		//		b.r += i + 1
		//		break
		//	}
		//}

		// Pending error?
		if b.err != nil {
			//b.r = b.w
			err = b.readErr()
			break
		}
		b.fill() // buffer is not full
	}
	return
}
