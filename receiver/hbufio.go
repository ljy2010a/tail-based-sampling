package receiver

import (
	"bytes"
	"go.uber.org/zap"
	"io"
	"runtime"
	"time"
)

func (b *HttpBlock) fill() {
	//n, err := b.rd.Read(b.buf[b.w:])
	n, err := io.ReadAtLeast(b.rd, b.buf[b.w:], b.readBufSize)
	b.w += n
	if err != nil {
		b.err = err
		//b.w = b.BufEnd
		//fmt.Printf("read err=%v n=%d \n", err, n)
		return
	}
	if n > 0 {
		//fmt.Printf("read n=%d \n", n)
		return
	}
	//b.err = io.ErrNoProgress
}

func (b *HttpBlock) asyncfill() {
	btime := time.Now()
	defer func() {
		logger.Info("async done",
			zap.Int("seq", b.Seq),
			zap.Int("b.w", b.w),
			zap.Int("b.BufEnd", b.BufEnd),
			zap.Bool("all from async", b.w == b.BufEnd),
			zap.Duration("cost", time.Since(btime)),
		)
		b.wg.Done()
	}()
	for {
		select {
		case <-b.readSignal:
			//logger.Info("async read exit", zap.Int("seq", seq))
			return
		default:
			//logger.Info("async read")
			var n int
			var err error
			if b.w+b.readBufSize <= b.BufEnd {
				n, err = io.ReadAtLeast(b.rd, b.buf[b.w:], b.readBufSize)
			} else {
				n, err = b.rd.Read(b.buf[b.w:])
			}
			b.w += n
			if err != nil {
				b.err = err
				//logger.Info("rush done",
				//	zap.Error(err),
				//	zap.Int("seq", b.Seq),
				//	zap.Int("b.w", b.w),
				//	zap.Duration("cost", time.Since(btime)),
				//)
				return
			}
			if n > 0 {
				//logger.Info("read ", zap.Int("b.w", b.w))
				runtime.Gosched()
				continue
			}
		}
	}
}

func (b *HttpBlock) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *HttpBlock) ReadSlicePos() (start, llen int, err error) {
	for {
		//if b.r+350 <= b.w {
		//	if i := bytes.IndexByte(b.buf[b.r+130:b.r+350], '\n'); i >= 0 {
		//		start = b.r
		//		llen = i + 1 + 130
		//		b.r += i + 1 + 130
		//		break
		//	}
		//} else {
		if i := bytes.IndexByte(b.buf[b.r:b.w], '\n'); i >= 0 {
			start = b.r
			llen = i + 1
			b.r += i + 1
			break
		}
		//}

		if b.err != nil {
			err = b.readErr()
			break
		}
		b.fill()
	}
	return
}
