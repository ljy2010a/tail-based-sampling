package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	"math/rand"
	"time"
)

func main() {
	h := md5.New()
	h.Write([]byte("MD5testing"))
	fmt.Println(hex.EncodeToString(h.Sum(nil)))

	rand.Seed(time.Now().Unix())
	var httpPort string
	flag.StringVar(&httpPort, "p", "", "port")
	flag.Parse()

	if httpPort == "8000" || httpPort == "8001" {
		receiver := receiver.Receiver{
			HttpPort:      httpPort,
			DataPort:      "8081",
			CompactorPort: "8002",
		}
		receiver.Run()
	} else if httpPort == "8002" {
		compactor := compactor.Compactor{
			HttpPort: httpPort,
			DataPort: "8081",
		}
		compactor.Run()
	}
	fmt.Sprintf("port not macth [%v] \n", httpPort)
}
