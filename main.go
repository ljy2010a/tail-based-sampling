package main

import (
	"flag"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	"math/rand"
	"runtime"
	"time"
)

func main() {
	fmt.Println(
		VERSION,
		time.Now().Format("2006-01-02 15:04:05"),
		time.Now().Unix(),
		runtime.NumCPU(),
	)

	rand.Seed(time.Now().Unix())
	var httpPort string
	flag.StringVar(&httpPort, "p", "", "port")
	flag.Parse()

	if httpPort == "8000" || httpPort == "8001" {
		receiver := receiver.Receiver{
			HttpPort:             httpPort,
			CompactorPort:        "8002",
			CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
			AutoDetect:           true,
		}
		runtime.GOMAXPROCS(2 * 16)
		receiver.Run()
	} else if httpPort == "8002" {
		compactor := compactor.Compactor{
			HttpPort: httpPort,
		}
		runtime.GOMAXPROCS(1 * 16)
		compactor.Run()
	}
	fmt.Printf("port not macth [%v] \n", httpPort)
	time.Sleep(10 * time.Second)
}
