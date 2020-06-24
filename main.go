package main

import (
	"flag"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	//_ "go.uber.org/automaxprocs"
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
		runtime.GOMAXPROCS(-1),
	)

	defer func() {
		err := recover()
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)
		}
	}()

	rand.Seed(time.Now().Unix())
	var httpPort string
	flag.StringVar(&httpPort, "p", "", "port")
	flag.Parse()

	if httpPort == "8000" || httpPort == "8001" {
		receiver := receiver.Receiver{
			HttpPort:             httpPort,
			CompactorPort:        "8002",
			CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
			//AutoDetect:           true,
		}
		runtime.GOMAXPROCS(2)
		receiver.Run()
	} else if httpPort == "8002" {
		compactor := compactor.Compactor{
			HttpPort: httpPort,
			//AutoDetect: true,
		}
		runtime.GOMAXPROCS(1 * 4)
		compactor.Run()
	}
	fmt.Printf("port not macth [%v] \n", httpPort)
	time.Sleep(10 * time.Second)
}
