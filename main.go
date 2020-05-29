package main

import (
	"flag"
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	"math/rand"
	"time"
)

func main() {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))

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
	fmt.Printf("port not macth [%v] \n", httpPort)
	time.Sleep(10 * time.Second)
}
