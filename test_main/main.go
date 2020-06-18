package main

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	"time"
)

func main() {
	rr := receiver.Receiver{
		HttpPort:             "8000",
		DataPort:             "8081",
		CompactorPort:        "8002",
		CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
		AutoDetect:           false,
	}

	rr2 := receiver.Receiver{
		HttpPort:             "8001",
		DataPort:             "8081",
		CompactorPort:        "8002",
		CompactorSetWrongUrl: fmt.Sprintf("http://127.0.0.1:8002/sw"),
		AutoDetect:           false,
	}

	compactor := compactor.Compactor{
		HttpPort: "8002",
		DataPort: "8081",
	}

	go compactor.Run()
	go rr.Run()
	go rr2.Run()
	time.Sleep(1 * time.Second)
	//receiver.ReadMem("/Users/liangjunyu/Desktop/trace1.data")
	//receiver.ReadMem("/Users/liangjunyu/Desktop/trace1b.data")

	//receiver.ReadFile("/Users/liangjunyu/Desktop/trace1.data")
	//receiver.ReadFile("/Users/liangjunyu/Desktop/trace1b.data")

	//go receiver.ReadHttp("http://127.0.0.1:8081/trace1.data")
	//go receiver.ReadHttp("http://127.0.0.1:8081/trace2.data")

	for {
		time.Sleep(1 * time.Second)
	}
}
