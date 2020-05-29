package main

import (
	"fmt"
	"github.com/ljy2010a/tailf-based-sampling/compactor"
	"github.com/ljy2010a/tailf-based-sampling/receiver"
	"time"
)

func main() {
	receiver := receiver.Receiver{
		HttpPort:      "8000",
		DataPort:      "8081",
		CompactorPort: "8002",
	}

	compactor := compactor.Compactor{
		HttpPort: "8002",
		DataPort: "8081",
	}

	go compactor.Run()
	go receiver.Run()
	time.Sleep(1 * time.Second)
	fmt.Println("start")
	//receiver.ReadMem("/Users/liangjunyu/Desktop/trace1.data")
	//receiver.ReadMem("/Users/liangjunyu/Desktop/trace1b.data")

	//receiver.ReadFile("/Users/liangjunyu/Desktop/trace1.data")
	//receiver.ReadFile("/Users/liangjunyu/Desktop/trace1b.data")

	go receiver.ReadHttp("http://127.0.0.1:8081/trace1.data")
	go receiver.ReadHttp("http://127.0.0.1:8081/trace2.data")
	//receiver.ReadHttp("http://127.0.0.1:8081/trace1b.data")

	//receiver2 := receiver.Receiver{
	//	HttpPort:      "8001",
	//	DataPort:      "8081",
	//	CompactorPort: "8002",
	//}
	//go receiver.ReadHttp("http://127.0.0.1:8081/trace2.data")

	for {
		time.Sleep(1 * time.Second)
	}
}
