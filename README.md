# slink

Base on mqtt protocal.

go get "github.com/USWS/slink"

## Features

1. Service discovery
1. RPC
2. Subscribe & Publish

## Connect

	slk := slink.New("mqtt_broker_host:port").SetUserName("user").SetPassword("password")
	if _, err := dis.Connect(); err != nil {
	   panic(err)
	}

## Service discovery

	// service provider side :
	slk.ProvideService(&slink.ServiceInfo{
	   Name: "service name",
	   IP:   "ip address",
	   Port: port number,
	})

	// client side or other service :
	SvcGetter, err := slk.SetOnServiceUp("service_name", nil)
	if err != nil {
	   log.Println(err)
	}

	go func() {
	   for {
	      log.Println(SvcGetter())
	      time.Sleep(1 * time.Second)
	   }
	}()
	
## RPC

	//RPC service provide side：
	Register, err := slk.InitRpcServer("service1")
	if err != nil {
	   panic(err)
	}
	Register("method_name_1", func(param string) (string, error) {
	   return "name", errors.New("some error")
	})
	Register("method_name_2", func(param string) (string, error) {
	   return "return value", nil
	})
	Register("method_name_3", func(param string) (string, error) {
	   return "return value", nil
	})
	Register("method_name_4", func(param string) (string, error) {
	   return "return value", nil
	})

	//RPC client side：
	Service1Invoker, err := slk.InitRpcClient("service1")
	if err != nil {
	   panic(err)
	}

    //async invoke
	if err := Service1Invoker(&slink.InvokeParam{
       Method:      "method_name_1",
       Param:       `json string`,
    }, func(result string, err error) {
       log.Println(result, err)
    }, time.Second*5); err != nil {
       log.Println(err)
    }

    //sync invoke
    slk.SyncInvoker(Service1Invoker,&slink.InvokeParam{
       Method:      "method_name_1",
       Param:       `json string`,
    }, func(result string, err error) {
       log.Println(result, err)
    }, time.Second*5);err != nil {
       log.Println(err)
    }

    //multi clients
    if clis, err := slk.NewRpcClients([]string{
       "svc1", "svc2", "svc3",
    }); err != nil {
       log.Println(err)
    } else {
       if err := clis["svc1"](&slink.InvokeParam{
	      Method: "GetName",
	      Param:  "",
       }, func(result string, err error) {
	      log.Println(result, err)
       }, time.Second*5); err != nil {
	      log.Println(err)
       }
    }


## Subscribe & Publish

    if err := slk.Subscribe("abc", func(msg []byte) {
		log.Println(string(msg))
	}); err != nil {
		panic(err)
	}

    if err := slk.Publish("abc", "123"); err != nil {
       log.Println(err)
    }

    if mul, err := slk.NewMultiSubscribe("topic"); err != nil {
       log.Println(err)
    } else {
       var a slink.TopicHandler = func(msg []byte) {
          log.Println(string(msg))
       }
       mul.Subscribe(&a)
       var b slink.TopicHandler = func(msg []byte) {
          log.Println(msg)
       }
       mul.Subscribe(&b)
       _ = slk.Publish("topic", "multi")
       mul.Unsubscribe(&a)
       _ = slk.Publish("topic", "end")
    }

    ms, _ := slk.NewMultiSubscribes([]string{
       "topic", "topic1",
    })
    var a slink.TopicHandler = func(msg []byte) {
       log.Println(string(msg))
    }
    ms["topic"].Subscribe(&a)
    var b slink.TopicHandler = func(msg []byte) {
       log.Println(string(msg))
    }
    ms["topic1"].Subscribe(&b)
    _ = slk.Publish("topic", "topic")
    _ = slk.Publish("topic1", "topic1")