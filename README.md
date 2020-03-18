# slink

Base on mqtt protocal.

go get "github.com/USWS/slink"

## Features

1. Service discovery
1. RPC

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
	Register, err := slk.InitRpcServer("service_name")
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
	Invoker, err := slk.InitRpcClient()
	if err != nil {
	   panic(err)
	}

	go func() {
	   for {
	      if err := Invoker(&slink.InvokeParam{
	         ServiceName: "service_name",
	         Method:      "method_name_1",
	         Param:       `json string`,
	      }, func(result string, err error) {
	         log.Println(result, err)
	      }, time.Second*5); err != nil {
	         log.Println(err)
	      }
	      time.Sleep(time.Nanosecond)
	   }
	}()
