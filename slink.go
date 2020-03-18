package slink

import (
	"encoding/json"
	"errors"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/satori/go.uuid"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ServiceUp        = "usws/svc/up"
	ServiceRpcIn     = "usws/rpc/in"
	ServiceRpcOut    = "usws/rpc/out"
	RecyleDuration   = time.Second * 5
	DefaultKeepAlive = time.Second * 5
	DefaultDeadTime  = time.Second * 15
)

type SLink struct {
	client    MQTT.Client
	host      string
	user      string
	password  string
	keepAlive time.Duration
	deadTime  time.Duration
}

func New(host string) *SLink {
	return &SLink{
		host:      host,
		keepAlive: DefaultKeepAlive,
		deadTime:  DefaultDeadTime,
	}
}

func (p *SLink) SetUserName(name string) *SLink {
	p.user = name
	return p
}

func (p *SLink) SetPassword(password string) *SLink {
	p.password = password
	return p
}

func (p *SLink) Connect() (MQTT.Client, error) {
	opts := MQTT.NewClientOptions().AddBroker(p.host)
	opts.SetUsername(p.user)
	opts.SetPassword(p.password)
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	p.client = c
	return c, nil
}

type ServiceInfo struct {
	Name string `json:"n"`
	IP   string `json:"i"`
	Port int    `json:"p"`
}

type innerServiceInfo struct {
	UUID    string      `json:"u"`
	SvcInfo ServiceInfo `json:"s"`
	upTime  time.Time
}

func (p *SLink) SetKeepAlive(t time.Duration) *SLink {
	p.keepAlive = t
	return p
}

func (p *SLink) SetDeadTime(t time.Duration) *SLink {
	p.deadTime = t
	return p
}

type ServiceGetter func() *ServiceInfo
type OnServiceUp func(p *ServiceInfo)

func (p *SLink) SetOnServiceUp(serviceName string, onServiceUp OnServiceUp) (ServiceGetter, error) {
	var pIsInfo *innerServiceInfo
	lock := new(sync.RWMutex)

	if token := p.client.Subscribe(ServiceUp+"/"+serviceName, 0, func(client MQTT.Client, message MQTT.Message) {
		var isInfo innerServiceInfo
		if err := json.Unmarshal(message.Payload(), &isInfo); err != nil {
			log.Println(err)
			return
		}
		lock.Lock()
		isInfo.upTime = time.Now()
		pIsInfo = &isInfo
		if onServiceUp != nil {
			go onServiceUp(&pIsInfo.SvcInfo)
		}
		lock.Unlock()
	}); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return func() *ServiceInfo {
		lock.RLock()
		defer lock.RUnlock()
		if pIsInfo == nil || time.Now().After(pIsInfo.upTime.Add(p.deadTime)) {
			return nil
		}
		return &pIsInfo.SvcInfo
	}, nil
}

func (p *SLink) ProvideService(sInfo *ServiceInfo) {
	go func() {
		isInfo := innerServiceInfo{
			UUID:    uuid.NewV4().String(),
			SvcInfo: *sInfo,
		}
		bInfo, err := json.Marshal(isInfo)
		if err != nil {
			panic(err)
		}
		for {
			if token := p.client.Publish(ServiceUp+"/"+sInfo.Name, 0, false, string(bInfo)); token.Wait() && token.Error() != nil {
				log.Println(token.Error())
			}
			time.Sleep(p.keepAlive)
		}
	}()
}

type MethodRequest func(param string) (string, error)
type Register func(method string, f MethodRequest)

func (p *SLink) InitRpcServer(serviceName string) (Register, error) {
	methods := make(map[string]MethodRequest)
	lock := new(sync.Mutex)

	if token := p.client.Subscribe(ServiceRpcIn+"/"+serviceName, 0, func(client MQTT.Client, message MQTT.Message) {
		var cParam clientInvokeParam
		if err := json.Unmarshal(message.Payload(), &cParam); err != nil {
			log.Println(err)
			return
		}
		lock.Lock()
		if v, ok := methods[cParam.Param.Method]; ok {
			go func() {
				str, err := v(cParam.Param.Param)
				jStr, err := json.Marshal(&serverReturnParam{
					ReqId:  cParam.ReqId,
					Result: str,
					ErrMsg: func() string {
						if err != nil {
							return "remote error:" + err.Error()
						}
						return ""
					}(),
				})
				if err != nil {
					log.Println(err)
					return
				}
				if token := p.client.Publish(cParam.ClientTopic, 0, false, jStr); token.Wait() && token.Error() != nil {
					log.Println(token.Error())
				}
			}()
		}
		lock.Unlock()
	}); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return func(method string, f MethodRequest) {
		lock.Lock()
		if _, ok := methods[method]; !ok {
			methods[method] = f
		}
		lock.Unlock()
	}, nil
}

type InvokeParam struct {
	ServiceName string `json:"n"`
	Method      string `json:"m"`
	Param       string `json:"p"`
}
type clientInvokeParam struct {
	ReqId       int64       `json:"i"`
	ClientTopic string      `json:"t"`
	Param       InvokeParam `json:"p"`
}
type serverReturnParam struct {
	ReqId  int64  `json:"i"`
	Result string `json:"r"`
	ErrMsg string `json:"e"`
}
type OnResult func(result string, err error)
type Invoker func(param *InvokeParam, on OnResult, timeout time.Duration) error
type timeOuter struct {
	On      OnResult
	ReqTime time.Time
	Timeout time.Duration
}

func (p *SLink) InitRpcClient() (Invoker, error) {
	var reqId int64 = 0
	callbacks := make(map[int64]*timeOuter)
	lock := new(sync.Mutex)
	clientTopic := ServiceRpcOut + "/" + uuid.NewV4().String()

	if token := p.client.Subscribe(clientTopic, 0, func(client MQTT.Client, message MQTT.Message) {
		var iInParam serverReturnParam
		if err := json.Unmarshal(message.Payload(), &iInParam); err != nil {
			log.Println(err)
			return
		}
		lock.Lock()
		timeOuter, ok := callbacks[iInParam.ReqId]
		if ok {
			delete(callbacks, iInParam.ReqId)
			go timeOuter.On(iInParam.Result, func() error {
				if iInParam.ErrMsg != "" {
					return errors.New(iInParam.ErrMsg)
				} else {
					return nil
				}
			}())
		}
		lock.Unlock()
	}); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	go func() {
		time.Sleep(RecyleDuration)
		lock.Lock()
		now := time.Now()
		for k, v := range callbacks {
			if now.After(v.ReqTime.Add(v.Timeout)) {
				delete(callbacks, k)
				go v.On("", errors.New("timeout"))
			}
		}
		lock.Unlock()
	}()

	return func(param *InvokeParam, on OnResult, timeout time.Duration) error {
		id := atomic.AddInt64(&reqId, 1)
		iParam := clientInvokeParam{
			ReqId:       id,
			Param:       *param,
			ClientTopic: clientTopic,
		}
		jStr, err := json.Marshal(iParam)
		if err != nil {
			return err
		}
		lock.Lock()
		callbacks[id] = &timeOuter{
			On:      on,
			ReqTime: time.Now(),
			Timeout: timeout,
		}
		lock.Unlock()
		if token := p.client.Publish(ServiceRpcIn+"/"+param.ServiceName, 0, false, jStr); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		return nil
	}, nil
}
