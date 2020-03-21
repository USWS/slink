package slink

import (
	"encoding/json"
	"errors"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/satori/go.uuid"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ServiceUp        = "usws/svc/up"
	ServiceRpcUp     = "usws/rpc/up"
	ServiceRpcIn     = "usws/rpc/in"
	ServiceRpcOut    = "usws/rpc/out"
	ServiceTopic     = "usws/com"
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
	uuid      string
}

func New(host string) *SLink {
	return &SLink{
		host:      host,
		keepAlive: DefaultKeepAlive,
		deadTime:  DefaultDeadTime,
		uuid:      uuid.NewV4().String(),
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

func (p *SLink) GetClient() MQTT.Client {
	return p.client
}

func (p *SLink) Connect() (MQTT.Client, error) {
	opts := MQTT.NewClientOptions().AddBroker(p.host)
	opts.SetUsername(p.user)
	opts.SetPassword(p.password)
	p.client = MQTT.NewClient(opts)
	if token := p.client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return p.client, nil
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
	services := make(map[string]*innerServiceInfo)
	lock := new(sync.RWMutex)

	if token := p.client.Subscribe(ServiceUp+"/"+serviceName, 0, func(client MQTT.Client, message MQTT.Message) {
		var isInfo innerServiceInfo
		if err := json.Unmarshal(message.Payload(), &isInfo); err != nil {
			log.Println(err)
			return
		}
		lock.Lock()
		if svc, ok := services[isInfo.UUID]; ok {
			svc.upTime = time.Now()
		} else {
			isInfo.upTime = time.Now()
			services[isInfo.UUID] = &isInfo
		}
		if onServiceUp != nil {
			go onServiceUp(&services[isInfo.UUID].SvcInfo)
		}
		lock.Unlock()
	}); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	go func() {
		for {
			time.Sleep(RecyleDuration)
			lock.Lock()
			now := time.Now()
			for k, v := range services {
				if now.After(v.upTime.Add(DefaultDeadTime)) {
					delete(services, k)
				}
			}
			lock.Unlock()
		}
	}()

	return func() *ServiceInfo {
		validSvc := make([]*innerServiceInfo, 0, 4)
		now := time.Now()
		lock.RLock()
		for _, v := range services {
			if !now.After(v.upTime.Add(DefaultDeadTime)) {
				validSvc = append(validSvc, v)
			}
		}
		lock.RUnlock()
		n := len(validSvc)
		if n < 1 {
			return nil
		} else {
			return &validSvc[rand.Intn(n)].SvcInfo
		}
	}, nil
}

func (p *SLink) ProvideService(sInfo *ServiceInfo) {
	go func() {
		isInfo := innerServiceInfo{
			UUID:    p.uuid,
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
type rpcSeverInfo struct {
	UUID   string `json:"u"`
	Name   string `json:"n"`
	upTime time.Time
}

func (p *SLink) InitRpcServer(serviceName string) (Register, error) {
	methods := make(map[string]MethodRequest)
	lock := new(sync.Mutex)
	svcUUID := p.uuid

	if token := p.client.Subscribe(ServiceRpcIn+"/"+serviceName+"/"+svcUUID, 0, func(client MQTT.Client, message MQTT.Message) {
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

	go func() {
		for {
			rpcSvr := rpcSeverInfo{
				UUID: svcUUID,
				Name: serviceName,
			}
			bInfo, err := json.Marshal(rpcSvr)
			if err != nil {
				panic(err)
			}
			for {
				if token := p.client.Publish(ServiceRpcUp+"/"+serviceName, 0, false, string(bInfo)); token.Wait() && token.Error() != nil {
					log.Println(token.Error())
				}
				time.Sleep(p.keepAlive)
			}
		}
	}()

	return func(method string, f MethodRequest) {
		lock.Lock()
		if _, ok := methods[method]; !ok {
			methods[method] = f
		}
		lock.Unlock()
	}, nil
}

type InvokeParam struct {
	Method string `json:"m"`
	Param  string `json:"p"`
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

func SyncInvoker(invoker Invoker, param *InvokeParam, on OnResult, timeout time.Duration) error {
	ch := make(chan bool)
	if err := invoker(param, func(result string, err error) {
		on(result, err)
		ch <- true
	}, timeout); err != nil {
		close(ch)
		return err
	}
	<-ch
	close(ch)
	return nil
}

func (p *SLink) InitRpcClient(serviceName string) (Invoker, error) {
	var reqId int64 = 0
	callbacks := make(map[int64]*timeOuter)
	lock := new(sync.Mutex)
	clientTopic := ServiceRpcOut + "/" + uuid.NewV4().String()
	rpcSvcs := make(map[string]*rpcSeverInfo)
	svcLock := new(sync.RWMutex)

	if token := p.client.Subscribe(ServiceRpcUp+"/"+serviceName, 0, func(client MQTT.Client, message MQTT.Message) {
		var rsi rpcSeverInfo
		if err := json.Unmarshal(message.Payload(), &rsi); err != nil {
			log.Println(err)
			return
		}
		svcLock.Lock()
		if svc, ok := rpcSvcs[rsi.UUID]; ok {
			svc.upTime = time.Now()
		} else {
			rsi.upTime = time.Now()
			rpcSvcs[rsi.UUID] = &rsi
		}
		svcLock.Unlock()
	}); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		return nil, token.Error()
	}

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
		svcLock.Lock()
		for k, v := range rpcSvcs {
			if now.After(v.upTime.Add(DefaultDeadTime)) {
				delete(rpcSvcs, k)
			}
		}
		svcLock.Unlock()
	}()

	return func(param *InvokeParam, on OnResult, timeout time.Duration) error {
		var rpcSvc *rpcSeverInfo
		validSvcs := make([]*rpcSeverInfo, 0, 4)
		now := time.Now()

		svcLock.RLock()
		for _, v := range rpcSvcs {
			if now.After(v.upTime.Add(DefaultDeadTime)) {
				continue
			} else {
				validSvcs = append(validSvcs, v)
			}
		}
		svcLock.RUnlock()
		n := len(validSvcs)
		if n < 1 {
			return errors.New("no service found")
		} else {
			rpcSvc = validSvcs[rand.Intn(n)]
		}

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
		if token := p.client.Publish(ServiceRpcIn+"/"+serviceName+"/"+rpcSvc.UUID, 0, false, jStr); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		return nil
	}, nil
}

type TopicHandler func(msg []byte)

func (p *SLink) Subscribe(topic string, th TopicHandler) error {
	if token := p.client.Subscribe(ServiceTopic+"/"+topic, 0, func(client MQTT.Client, message MQTT.Message) {
		go th(message.Payload())
	}); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

type MultiSubscribe struct {
	Subscribe   func(lis *TopicHandler)
	Unsubscribe func(lis *TopicHandler)
}

func (p *SLink) NewMultiSubscribes(topics []string) (map[string]*MultiSubscribe, error) {
	mss := make(map[string]*MultiSubscribe)
	for _, v := range topics {
		if s, err := p.NewMultiSubscribe(v); err != nil {
			return nil, err
		} else {
			mss[v] = s
		}
	}
	return mss, nil
}

func (p *SLink) NewMultiSubscribe(topic string) (*MultiSubscribe, error) {
	liss := make(map[*TopicHandler]bool)
	lock := new(sync.RWMutex)
	if token := p.client.Subscribe(ServiceTopic+"/"+topic, 0, func(client MQTT.Client, message MQTT.Message) {
		lock.RLock()
		for k, _ := range liss {
			go (*k)(message.Payload())
		}
		lock.RUnlock()
	}); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	return &MultiSubscribe{
		Subscribe: func(lis *TopicHandler) {
			lock.Lock()
			liss[lis] = true
			lock.Unlock()
		},
		Unsubscribe: func(lis *TopicHandler) {
			lock.Lock()
			delete(liss, lis)
			lock.Unlock()
		},
	}, nil
}

func (p *SLink) Unsubscribe(topic string) error {
	if token := p.client.Unsubscribe(ServiceTopic + "/" + topic); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (p *SLink) Publish(topic string, msg string) error {
	if token := p.client.Publish(ServiceTopic+"/"+topic, 0, false, msg); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (p *SLink) NewRpcClients(svcnames []string) (map[string]Invoker, error) {
	clients := make(map[string]Invoker)
	for _, v := range svcnames {
		if c, err := p.InitRpcClient(v); err != nil {
			return nil, err
		} else {
			clients[v] = c
		}
	}
	return clients, nil
}
