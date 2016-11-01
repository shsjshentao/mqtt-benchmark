package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

type MQTTOption struct {
	Host         string
	Port         string
	Action       string
	Qos          int
	Retain       bool
	Topic        string
	Msg          string
	Username     string
	Password     string
	TLS          bool
	IntervalTime int
	Client       int
	MessageSize  int
	Interval     int
	Clean        bool
	KeepAlive    int
}

var wg sync.WaitGroup
var pubCount uint64 = 0
var connCount int = 0
var msgCount uint64 = 0
var lastMsgCount uint64 = 0

func main() {
	var mqttOption MQTTOption
	host := flag.String("h", "", "host of the broker")
	port := flag.String("p", "", "port")
	action := flag.String("a", "p", "p:publish,s:subscribe")
	qos := flag.Int("q", 0, "quality level 0|1|2")
	retain := flag.Bool("r", false, "message retain")
	topic := flag.String("t", "mqtt-benchmark", "topic")
	msg := flag.String("m", "hello world", "messages")
	username := flag.String("u", "", "username")
	password := flag.String("P", "", "password")
	client := flag.Int("c", 200, "number of concurrency")
	interval := flag.Int("i", 10, "interval of connecting to the broker")
	tls := flag.Bool("tls", false, "tls")
	clean := flag.Bool("C", false, "clean session")
	keepalive := flag.Int("k", 300, "keepalive")

	flag.Parse()

	mqttOption.Host = *host
	mqttOption.Port = *port
	mqttOption.Action = *action
	mqttOption.Qos = *qos
	mqttOption.Retain = *retain
	mqttOption.Topic = *topic
	mqttOption.Msg = *msg
	mqttOption.Username = *username
	mqttOption.Password = *password
	mqttOption.IntervalTime = *interval
	mqttOption.Client = *client
	mqttOption.TLS = *tls
	mqttOption.Clean = *clean
	mqttOption.KeepAlive = *keepalive

	if mqttOption.Host == "" || mqttOption.Port == "" {
		log.Println("参数输入不全")
	}

	wg.Add(mqttOption.Client)
	for i := 1; i <= mqttOption.Client; i++ {
		go connection(mqttOption)
		wg.Done()
		connCount++
		log.Println("成功连接第" + strconv.Itoa(connCount) + "个客户端")
		time.Sleep(time.Millisecond * time.Duration(1000/mqttOption.IntervalTime))
	}
	wg.Wait()
	if mqttOption.Action == "p" {
		for {
			time.Sleep(time.Second)
			log.Println("已发送" + strconv.FormatUint(pubCount, 10) + "条数据")
		}
	} else {
		for {
			time.Sleep(time.Second)
			log.Println("已接收"+strconv.FormatUint(msgCount, 10)+"条数据", "速度为"+strconv.FormatUint(msgCount-lastMsgCount, 10)+"条/秒")
			lastMsgCount = msgCount
		}
	}

	sig := make(chan os.Signal, 0)
	signal.Notify(sig)
	log.Println("Got signal:", <-sig)
}

func connection(mqttOption MQTTOption) {
	var protocol string
	if mqttOption.TLS {
		protocol = "ssl"
	} else {
		protocol = "tcp"
	}
	r := rand.Int()
	url := protocol + "://" + mqttOption.Host + ":" + mqttOption.Port
	opts := MQTT.NewClientOptions().AddBroker(url)
	if mqttOption.TLS {
		opts.TLSConfig.InsecureSkipVerify = true

	} else {
		opts.TLSConfig.InsecureSkipVerify = false
	}

	opts.SetUsername(mqttOption.Username)
	opts.SetPassword(mqttOption.Password)
	opts.SetClientID(strconv.Itoa(r))
	if mqttOption.Action != "p" {
		opts.SetDefaultPublishHandler(msgHandler)
	}

	opts.SetConnectionLostHandler(connLostHandler)
	opts.SetAutoReconnect(false)
	opts.SetKeepAlive(time.Second * time.Duration(mqttOption.KeepAlive))
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Panic("mqtt连接错误", token.Error())
	}
	wg.Wait()
	if mqttOption.Action == "p" {
		for {
			if !c.IsConnected() {
				break
			}
			if token := c.Publish(mqttOption.Topic, byte(mqttOption.Qos), mqttOption.Retain, mqttOption.Msg); token.Wait() && token.Error() != nil {
			} else {
				atomic.AddUint64(&pubCount, 1)
			}
			time.Sleep(time.Second)
		}
	} else {
		if token := c.Subscribe(mqttOption.Topic, byte(mqttOption.Qos), nil); token.Wait() && token.Error() != nil {
			log.Panic(token.Error())
		}
	}

}

var msgHandler MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
	atomic.AddUint64(&msgCount, 1)
}

var connLostHandler MQTT.ConnectionLostHandler = func(client *MQTT.Client, err error) {
	log.Println("已断开", err)
}
