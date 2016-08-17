package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

type MQTTOption struct {
	Host         string
	Port         string
	Qos          int
	Retain       bool
	Topic        string
	Subscribe    string
	Msg          string
	Username     string
	Password     string
	TLS          bool
	Count        int
	MessageSize  int
	IntervalTime int
	KeepAlive    int
}

var wg sync.WaitGroup
var mutex sync.Mutex
var pubCount int = 0
var connCount int = 0

func main() {
	var mqttOption MQTTOption
	host := flag.String("h", "", "host of the broker")
	port := flag.String("p", "", "port")
	qos := flag.Int("q", 0, "quality level 0|1|2")
	retain := flag.Bool("r", false, "message retain")
	topic := flag.String("t", "mqtt-benchmark", "publish topic")
	msg := flag.String("m", "hello world", "messages")
	subscribe := flag.String("s", "mqtts/test", "subscribe topic")
	username := flag.String("u", "", "username")
	password := flag.String("P", "", "password")
	count := flag.Int("c", 200, "number of concurrency")
	tls := flag.Bool("tls", false, "tls")
	keepalive := flag.Int("k", 300, "keepalive")

	flag.Parse()
	if host == "" || port == "" {
		log.Println("参数输入不全")
	}
	mqttOption.Host = *host
	mqttOption.Port = *port
	mqttOption.Qos = *qos
	mqttOption.Retain = *retain
	mqttOption.Topic = *topic
	mqttOption.Msg = *msg
	mqttOption.Subscribe = *subscribe
	mqttOption.Username = *username
	mqttOption.Password = *password
	mqttOption.Count = *count
	mqttOption.TLS = *tls
	mqttOption.KeepAlive = *keepalive

	wg.Add(mqttOption.Count)
	for i := 1; i <= mqttOption.Count; i++ {
		go connection(mqttOption)
		wg.Done()

		time.Sleep(time.Millisecond * 100)
	}
	for {
		time.Sleep(time.Second)
		log.Println("已发送" + strconv.Itoa(pubCount) + "条数据")
	}
	sig := make(chan os.Signal, 0)
	signal.Notify(sig)
	log.Println("Got signal:", <-sig)
}

func connection(mqttOption MQTTOption) {
	var protocol string
	if mqttOption.TLS == false {
		protocol = "tcp"
	} else {
		protocol = "ssl"
	}
	r := rand.Int()
	url := protocol + "://" + mqttOption.Host + ":" + mqttOption.Port
	opts := MQTT.NewClientOptions().AddBroker(url)
	if mqttOption.TLS == false {
		opts.TLSConfig.InsecureSkipVerify = false
	} else {
		opts.TLSConfig.InsecureSkipVerify = true
	}

	opts.SetUsername(mqttOption.Username)
	opts.SetPassword(mqttOption.Password)
	opts.SetClientID(strconv.Itoa(r))
	opts.SetDefaultPublishHandler(msgHandler)
	opts.SetConnectionLostHandler(connLostHandler)
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(time.Second * time.Duration(mqttOption.KeepAlive))
	opts.SetOnConnectHandler(onConnHandler)
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Panic("mqtt连接错误", token.Error())
	}
	if token := c.Subscribe("test", 0, nil); token.Wait() && token.Error() != nil {
		log.Panic(token.Error())
	}
	wg.Wait()
	for {
		token := c.Publish(mqttOption.Topic, byte(mqttOption.Qos), mqttOption.Retain, mqttOption.Msg)
		token.Wait()
		mutex.Lock()
		pubCount++
		mutex.Unlock()
		time.Sleep(time.Second)
	}
}

var msgHandler MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
}

var connLostHandler MQTT.ConnectionLostHandler = func(client *MQTT.Client, err error) {
	log.Println("已断开", err)
}

var onConnHandler MQTT.OnConnectHandler = func(client *MQTT.Client) {
	connCount++
	log.Println("成功连接第" + strconv.Itoa(connCount) + "个客户端")
}
