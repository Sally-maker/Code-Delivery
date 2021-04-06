package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"	
	"github.com/codeedu/imersaofsfc2-simulator/application/route"
	"github.com/codeedu/imersaofsfc2-simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(msg *ckafka.Message){
	produce := kafka.NewKafkaProducerr()	
	route2 := route.NewRoute()
	json.Unmarshal(msg.Value, &route2)
	route2.LoadPositions()
	positions, err := route2.ExportJsonPositions()
	if err!= nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p,os.Getenv("KafkaProduceTopic"), produce)
		time.Sleep(time.Millisecond * 500)
	}	
}