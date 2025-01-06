package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
	"encoding/json"
	"net/smtp"
)

func main() {
	
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
			"security.protocol": "SASL_SSL",
			"sasl.username": "TYNKLLJMPGCT4C3L",
			"sasl.password": "kUp7KuyBiAH/f2+ltEPaRQs1C93uT5A4HzCejxujZ9FROckxq3HVSI2bmgk8fJxG",
			"sasl.mechanisms": "PLAIN",
			"group.id":          "myGroup",
			"auto.offset.reset": "earliest",
		})

		if err != nil {
			panic(err)
		}

		err = consumer.SubscribeTopics([]string{"default"}, nil)

		if err != nil {
			panic(err)
		}

		// A signal handler or similar could be used to set this to false to break the loop.
		run := true

		for run {
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
					//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
					// The client will automatically try to recover from all errors.
					// Timeout is not considered an error because it is raised by
					// ReadMessage in absence of messages.
					fmt.Printf("Consumer error: %v (%v)\n", err, msg)
					return
			} 

			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

			var message map[string]interface{}

			json.Unmarshal(msg.Value, &message)

			ambassadorMessage := []byte(fmt.Sprintf("You earned $%f from the link #%s", message["ambassador_revenue"].(float64), message["code"]))

			smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{message["ambassador_email"].(string)}, ambassadorMessage)

			adminMessage := []byte(fmt.Sprintf("Order #%f with a total of $%f has been completed", message["id"].(float64), message["admin_revenue"].(float64)))

			smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{"admin@admin.com"}, adminMessage)
	
		}

		consumer.Close()

} 
