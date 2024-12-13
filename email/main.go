package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
	//"net/smtp"
)

func main() {


		ambassadorMessage := []byte(fmt.Sprintf("You earned $%f from the link #%s", ambassadorRevenue, order.Code))

		smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{order.AmbassadorEmail}, ambassadorMessage)

		adminMessage := []byte(fmt.Sprintf("Order #%d with a total of $%f has been completed", order.Id, adminRevenue))

		smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{"admin@admin.com"}, adminMessage)
	
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
			"security.protocol": "SASL_SSL",
			"sasl.username": "TYNKLLJMPGCT4C3L",
			"sasl.password": "kUp7KuyBiAH/f2+ltEPaRQs1C93uT5A4HzCejxujZ9FROckxq3HVSI2bmgk8fJxG",
			"sasl.mechanism": "PLAIN",
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
			if err == nil {
					fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else if !err.(kafka.Error).IsTimeout() {
				// The client will automatically try to recover from all errors.
				// Timeout is not considered an error because it is raised by
				// ReadMessage in absence of messages.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}

		consumer.Close()

}
