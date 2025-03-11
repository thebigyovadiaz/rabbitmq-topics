package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thebigyovadiaz/rabbitmq-topics/src/util"
	"os"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	util.LogFailOnError(err, "Failed to connect to RabbitMQ")
	util.LogSuccessful("Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.LogFailOnError(err, "Failed to open a channel")
	util.LogSuccessful("Channel open successfully")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		errStr := fmt.Errorf("usage: %s [info] [warning] [error]", os.Args[0])
		util.LogFailOnError(errStr, errStr.Error())
		os.Exit(0)
	}

	for _, s := range os.Args[1:] {
		util.LogSuccessful(fmt.Sprintf("Binding queue %s to exchange %s with routing key %s",
			q.Name, "logs_direct", s))

		err = ch.QueueBind(
			q.Name,       // queue name
			s,            // routing key
			"logs_topic", // exchange
			false,
			nil)

		util.LogFailOnError(err, "Failed to bind a queue")
	}

	messages, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for msg := range messages {
			util.LogSuccessful(fmt.Sprintf(" [x] Sent %s", msg.Body))
		}
	}()

	util.LogSuccessful(" [*]  Waiting for logs. To exit press CTRL+C")
	<-forever
}
