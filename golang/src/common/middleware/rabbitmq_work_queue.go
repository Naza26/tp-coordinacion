package middleware

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitWorkQueueMiddleware struct {
	Queue        amqp091.Queue
	Connection   *amqp091.Connection
	Channel      *amqp091.Channel
	ConsumerName string
}

func InitializeRabbitWQ(queueName string, connectionSettings ConnSettings) (*RabbitWorkQueueMiddleware, error) {
	hostname := connectionSettings.Hostname
	port := connectionSettings.Port
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port)
	connection, err := amqp091.Dial(address)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}

	const EMPTY_NAME = ""

	return &RabbitWorkQueueMiddleware{
		Queue:        queue,
		Connection:   connection,
		Channel:      channel,
		ConsumerName: EMPTY_NAME,
	}, nil

}

func (rabbitMiddleware *RabbitWorkQueueMiddleware) Send(message Message) error {
	messageToPublish := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message.Body),
	}
	const DEFAULT_EXCHANGE = ""
	err := rabbitMiddleware.Channel.Publish(
		DEFAULT_EXCHANGE,
		rabbitMiddleware.Queue.Name,
		false,
		false,
		messageToPublish)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (rabbitMiddleware *RabbitWorkQueueMiddleware) Close() error {
	if rabbitMiddleware.Connection.IsClosed() {
		return nil
	}
	// Closing a connection closes associated channels too
	if err := rabbitMiddleware.Connection.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func (rabbitMiddleware *RabbitWorkQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	const CONSUMER_NAME = "data"
	messages, err := rabbitMiddleware.Channel.Consume(
		rabbitMiddleware.Queue.Name,
		CONSUMER_NAME,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	rabbitMiddleware.ConsumerName = CONSUMER_NAME

	for delivery := range messages {
		receivedMessage := Message{Body: string(delivery.Body)}
		ack := func() { delivery.Ack(false) }
		nack := func() { delivery.Nack(false, false) }
		callbackFunc(receivedMessage, ack, nack)
	}

	return nil
}

func (rabbitMiddleware *RabbitWorkQueueMiddleware) StopConsuming() error {
	if rabbitMiddleware.ConsumerName == "" {
		return nil
	}

	err := rabbitMiddleware.Channel.Cancel(rabbitMiddleware.ConsumerName, false)

	if err != nil {
		return ErrMessageMiddlewareClose
	}

	rabbitMiddleware.ConsumerName = ""

	return nil
}
