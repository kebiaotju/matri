package rmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

//Producer is rabbit mq producer
type Producer struct {
	*amqp.Channel
	*amqp.Connection

	URL string
}

//Init rmq
func (r *Producer) Init(url string) (err error) {
	r.URL = url
	return r.ReConnect()
}

//ReConnect return amqp.Dial(r.URL)
func (r *Producer) ReConnect() (err error) {
	if err = r.TryConnect(); err != nil {
		return fmt.Errorf("Failed to Connectionect to RabbitMQ: %v", err)
	}
	if err = r.TryChannel(); err != nil {
		return fmt.Errorf("Failed to open a channel: %v", err)
	}
	return nil
}

//TryConnect return amqp.Dial(r.URL)
func (r *Producer) TryConnect() (err error) {
	if r.Connection != nil {
		r.Connection.Close() //ignore error
	}
	r.Connection, err = amqp.Dial(r.URL)
	return
}

//TryChannel return amqp.Dial(r.URL).Channel()
func (r *Producer) TryChannel() (err error) {
	if r.Channel != nil {
		r.Channel.Close() //ignore error
	}
	r.Channel, err = r.Connection.Channel()
	return
}

//Destroy close mq
func (r *Producer) Destroy() {
	if r.Channel != nil {
		r.Channel.Close()
	}
	if r.Connection != nil {
		r.Connection.Close()
	}
}

//Push raw msg to route exchange
func (r *Producer) Push(exchange, key string, data []byte) error {
	return r.Channel.Publish(
		exchange, // exchange
		key,      // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
}
