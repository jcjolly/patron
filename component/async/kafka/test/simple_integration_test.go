// +build integration

package simple

import (
	"testing"
	"time"

	"github.com/beatlabs/patron/component/async/kafka/simple"

	"github.com/Shopify/sarama"
	"github.com/beatlabs/patron/component/async/kafka"
	dockerKafka "github.com/beatlabs/patron/test/docker/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleConsume(t *testing.T) {
	sent := []string{"one", "two", "three"}
	chMessages := make(chan []string)
	chErr := make(chan error)
	go func() {

		factory, err := simple.New("test1", topic, dockerKafka.Brokers(), kafka.DecoderJSON(), kafka.Version(sarama.V2_1_0_0.String()),
			kafka.StartFromNewest())
		if err != nil {
			chErr <- err
			return
		}

		consumer, err := factory.Create()
		if err != nil {
			chErr <- err
			return
		}
		defer func() {
			_ = consumer.Close()
		}()

		received, err := consumeMessages(consumer, len(sent))
		if err != nil {
			chErr <- err
			return
		}

		chMessages <- received
	}()

	time.Sleep(5 * time.Second)

	prod, err := dockerKafka.NewProducer()
	require.NoError(t, err)
	for _, message := range sent {
		_, _, err = prod.SendMessage(getProducerMessage(message))
		require.NoError(t, err)
	}

	var received []string

	select {
	case received = <-chMessages:
	case err = <-chErr:
		require.NoError(t, err)
	}

	assert.Equal(t, sent, received)
}

func TestSimpleConsume_ClaimMessageError(t *testing.T) {
	chMessages := make(chan []string)
	chErr := make(chan error)
	go func() {

		factory, err := simple.New("test1", topic, dockerKafka.Brokers(), kafka.Version(sarama.V2_1_0_0.String()),
			kafka.StartFromNewest())
		if err != nil {
			chErr <- err
			return
		}

		consumer, err := factory.Create()
		if err != nil {
			chErr <- err
			return
		}
		defer func() {
			_ = consumer.Close()
		}()

		received, err := consumeMessages(consumer, 1)
		if err != nil {
			chErr <- err
			return
		}

		chMessages <- received
	}()

	time.Sleep(5 * time.Second)

	prod, err := dockerKafka.NewProducer()
	require.NoError(t, err)
	_, _, err = prod.SendMessage(getProducerMessage("123"))
	require.NoError(t, err)

	select {
	case <-chMessages:
		require.Fail(t, "no messages where expected")
	case err = <-chErr:
		require.EqualError(t, err, "could not determine decoder  failed to determine content type from message headers [] : content type header is missing")
	}
}
