package yolo_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/google/trillian"
	"github.com/google/trillian/crypto/keys"
	"github.com/google/trillian/extension"
	i "github.com/google/trillian/integration"
	"github.com/google/trillian/storage/yolo"
	"github.com/google/trillian/testonly/integration"
)

func TestLogIntegration(t *testing.T) {
	ctx := context.Background()
	const numSequencers = 2

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	// producer, err := sarama.NewSyncProducer(brokerList, config)
	producer := mocks.NewSyncProducer(t, config)
	defer producer.Close()
	consumer := &consumerMock{}

	ms := yolo.NewLogStorage(producer, consumer)

	reggie := extension.Registry{
		AdminStorage:  yolo.NewAdminStorage(ms),
		SignerFactory: keys.PEMSignerFactory{},
		LogStorage:    ms,
	}

	env, err := integration.NewLogEnvWithRegistry(ctx, numSequencers, "TestLogIntegration", reggie)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	logID, err := env.CreateLog()
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	consumer.name = strconv.FormatInt(logID, 10)
	for i := 0; i < 1000; i++ {
		producer.ExpectSendMessageWithCheckerFunctionAndSucceed(
			mocks.ValueChecker(consumer.addMessage))
	}

	client := trillian.NewTrillianLogClient(env.ClientConn)
	params := i.DefaultTestParameters(logID)
	if err := i.RunLogIntegration(client, params); err != nil {
		t.Fatalf("Test failed: %v", err)
	}
}

type consumerMock struct {
	sync.RWMutex
	name  string
	topic [][]byte
}

func (c *consumerMock) addMessage(val []byte) error {
	c.Lock()
	defer c.Unlock()
	c.topic = append(c.topic, val)
	return nil
}

func (*consumerMock) Topics() ([]string, error)                  { panic("unimplemented") }
func (*consumerMock) Partitions(topic string) ([]int32, error)   { panic("unimplemented") }
func (*consumerMock) HighWaterMarks() map[string]map[int32]int64 { panic("unimplemented") }
func (*consumerMock) Close() error                               { return nil }
func (*consumerMock) AsyncClose()                                { return }
func (*consumerMock) HighWaterMarkOffset() int64                 { panic("unimplemented") }
func (*consumerMock) Errors() <-chan *sarama.ConsumerError       { panic("unimplemented") }

func (c *consumerMock) ConsumePartition(topic string, partition int32, offset int64) (
	sarama.PartitionConsumer, error) {
	if topic != c.name || partition != 0 || offset != 0 {
		return nil, errors.New("consumerMock: invalid ConsumePartition call")
	}
	return c, nil
}

func (cm *consumerMock) Messages() <-chan *sarama.ConsumerMessage {
	c := make(chan *sarama.ConsumerMessage)
	go func() {
		cm.RLock()
		topic := cm.topic
		cm.RUnlock()
		for offset, val := range topic {
			c <- &sarama.ConsumerMessage{
				Value:     val,
				Topic:     cm.name,
				Partition: 0,
				Offset:    int64(offset),
			}
		}
		close(c)
	}()
	return c
}
