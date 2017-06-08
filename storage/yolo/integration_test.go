package yolo_test

import (
	"context"
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

	hbaseClient := yolo.NewMockClient("mock_quorum")
	defer hbaseClient.Close()

	ms := yolo.NewLogStorage(producer, consumer, hbaseClient)

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

// Kafka mocks

type consumerMock struct {
	sync.RWMutex
	name  string
	topic [][]byte
}

type partConsMock struct {
	cm     *consumerMock
	offset int64
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

func (partConsMock) Close() error                         { return nil }
func (partConsMock) AsyncClose()                          { return }
func (partConsMock) HighWaterMarkOffset() int64           { panic("unimplemented") }
func (partConsMock) Errors() <-chan *sarama.ConsumerError { panic("unimplemented") }

func (c *consumerMock) ConsumePartition(topic string, partition int32, offset int64) (
	sarama.PartitionConsumer, error) {
	if c.name == "" {
		c.name = topic
	}
	if topic != c.name || partition != 0 {
		panic("consumerMock: invalid ConsumePartition call")
	}
	return partConsMock{c, offset}, nil
}

func (p partConsMock) Messages() <-chan *sarama.ConsumerMessage {
	c := make(chan *sarama.ConsumerMessage)
	go func() {
		p.cm.RLock()
		topic := p.cm.topic
		p.cm.RUnlock()
		for offset, val := range topic[p.offset:] {
			c <- &sarama.ConsumerMessage{
				Value:     val,
				Topic:     p.cm.name,
				Partition: 0,
				Offset:    int64(offset),
			}
		}
		close(c)
	}()
	return c
}
