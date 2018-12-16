package main

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-protobuf"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "analyzer")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "Environment", config.Environment()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraHosts", config.CassandraHosts()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraKeyspace", config.CassandraKeyspace()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaAnalyzerTopic", config.KafkaAnalyzerTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaChooserTopic", config.KafkaChooserTopic()))

	// twse
	// twse.Init()
}

var environment string

func process() {

	if environment == config.EnvironmentProd {
		defer func() {
			if err := recover(); err != nil {
				logger.Panic(fmt.Sprintf("recover %v", err))
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace())
	if err != nil {
		logger.Panic(fmt.Sprintf("cassandra.NewClient %v", err))
	}
	defer cassandraClient.Close()

	// analyzer consumer
	var analyzerConsumer *kafka.Consumer
	analyzerConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "analyzer", config.KafkaAnalyzerTopic())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewConsumer %v", err))
	}
	defer analyzerConsumer.Close()

	// chooser producer
	// var chooserProducer *kafka.Producer
	// chooserProducer, err = kafka.NewProducer(config.KafkaBootstrapServers())
	// if err != nil {
	// 	logger.Panic(fmt.Sprintf("kafka.NewProducer %v", err))
	// }
	// defer chooserProducer.Close()

	for {

		message := &protobuf.Analyzer{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		topic, partition, offset, value, err = analyzerConsumer.Consume()
		if err != nil {
			logger.Panic(fmt.Sprintf("Consume %v", err))
		}

		err = proto.Unmarshal(value, message)
		if err != nil {
			logger.Panic(fmt.Sprintf("proto.Unmarshal %v", err))
		}

		switch message.Exchange {
		case "TWSE":
			logger.Info(message.String())
			// err = twse.Process(message.Period, datetime.GetTime(message.Datetime), message.Path, message.IsFinished, cassandraClient, analyzerProducer, analyzerTopic)
			// if err != nil {
			// 	logger.Panic(fmt.Sprintf("Process %v", err))
			// }
		default:
			logger.Panic("Unknown exchange")
		}

		// strange
		offset++

		err = analyzerConsumer.CommitOffset(topic, partition, offset)
		if err != nil {
			logger.Panic(fmt.Sprintf("CommitOffset %v", err))
		}
	}
}

func main() {

	logger.Info("Starting analyzer...")

	// environment
	switch environment = config.Environment(); environment {
	case config.EnvironmentDev, config.EnvironmentTest, config.EnvironmentStg, config.EnvironmentProd:
	default:
		logger.Panic("Unknown environment")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environment != config.EnvironmentProd {
			break
		}
	}
}
