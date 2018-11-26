package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-protobuf"
)

func init() {

	// log
	logfile, err := os.OpenFile("logfile.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("FATAL", "Open log file error:", err)
	}

	log.SetOutput(logfile)
	log.SetPrefix("ANALYZER ")
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	// log config
	log.Println("INFO", "Environment:", config.Environment())
	log.Println("INFO", "CassandraHosts:", config.CassandraHosts())
	log.Println("INFO", "CassandraKeyspace:", config.CassandraKeyspace())
	log.Println("INFO", "KafkaBootstrapServers:", config.KafkaBootstrapServers())
	log.Println("INFO", "KafkaAnalyzerTopic:", config.KafkaAnalyzerTopic())
	log.Println("INFO", "KafkaChooserTopic:", config.KafkaChooserTopic())
}

var environment string

func process() {

	if environment == "prod" {
		defer func() {
			if err := recover(); err != nil {
				log.Println("PANIC", "recover", err)
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	if cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace()); err != nil {
		return
	}
	defer cassandraClient.Close()

	// analyzer consumer
	var analyzerConsumer *kafka.Consumer
	if analyzerConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "analyzer", config.KafkaAnalyzerTopic()); err != nil {
		return
	}
	defer analyzerConsumer.Close()

	// chooser producer
	// var chooserProducer *kafka.Producer
	// if chooserProducer, err = kafka.NewProducer(config.KafkaBootstrapServers()); err != nil {
	// 	return
	// }
	// defer chooserProducer.Close()

	for {

		message := &protobuf.Analyzer{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		if topic, partition, offset, value, err = analyzerConsumer.Consume(); err != nil {
			log.Panicln("PANIC", "Consume", err)
		}

		if err = proto.Unmarshal(value, message); err != nil {
			log.Panicln("PANIC", "Unmarshal", err)
		}

		switch message.Exchange {
		case "TWSE":
			fmt.Println(message)
			// if err = twse.Process(message.Period, datetime.GetTime(message.Datetime), message.Path, message.IsFinished, cassandraClient, analyzerProducer, analyzerTopic); err != nil {
			// 	log.Panicln("PANIC", "Process", err)
			// }
		default:
			log.Panicln("PANIC", "Unknown exchange")
		}

		// strange
		offset++

		if err = analyzerConsumer.CommitOffset(topic, partition, offset); err != nil {
			log.Panicln(err)
		}
	}
}

func main() {

	log.Println("INFO", "Starting analyzer...")

	// environment
	environment = config.Environment()

	if environment != "dev" && environment != "test" && environment != "stg" && environment != "prod" {
		log.Panicln("PANIC", "Unknown environment")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environment != "prod" {
			break
		}
	}
}
