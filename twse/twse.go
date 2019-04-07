package twse

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/decimal"
	"github.com/joshchu00/finance-go-common/indicator"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf"
	inf "gopkg.in/inf.v0"
)

var location *time.Location
var indicators []*indicator.Indicator

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
	indicators = []*indicator.Indicator{indicator.SMA0060, indicator.SMA0120, indicator.SMA0240}
}

func Process(symbol string, period string, ts int64, client *cassandra.Client, producer *kafka.Producer, topic string) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse process...", datetime.GetTimeString(ts, location)))

	var rrs []*cassandra.RecordRow

	rrs, err = client.SelectRecordRowsByPartitionKey(
		&cassandra.RecordPartitionKey{
			Exchange: "TWSE",
			Symbol:   symbol,
			Period:   period,
		},
	)
	if err != nil {
		return
	}

	closes := make([]float64, 0)

	for _, rr := range rrs {

		var close float64
		close, err = strconv.ParseFloat(rr.Close.String(), 64)
		if err != nil {
			return
		}

		closes = append(closes, close)
	}

	for _, idct := range indicators {

		var values []float64

		switch idct.Type {
		case indicator.SMA:
			values = indicator.CalculateSMA(closes, idct.Period)
		default:
			err = errors.New("Unknown indicator type")
			return
		}

		for i, rr := range rrs {

			if datetime.GetTimestamp(rr.Datetime) >= ts {

				var value *inf.Dec
				value, err = decimal.GetDecimal(strconv.FormatFloat(values[i], 'f', -1, 64))
				if err != nil {
					return
				}

				client.InsertIndicatorRowDecimalColumn(
					&cassandra.IndicatorPrimaryKey{
						IndicatorPartitionKey: cassandra.IndicatorPartitionKey{
							Exchange: "TWSE",
							Symbol:   symbol,
							Period:   period,
						},
						Datetime: rr.Datetime,
					},
					idct.Column,
					value,
				)
			}
		}
	}

	message := &protobuf.Chooser{
		Exchange: "TWSE",
		Symbol:   symbol,
		Period:   period,
		Datetime: ts,
	}

	var bytes []byte

	bytes, err = proto.Marshal(message)
	if err != nil {
		return
	}

	producer.Produce(topic, 0, bytes)

	return
}
