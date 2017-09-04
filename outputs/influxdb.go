package outputs

import (
	"errors"
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/negbie/fluxify/logp"
)

type InfluxdbOutputer struct {
	addr          string
	client        *InfluxClient
	InfluxdbQueue chan []byte
}

type Metric struct {
	measurement string
	tags        map[string]string
	fields      map[string]interface{}
	time        time.Time
}

type InfluxClient struct {
	client        influx.Client
	database      string
	batchSize     int
	pointsChannel chan *influx.Point
	batchConfig   influx.BatchPointsConfig
	errorFunc     func(err error)
}

type InfluxClientConfig struct {
	Endpoint     string
	Database     string
	BatchSize    int
	FlushTimeout time.Duration
	ErrorFunc    func(err error)
}

func NewInfluxdbOutputer(serverAddr string) (*InfluxdbOutputer, error) {
	i := &InfluxdbOutputer{
		addr:          serverAddr,
		InfluxdbQueue: make(chan []byte, 1024),
	}
	err := i.Init()
	if err != nil {
		return nil, err
	}
	go i.Start()
	return i, nil
}

func (i *InfluxdbOutputer) Init() error {
	conn, err := i.ConnectServer(i.addr)
	if err != nil {
		logp.Err("InfluxdbOutputer server connection error: %v", err)
		return err
	}
	i.client = conn
	return nil
}

func (i *InfluxdbOutputer) Close() {
	i.client.client.Close()
	logp.Info("InfluxdbOutputer connection close.")
}

func (i *InfluxdbOutputer) ReConnect() error {
	logp.Warn("reconnect server.")
	conn, err := i.ConnectServer(i.addr)
	if err != nil {
		logp.Err("reconnect server error: %v", err)
		return err
	}
	i.client = conn
	return nil
}

func (i *InfluxdbOutputer) ConnectServer(addr string) (*InfluxClient, error) {
	ic, err := newInfluxClient(&InfluxClientConfig{
		Endpoint:     "http://" + addr,
		Database:     "fluxify",
		BatchSize:    256,
		FlushTimeout: 5 * time.Second,
		ErrorFunc:    checkErr,
	})
	if err != nil {
		return nil, err
	}
	return ic, nil
}

func (i *InfluxdbOutputer) Output(msg []byte) {
	i.InfluxdbQueue <- msg
}

func (i *InfluxdbOutputer) Send(msg []byte) {
	tags := map[string]string{
		"host": "sniffer",
	}
	fields := map[string]interface{}{
		"prefix": string(msg),
	}

	defer func() {
		if err := recover(); err != nil {
			logp.Err("send msg error: %v", err)
		}
	}()

	if err := i.client.send(NewMetric("fluxify", tags, fields)); err != nil {
		log.Printf("Could not send metric to influxDB: %s\n", err.Error())
		if i.client.errorFunc != nil {
			i.client.errorFunc(err)
		}
	}
}

func (i *InfluxdbOutputer) Start() {
	defer func() {
		if err := recover(); err != nil {
			logp.Err("recover() error: %v", err)
		}
		i.Close()
	}()

	for {
		select {
		case msg := <-i.InfluxdbQueue:
			i.Send(msg)
		}
	}
}

func NewMetric(measurement string, tags map[string]string, fields map[string]interface{}) *Metric {
	return &Metric{
		measurement: measurement,
		tags:        tags,
		fields:      fields,
		time:        time.Now(),
	}
}

func (influxDB *InfluxClient) send(metric *Metric) error {
	if influxDB == nil {
		return errors.New("Failed to create influxDB client")
	}

	pt, err := influx.NewPoint(metric.measurement, metric.tags, metric.fields, metric.time)
	if err != nil {
		return err
	}

	influxDB.pointsChannel <- pt
	return nil
}

func (influxDB *InfluxClient) bulk(points chan *influx.Point, ticker *time.Ticker) {
	pointsBuffer := make([]*influx.Point, influxDB.batchSize)
	currentBatchSize := 0
	for {
		select {
		case <-ticker.C:
			err := influxDB.flush(pointsBuffer, currentBatchSize)
			if err != nil && influxDB.errorFunc != nil {
				influxDB.errorFunc(err)
			}
			currentBatchSize = 0
		case point := <-points:
			pointsBuffer[currentBatchSize] = point
			currentBatchSize++
			if influxDB.batchSize == currentBatchSize {
				err := influxDB.flush(pointsBuffer, currentBatchSize)
				if err != nil && influxDB.errorFunc != nil {
					influxDB.errorFunc(err)
				}
				currentBatchSize = 0
			}
		}
	}
}

func (influxDB *InfluxClient) flush(points []*influx.Point, size int) error {
	if size > 0 {
		newBatch, err := influx.NewBatchPoints(influxDB.batchConfig)
		if err != nil {
			return err
		}
		newBatch.AddPoints(points[0:size])
		err = influxDB.client.Write(newBatch)
		if err != nil {
			return err
		}
	}
	return nil
}

func newInfluxClient(config *InfluxClientConfig) (*InfluxClient, error) {
	// This is just a connection to see if we have no errors
	httpConfig := influx.HTTPConfig{Addr: config.Endpoint, Timeout: 5 * time.Second}
	influxDBClient, err := influx.NewHTTPClient(httpConfig)
	if err != nil {
		return nil, err
	}
	// Create database if it's not already created
	_, err = influxDBClient.Query(influx.Query{
		Command: "CREATE DATABASE fluxify",
	})
	if err != nil {
		return nil, err
	}

	iClient := &InfluxClient{
		client:        influxDBClient,
		database:      config.Database,
		batchSize:     config.BatchSize,
		pointsChannel: make(chan *influx.Point, config.BatchSize*50),
		batchConfig:   influx.BatchPointsConfig{Database: config.Database},
		errorFunc:     config.ErrorFunc,
	}
	go iClient.bulk(iClient.pointsChannel, time.NewTicker(config.FlushTimeout))
	return iClient, nil
}

func checkErr(err error) {
	if err != nil {
		logp.Warn("%v", err)
	}
}

func checkCritErr(err error) {
	if err != nil {
		logp.Critical("%v", err)
	}
}
