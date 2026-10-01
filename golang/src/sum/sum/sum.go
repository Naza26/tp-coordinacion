package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue          middleware.Middleware
	outputExchange      middleware.Middleware
	flushInputExchange  middleware.Middleware
	flushOutputExchange middleware.Middleware
	mutex               sync.Mutex
	fruitItemMap        map[int]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for nodeId := range config.AggregationAmount {
		outputExchangeRouteKeys[nodeId] = fmt.Sprintf("%s_%d", config.AggregationPrefix, nodeId)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	flushInputExchangeRouteKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	flushInputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, flushInputExchangeRouteKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	flushOutputExchangeRouteKeys := make([]string, config.SumAmount)
	for nodeId := range config.SumAmount {
		flushOutputExchangeRouteKeys[nodeId] = fmt.Sprintf("%s_%d", config.SumPrefix, nodeId)
	}

	flushOutputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, flushOutputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		flushInputExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:          inputQueue,
		outputExchange:      outputExchange,
		flushInputExchange:  flushInputExchange,
		flushOutputExchange: flushOutputExchange,
		fruitItemMap:        map[int]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.flushInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleFlushMessage(msg, ack, nack)
	})
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId int) error {
	slog.Info("Received End Of Records message, broadcasting flush", "clientId", clientId)
	flushMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, flushMessage)
	if err != nil {
		slog.Debug("While serializing flush message", "err", err)
		return err
	}
	if err := sum.flushOutputExchange.Send(*message); err != nil {
		slog.Debug("While sending flush message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientId int, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	clientFruitItemMap, ok := sum.fruitItemMap[clientId]
	if !ok {
		clientFruitItemMap = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[clientId] = clientFruitItemMap
	}
	for _, fruitRecord := range fruitRecords {
		_, ok := clientFruitItemMap[fruitRecord.Fruit]
		if ok {
			clientFruitItemMap[fruitRecord.Fruit] = clientFruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) handleFlushMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, _, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing flush message", "err", err)
		return
	}

	if err := sum.flushClient(clientId); err != nil {
		slog.Error("While flushing client", "err", err)
	}
}

func (sum *Sum) flushClient(clientId int) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	slog.Info("Flushing client", "clientId", clientId)
	clientFruitItemMap := sum.fruitItemMap[clientId]
	for key := range clientFruitItemMap {
		fruitRecord := []fruititem.FruitItem{clientFruitItemMap[key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(sum.fruitItemMap, clientId)
	return nil
}
