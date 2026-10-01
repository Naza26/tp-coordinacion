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
	inputQueue              middleware.Middleware
	outputExchange          middleware.Middleware
	flushInputExchange      middleware.Middleware
	flushOutputExchange     middleware.Middleware
	mutex                   sync.Mutex
	fruitItemMap            map[int]map[string]fruititem.FruitItem
	processedClientMessages map[int]int
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
		inputQueue:              inputQueue,
		outputExchange:          outputExchange,
		flushInputExchange:      flushInputExchange,
		flushOutputExchange:     flushOutputExchange,
		fruitItemMap:            map[int]map[string]fruititem.FruitItem{},
		processedClientMessages: map[int]int{},
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

	protocolMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch message := protocolMessage.(type) {
	case inner.DataMessage:
		if err := sum.handleDataMessage(message.ClientId, message.FruitRecords); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	case inner.EofMessage:
		if err := sum.handleEndOfRecordMessage(message.ClientId, message.Total); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	default:
		slog.Error("Unexpected message type on input queue", "clientId", message.GetClientId())
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId int, total int) error {
	slog.Info("Received End Of Records message, broadcasting flush", "clientId", clientId)
	flushMessage := inner.FlushMessage{ClientId: clientId, Total: total}
	message, err := inner.SerializeFlushMessage(flushMessage)
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
	sum.processedClientMessages[clientId] += 1
	return nil
}

func (sum *Sum) handleFlushMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	protocolMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing flush message", "err", err)
		return
	}

	flushMessage, ok := protocolMessage.(inner.FlushMessage)
	if !ok {
		slog.Error("Unexpected message type on flush queue", "clientId", protocolMessage.GetClientId())
		return
	}

	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if err := sum.flushClient(flushMessage.ClientId); err != nil {
		slog.Error("While flushing client", "err", err)
	}

	if err := sum.notifySumFinalization(flushMessage.ClientId, flushMessage.Total); err != nil {
		slog.Error("While notifying sum finalization", "err", err)
	}
}

func (sum *Sum) flushClient(clientId int) error {
	slog.Info("Flushing client", "clientId", clientId)
	clientFruitItemMap := sum.fruitItemMap[clientId]
	for key := range clientFruitItemMap {
		dataMessage := inner.DataMessage{ClientId: clientId, FruitRecords: []fruititem.FruitItem{clientFruitItemMap[key]}}
		message, err := inner.SerializeDataMessage(dataMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	delete(sum.fruitItemMap, clientId)
	return nil
}

func (sum *Sum) notifySumFinalization(clientId int, totalExpectedMessages int) error {
	processedMessageCount := sum.processedClientMessages[clientId]
	slog.Info("Received FIN message", "clientId", clientId, "processed", processedMessageCount)
	finMessage := inner.FinMessage{ClientId: clientId, Processed: processedMessageCount, Total: totalExpectedMessages}
	message, err := inner.SerializeFinMessage(finMessage)
	if err != nil {
		slog.Debug("While serializing fin message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending fin message", "err", err)
		return err
	}
	return nil
}
