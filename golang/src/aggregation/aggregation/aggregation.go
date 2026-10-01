package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	fruitItemMap          map[int]map[string]fruititem.FruitItem
	topSize               int
	processedClientSums   map[int]int
	totalExpectedMessages map[int]int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		fruitItemMap:          map[int]map[string]fruititem.FruitItem{},
		topSize:               config.TopSize,
		processedClientSums:   map[int]int{},
		totalExpectedMessages: map[int]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	protocolMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch message := protocolMessage.(type) {
	case inner.DataMessage:
		aggregation.handleDataMessage(message.ClientId, message.FruitRecords)
	case inner.FinMessage:
		if err := aggregation.handleSumFinalizationMessage(message.ClientId, message.Processed, message.Total); err != nil {
			slog.Error("While handling sum finalization message", "err", err)
		}
	default:
		slog.Error("Unexpected message type on aggregation input", "clientId", message.GetClientId())
	}
}

func (aggregation *Aggregation) handleSumFinalizationMessage(clientId int, processed int, total int) error {
	slog.Info("Received sum finalization message", "clientId", clientId, "processed", processed, "total", total)
	aggregation.processedClientSums[clientId] += processed
	aggregation.totalExpectedMessages[clientId] = total
	clientFruitItemMap := aggregation.fruitItemMap[clientId]
	fruitTopRecords := aggregation.buildFruitTop(clientFruitItemMap)
	topMessage := inner.TopMessage{ClientId: clientId, TopRecords: fruitTopRecords}
	processedCount := aggregation.processedClientSums[clientId]
	totalCount := aggregation.totalExpectedMessages[clientId]
	if processedCount == totalCount {
		slog.Info("All sums processed for client", "clientId", clientId)
		message, err := inner.SerializeTopMessage(topMessage)
		if err != nil {
			slog.Debug("While serializing top message", "err", err)
			return err
		}
		if err := aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)
			return err
		}

		delete(aggregation.fruitItemMap, clientId)
		delete(aggregation.processedClientSums, clientId)
		delete(aggregation.totalExpectedMessages, clientId)
	} else {
		slog.Info("Not all sums processed for client", "clientId", clientId, "processedCount", processedCount, "totalCount", totalCount)
	}

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId int, fruitRecords []fruititem.FruitItem) {
	clientFruitItemMap, ok := aggregation.fruitItemMap[clientId]
	if !ok {
		clientFruitItemMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMap[clientId] = clientFruitItemMap
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := clientFruitItemMap[fruitRecord.Fruit]; ok {
			clientFruitItemMap[fruitRecord.Fruit] = clientFruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientFruitItemMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruitItemMap))
	for _, item := range clientFruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
