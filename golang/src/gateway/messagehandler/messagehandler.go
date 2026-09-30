package messagehandler

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SafeClientIdGenerator struct {
	mutex sync.Mutex
	value int
}

func (counter *SafeClientIdGenerator) GenerateId() int {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()
	clientId := counter.value
	counter.value++
	return clientId
}

var clientIdCounter = SafeClientIdGenerator{value: 1}

type MessageHandler struct {
	clientId int
}

func NewMessageHandler() MessageHandler {
	clientId := clientIdCounter.GenerateId()
	messageHandler := MessageHandler{clientId}
	return messageHandler
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	fruitData := []fruititem.FruitItem{fruitRecord}
	clientId := messageHandler.clientId
	return inner.SerializeMessage(clientId, fruitData)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	clientId := messageHandler.clientId
	return inner.SerializeMessage(clientId, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientId != messageHandler.clientId {
		return nil, nil
	}
	if isEof { // TODO: Think how to handle [id, []] (empty top?)
		return nil, nil
	}
	return fruitRecords, nil
}
