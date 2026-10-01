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
	dataMessage := inner.DataMessage{ClientId: messageHandler.clientId, FruitRecords: []fruititem.FruitItem{fruitRecord}}
	return inner.SerializeDataMessage(dataMessage)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	eofMessage := inner.EofMessage{ClientId: messageHandler.clientId}
	return inner.SerializeEofMessage(eofMessage)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	protocolMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	topMessage, ok := protocolMessage.(inner.TopMessage)
	if !ok || topMessage.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return topMessage.TopRecords, nil
}
