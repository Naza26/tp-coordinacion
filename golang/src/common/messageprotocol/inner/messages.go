package inner

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type ProtocolMessage interface {
	GetClientId() int
}

type DataMessage struct {
	ClientId     int
	FruitRecords []fruititem.FruitItem
}

func (message DataMessage) GetClientId() int {
	return message.ClientId
}
func SerializeDataMessage(message DataMessage) (*middleware.Message, error) {
	return toMiddlewareMessage([]interface{}{DataType, message.ClientId, serializeFruitRecords(message.FruitRecords)})
}
func deserializeDataMessage(data []interface{}) (DataMessage, error) {
	if len(data) != 3 {
		return DataMessage{}, errors.New("Data message is not a (type, client id, records) triple")
	}
	clientId, err := deserializeNumber(data[1])
	if err != nil {
		return DataMessage{}, err
	}
	fruitRecords, err := deserializeFruitRecords(data[2])
	if err != nil {
		return DataMessage{}, err
	}
	return DataMessage{ClientId: clientId, FruitRecords: fruitRecords}, nil
}

type EofMessage struct {
	ClientId int
	Total    int
}

func (message EofMessage) GetClientId() int {
	return message.ClientId
}
func SerializeEofMessage(message EofMessage) (*middleware.Message, error) {
	return toMiddlewareMessage([]interface{}{EofType, message.ClientId, message.Total})
}
func deserializeEofMessage(data []interface{}) (EofMessage, error) {
	if len(data) != 3 {
		return EofMessage{}, errors.New("EOF message is not a (type, client id, total) triple")
	}
	clientId, err := deserializeNumber(data[1])
	if err != nil {
		return EofMessage{}, err
	}
	total, err := deserializeNumber(data[2])
	if err != nil {
		return EofMessage{}, err
	}
	return EofMessage{ClientId: clientId, Total: total}, nil
}

type FlushMessage struct {
	ClientId int
	Total    int
}

func (message FlushMessage) GetClientId() int {
	return message.ClientId
}

func SerializeFlushMessage(message FlushMessage) (*middleware.Message, error) {
	return toMiddlewareMessage([]interface{}{FlushType, message.ClientId, message.Total})
}

func deserializeFlushMessage(data []interface{}) (FlushMessage, error) {
	if len(data) != 3 {
		return FlushMessage{}, errors.New("Flush message is not a (type, client id, total) triple")
	}
	clientId, err := deserializeNumber(data[1])
	if err != nil {
		return FlushMessage{}, err
	}
	total, err := deserializeNumber(data[2])
	if err != nil {
		return FlushMessage{}, err
	}
	return FlushMessage{ClientId: clientId, Total: total}, nil
}

type FinMessage struct {
	ClientId  int
	Processed int
	Total     int
}

func (message FinMessage) GetClientId() int {
	return message.ClientId
}
func SerializeFinMessage(message FinMessage) (*middleware.Message, error) {
	return toMiddlewareMessage([]interface{}{FinType, message.ClientId, message.Processed, message.Total})
}
func deserializeFinMessage(data []interface{}) (FinMessage, error) {
	if len(data) != 4 {
		return FinMessage{}, errors.New("Fin message is not a (type, client id, processed, total) tuple")
	}
	clientId, err := deserializeNumber(data[1])
	if err != nil {
		return FinMessage{}, err
	}
	processed, err := deserializeNumber(data[2])
	if err != nil {
		return FinMessage{}, err
	}
	total, err := deserializeNumber(data[3])
	if err != nil {
		return FinMessage{}, err
	}
	return FinMessage{ClientId: clientId, Processed: processed, Total: total}, nil
}

type TopMessage struct {
	ClientId   int
	TopRecords []fruititem.FruitItem
}

func (message TopMessage) GetClientId() int {
	return message.ClientId
}
func SerializeTopMessage(message TopMessage) (*middleware.Message, error) {
	return toMiddlewareMessage([]interface{}{TopType, message.ClientId, serializeFruitRecords(message.TopRecords)})
}
func deserializeTopMessage(data []interface{}) (TopMessage, error) {
	if len(data) != 3 {
		return TopMessage{}, errors.New("Top message is not a (type, client id, records) triple")
	}
	clientId, err := deserializeNumber(data[1])
	if err != nil {
		return TopMessage{}, err
	}
	topRecords, err := deserializeFruitRecords(data[2])
	if err != nil {
		return TopMessage{}, err
	}
	return TopMessage{ClientId: clientId, TopRecords: topRecords}, nil
}
