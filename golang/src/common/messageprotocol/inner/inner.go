package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	DataType  = "DATA"
	EofType   = "EOF"
	FlushType = "FLUSH"
	FinType   = "FIN"
	TopType   = "TOP"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func toMiddlewareMessage(fields []interface{}) (*middleware.Message, error) {
	body, err := serializeJson(fields)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}
func deserializeNumber(raw interface{}) (int, error) {
	rawFloat, ok := raw.(float64)
	if !ok {
		return 0, errors.New("Datum is not a number")
	}
	return int(rawFloat), nil
}
func serializeFruitRecords(records []fruititem.FruitItem) []interface{} {
	data := []interface{}{}
	for _, fruitRecord := range records {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}
	return data
}
func deserializeFruitRecords(raw interface{}) ([]fruititem.FruitItem, error) {
	rawFruitRecords, ok := raw.([]interface{})
	if !ok {
		return nil, errors.New("Fruit Records is not an array")
	}
	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range rawFruitRecords {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}
	return fruitRecords, nil
}

func SerializeMessage(clientId int, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	messageData := []interface{}{clientId, data}

	body, err := serializeJson(messageData)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (ProtocolMessage, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("Message is empty")
	}
	messageType, ok := data[0].(string)
	if !ok {
		return nil, errors.New("Message type is not a string")
	}

	switch messageType {
	case DataType:
		return deserializeDataMessage(data)
	case EofType:
		return deserializeEofMessage(data)
	case FlushType:
		return deserializeFlushMessage(data)
	case FinType:
		return deserializeFinMessage(data)
	case TopType:
		return deserializeTopMessage(data)
	default:
		return nil, errors.New("Unknown message type")
	}
}
