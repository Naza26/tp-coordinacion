package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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

func DeserializeMessage(message *middleware.Message) (int, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return 0, nil, false, err
	}

	if len(data) != 2 {
		return 0, nil, false, errors.New("Message is not a (client id, records) pair")
	}

	rawId, ok := data[0].(float64)
	if !ok {
		return 0, nil, false, errors.New("Datum is not an id number")
	}
	clientId := int(rawId)
	rawFruitRecords, ok := data[1].([]interface{})
	if !ok {
		return 0, nil, false, errors.New("Fruit Records is not an array")
	}
	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range rawFruitRecords {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return 0, nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return 0, nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	isEof := len(fruitRecords) == 0
	return clientId, fruitRecords, isEof, nil
}
