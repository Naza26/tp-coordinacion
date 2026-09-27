package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	rabbitMQ, err := InitializeRabbitWQ(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}
	return rabbitMQ, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	rabbitExchange, err := InitializeRabbitExchange(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}
	return rabbitExchange, nil
}
