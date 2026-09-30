Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Escenario 2: Múltiples clientes

Cubierto en los diagrams de excalidraw. Detallar después.

Dudas: Si nunca recibo un EOF de parte del cliente, nunca puedo limpiar el mapa y se seguiría acumulando infinitamente en memoria, tampoco mandaría nunca esos datos al aggregator asi que quedaría bloqueado para ese cliente. Pensar cómo y si tengo que manejar esta situación.