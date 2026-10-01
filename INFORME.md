Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

# Escenario 2: Múltiples clientes

Cubierto en los diagrams de excalidraw. Detallar después.

Dudas: Si nunca recibo un EOF de parte del cliente, nunca puedo limpiar el mapa y se seguiría acumulando infinitamente en memoria, tampoco mandaría nunca esos datos al aggregator asi que quedaría bloqueado para ese cliente. Pensar cómo y si tengo que manejar esta situación.

Tuve que agregar la instanciación del contador que me genera los client ids de manera global en message handler porque no puedo modificar la interfaz del gateway, pero el lugar más prolijo sería definirlo ahí, donde se maneja la conexión de cada cliente y pasarselo al handler de cada mensaje.

# Escenario 3: Múltiples clientes + Múltiples Nodos de Suma

El primer problema que tengo es que al tener más de un nodo de suma, solo una de las instancias recibe el EOF, que era la señal que nos permitía envíar toda la información al nodo de aggregate.
Esto hace que solo uno de los nodos sepa que tiene que envíar información sobre ese client_id mientras que los demas nodos estarían teniendo en sus diccionarios información que nunca se envía al aggregator.
Para resolver esto, crearía un nuevo mensaje, que le de aviso al resto de los nodos de suma que tienen que envíar los datos al aggregator y limpiar así sus diccionarios.
Para que el mensaje le llegue a todas los nodos de suma, creo un exchange que le asocie una cola a cada nodo para que puedan recibir efectivamente esta señal.
Para mantener consistencia entre los nodos de suma, el que recibe el EOF no envía la data directamente, también espera el mensaje de flush para seguir el mismo camino.
Como ahora cada nodo de suma consume de dos colas en go rotuines, creo un mutex para proteger el diccionario (que tiene los datos), dado que uno puede estar leyendolo mientras otro puede estar borrando (flush).


El segundo problema es que el aggregator recibe resultados parciales de un mismo cliente desde varios nodos suma, en cualquier orden, y no sabe cuándo ya tiene toda la información de ese cliente.
Para saberlo, cuento mensajes. El gateway cuenta cuántos mensajes de datos envió cada cliente (total) y lo manda en el EOF, el flush lleva ese total a todos los nodos suma. Cada nodo suma cuenta cuántos mensajes de entrada procesó de ese cliente (processed) y lo envía al aggregator en su mensaje de fin, junto con total.

El tercer problema es que RabbitMQ solo garantiza el orden dentro de una misma cola pero no entre colas distintas.
Un mensaje de datos puede salir del input_queue antes que el EOF, pero el flush viaja por otra cola (la de cada nodo suma) y puede llegarle antes a un nodo suma que ese mensaje de datos. Ese nodo envía sus parciales y su fin sin incluirlo, y su diccionario parece completo aunque no lo está realmente.
Por eso no alcanza con esperar un fin de cada nodo suma, necesito saber cuántos mensajes tengo que esperar. Para saberlo, cuento mensajes. El gateway cuenta cuántos mensajes de datos envió cada cliente (total) y lo manda en el EOF, el flush lleva ese total a todos los nodos suma. Cada nodo suma cuenta cuántos mensajes de entrada procesó de ese cliente (processed) y lo envía al aggregator en su mensaje de fin, junto con total.
El aggregator suma los processed que recibe de cada cliente. Cuando sum(processed) == total, recibió toda la información de ese cliente, ahí calcula el top N, lo envía al join y borra el estado de ese cliente.
Esto no depende de cuántos nodos suma haya ni del orden en que lleguen los mensajes.