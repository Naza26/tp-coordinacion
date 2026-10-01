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


El cuarto problema es que el mensaje que estaba en tránsito le llega a un nodo suma después de que ese nodo ya hizo el flush de ese cliente.
Si el nodo lo guarda en su diccionario como cualquier otro mensaje, queda ahí para siempre, porque no va a llegar un segundo flush para ese cliente. Entonces el aggregator nunca llega a sum(processed) == total, el cliente nunca recibe su resultado y ese estado nunca se libera de la memoria del nodo suma.
Además, con el diccionario solo no puedo distinguir si es el primer mensaje de un cliente que todavía no hizo flush (y lo tengo que guardar) o si es un mensaje de un cliente al que ya le hice flush (y lo tengo que enviar), porque en el flush borro la entrada de ese cliente.
Para resolver esto, resolví que cada nodo suma guarda los clientes a los que ya les hizo flush junto con su total (flushedClients). Lo marco dentro del flush, con el mismo mutex, para que ningún mensaje de datos pueda procesarse entre que envío los parciales y marco al cliente.
Cuando llega un mensaje de datos de un cliente que ya está en flushedClients, no lo guardo sinoq ue lo envío directamente al aggregator y después envío un mensaje de fin con processed = 1 y el total de ese cliente.
El aggregator no cambia, suma ese 1 a los processed que ya tenía, llega a total y calcula el top. Como el mensaje de datos y el de fin salen del mismo nodo suma hacia la misma cola del aggregator, llegan en orden, así que el aggregator siempre recibe los datos antes que el fin.
Decidí no borrar nunca flushedClients, porque es solo un número por cliente asi que el costo en memoria no es alto pero lo más porolijo sería manejar eso.

# Escenario 4: Múltiples clients + Múltiples Nodos de suma + Múltiples nodos de agregación

Lamentablemente llegué muy justa a la entrega por lo cual, no tengo tiempo de terminar de implementar la solución. Si bien todos los tests me están pasando, no estoy escalando con respecto a múltiples nodos de agregación.
Quiero plantear alto nivel acá el problema y cómo lo solucionaría en caso de que valga de algo para la entrega y para formalizar que soy consciente de que no está escalando.

En este momento, todos los nodos de agregación esán recibiendo los datos. Cada uno está procesando el tope correctamente pero haciendo el mismo trabajo y luego delegándoselo al nodo de join.
Entiendo que en la práctica esto no es relevante porque ni bien el gateway recibe un tope correcto lo escribe a output y se deben descartar los otros resultados.

Debería definir un criterio de separación.
Podría ser por dato o por cliente. En este momento no puedo hacer bien un tradeoff sobre las ventajas / desventajas de cada uno de los enfoques más que comentar que el particionamiento por dato a priori me parece más correcto pero a la vez más complejo, porque de nuevo, tengo ahora en este nodo un pedazo de los datos de cada cliente y tengo que pasarle la responsibilidad de joinear eso al otro nodo, al de join, porque siempre tiene que haber un funnel que agrupe la información que sigo particionando, sino nunca se encontraría para realizar el cáluclo final.

# Escenario 5: Nombres al azar

No tengo ningún valor harcodeado por lo que entiendo que no me encuentro con este problema directamente. De todos modos vale la pena aclarar que no tengo implementada la replicación de los nodos de agregación pero no vería el problema incluso con eso implementado de sufrir algún cambio en este escenario.