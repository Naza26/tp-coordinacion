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
