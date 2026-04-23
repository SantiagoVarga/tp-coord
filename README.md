# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

---

## Informe Técnico: Coordinación y Escalabilidad 

Se redacta a continuación el siguiente informe el cual tiene como  fin explicar y justificar los cambios realizados al sistema provisto para cumplir con las especificaciones del enunciado y las características de un sistema distribuido.

### 1. Arquitectura de Coordinación entre Sum y Aggregation

#### 1.1 Problema Original y Solución Implementada

El esqueleto original presentaba dos limitaciones críticas en Sum:
- Solo una instancia de Sum recibía la notificación EOF del Gateway
- Todas las instancias enviaban broadcast a todas las Aggregations, causando redundancia

La solución implementa un **dual-channel messaging pattern**:

**Canal 1: Data Channel (Queue única)**
- Todas las instancias de Sum comparten una única queue de entrada (`INPUT_QUEUE`)
- Cada mensaje DATA se procesa por la instancia que lo recibe primero
- Evita duplicación de procesamiento puesto que el broker consume el mensaje

**Canal 2: Control Channel (Exchange broadcast)**
```python
# SUM_CONTROL_EXCHANGE permite que TODAS las instancias reciban EOF
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
SUM_CONTROL_ROUTING_KEY = f"{SUM_CONTROL_EXCHANGE}.broadcast"
```

Cuando el Gateway envía EOF:
1. Se publica en el control exchange
2. **Todas las instancias de Sum** lo reciben simultáneamente
3. Cada una procesa su estado independientemente (cliente_id como key)

```python
def process_control_message(self, message, ack, nack):
    msg_obj = self.message_handler.deserialize_control_message(message)
    
    if msg_obj.message_type == MessageType.EOF:
        # Todas las instancias ejecutan esto para TODOS los clientes
        self._process_eof(client_id, from_control=True)
```

#### 1.2 Hashing Consistente: Evitar Redundancia en Aggregation

En lugar de broadcast a todas las Aggregations, cada Sum envía datos a Aggregations selectivas usando **consistent hashing**:

```python
def _target_aggregation_index(self, fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

# Al finalizar, cada fruta va SOLO a su Aggregation target
for final_fruit_item in fruits_dict.values():
    target_aggregation = self.data_output_exchanges[
        self._target_aggregation_index(final_fruit_item.fruit)
    ]
    target_aggregation.send(
        self.message_handler.serialize_sum_message(
            [final_fruit_item.fruit, final_fruit_item.amount],
            client_id,
            correlation_id=f"{ID}:{final_fruit_item.fruit}",
        )
    )
```

**Ventajas:**
- Cada fruta se procesa exactamente una vez por Aggregation
- Determinístico: misma fruta → siempre la misma Aggregation
- Escalable: agregar Aggregations solo requiere recomputar hash, no reprocesar datos

#### 1.3 Coordinación Sum → Aggregation: EOF Broadcast

El único mensaje que se envía a TODAS las Aggregations es EOF:

```python
self.logger.info(f"Broadcasting EOF to Aggregations [client={client_id}]")
for data_output_exchange in self.data_output_exchanges:
    data_output_exchange.send(
        self.message_handler.serialize_eof_message(
            None,
            client_id,
            correlation_id=str(ID),
        )
    )
```

Esto es correcto porque:
- EOF es idempotente: contar N EOFs solo requiere agregar el contador
- Cada Aggregation sabe internamente cuántos Sum existen (`SUM_AMOUNT`)
- Solo cuando recibe N EOFs procede a calcular el top parcial

```python
# En Aggregation
def _process_eof(self, client_id, source_id=None):
    with self.lock:
        if client_id not in self.sums_received_by_client:
            self.sums_received_by_client[client_id] = 0
        
        self.sums_received_by_client[client_id] += 1
        should_send = self.sums_received_by_client[client_id] >= SUM_AMOUNT
    
    if should_send:
        self._send_top(client_id)  # Solo calcula partial top cuando conteo == SUM_AMOUNT
```

---

### 2. Proceso de Join: Consolidación de Resultados Parciales

#### 2.1 Flujo de Datos

Join recibe **AGGREGATION_AMOUNT** tops parciales (uno de cada Aggregation) y debe:
1. Consolidar los datos de todas las Aggregations
2. Esperar a que todas estén sincronizadas
3. Enviar el top final al Gateway

#### 2.2 Mecanismo de Coordinación Join

Join implementa un **coordination barrier** con dos tipos de messages:

**Datos:** Tops parciales desde Aggregations
```python
def _process_partial_top(self, fruit_top, client_id, source_id=None):
    with self.lock:
        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = {}
            self.aggs_received_by_client[client_id] = set()
        
        # Deduplicación: si ya recibimos de esta agregation, ignorar
        if source_id is not None and source_id in self.aggs_received_by_client[client_id]:
            return
        
        source_key = source_id if source_id is not None else f"legacy-{len(...)}"
        self.partial_tops_by_client[client_id][source_key] = list(fruit_top)
        self.aggs_received_by_client[client_id].add(source_key)
```

**Sincronización:** Signals desde Aggregations
```python
def _listen_coordination(self, agg_id):
    def coordination_callback(message, ack, nack):
        deserialized = self.message_handler.deserialize_coordination_message(message)
        if isinstance(deserialized, Message):
            client_id = deserialized.client_id
            with self.lock:
                if client_id not in self.aggs_finished_by_client:
                    self.aggs_finished_by_client[client_id] = set()
                
                source_id = deserialized.correlation_id if deserialized.correlation_id is not None else str(agg_id)
                self.aggs_finished_by_client[client_id].add(source_id)
                
                # Barrier: cuando TODAS las agregations señalizaron
                if len(self.aggs_finished_by_client[client_id]) == AGGREGATION_AMOUNT:
                    self.coordination_ready_by_client[client_id] = True
```

#### 2.3 Monitor de Finalización

Un thread dedicado monitorea la finalización y gatilla el envío del top final:

```python
def _monitor_completion(self):
    while not self.should_exit:
        clients_to_send = []
        
        with self.lock:
            for client_id in list(self.coordination_ready_by_client.keys()):
                # Condición: todas las aggregations coordinadas Y datos recibidos
                if (self.coordination_ready_by_client[client_id] and 
                    self.filter.get_aggs_received_count(client_id) >= AGGREGATION_AMOUNT):
                    clients_to_send.append(client_id)
        
        for client_id in clients_to_send:
            self.filter._send_final_top(client_id)
```

#### 2.4 Construcción del Top Final

```python
def _send_final_top(self, client_id):
    partial_tops = list(self.partial_tops_by_client.get(client_id, {}).values())
    amount_by_fruit = {}
    
    # Consolidar todos los tops parciales
    for partial_top in partial_tops:
        for fruit, amount in partial_top:
            current_amount = amount_by_fruit.get(fruit, 0)
            amount_by_fruit[fruit] = current_amount + amount
    
    # Tomar los TOP_SIZE más altos
    payload = sorted(
        [(fruit, amount) for fruit, amount in amount_by_fruit.items()],
        key=lambda fruit_record: (fruit_record[1], fruit_record[0]),
        reverse=True,
    )[:TOP_SIZE]
    
    # Enviar al Gateway
    final_msg = self.message_handler.serialize_final_top(payload, client_id)
    self.output_queue.send(final_msg)
```

---

### 3. Escalabilidad del Sistema

#### 3.1 Escalabilidad por Número de Clientes

**Cliente-Independencia:** Los datos de cada cliente se mantienen en diccionarios separados:

| Componente | Estructura | Complejidad |
|-----------|-----------|-----------|
| Sum | `amount_by_fruit_by_client: Dict[client_id, Dict[fruit, amount]]` | O(1) lookup por cliente |
| Aggregation | `fruit_top_by_client: Dict[client_id, List[FruitItem]]` | O(1) lookup por cliente |
| Join | `partial_tops_by_client: Dict[client_id, Dict[agg_index, top]]` | O(1) lookup por cliente |

**Resultado:** N clientes → N canales independientes sin contención. No hay mecanismo de serialización global.

#### 3.2 Escalabilidad por Número de Controles

**Sum instances:**
- Comparten una data queue → carga balanceada automáticamente por RabbitMQ
- Cada mensaje es consumido por UNA instancia (no duplicación)
- Comparten (suscripción) control exchange → todos reciben EOF
- Escalabilidad: O(1) agregar una nueva Sum, no requiere reconfiguración 

**Aggregation instances:**
- Cada una recibe datos de UNA fruta específica (consistent hashing)
- Si aumenta AGGREGATION_AMOUNT: nuevo hashing, datos se rebalancean automáticamente
- Escalabilidad: O(1) agregar una nueva Aggregation

**Join instance:**
- Recibe datos de TODAS las Aggregations
- Complejidad: O(AGGREGATION_AMOUNT) para consolidar tops
- No es un cuello de botella real porque:
  - El volumen de datos es TOP_SIZE × AGGREGATION_AMOUNT. Estas son variables que no escalan tan rápido (no harían falta tantos aggregators y el TOP_SIZE no debería ser muya grande para que hacer un top de frutas tuviera sentido).
  - Solo se ejecuta una vez por cliente
  - Altamente paralelizable por cliente (cada cliente es independiente)

#### 3.3 Latencia de Procesamiento

Para un cliente individual:
```
Gateway → Sum (O(datos)) → Aggregation (O(datos/AGGREGATION_AMOUNT)) 
  → Join (O(TOP_SIZE × AGGREGATION_AMOUNT)) → Gateway
```

Con N clientes concurrentes:
- Cada cliente sigue su propio camino sin contención
- Worst case: O(N × (tiempo_sum + tiempo_agg + tiempo_join))
- Best case: O(tiempo_sum + tiempo_agg + tiempo_join) 

---

### 4. Thread Safety y el Global Interpreter Lock (GIL)

#### 4.1 Por Qué los Threads Funcionan Correctamente

El GIL de Python es un mecanismo que permite que solo un thread ejecute bytecode Python a la vez. Sin embargo, **el sistema NO está limitado por este problema** por las siguientes razones:

#### 4.2 Punto Crítico: I/O Blocking Releases GIL

```python
class SumFilter:
    def start(self):
        data_thread = threading.Thread(
            target=self.input_queue.start_consuming,  # ← AQUÍ: I/O BLOQUEANTE
            daemon=False,
            name=f"SumDataListener{ID}",
        )
        control_thread = threading.Thread(
            target=self.control_input_exchange.start_consuming,  # ← AQUÍ: I/O BLOQUEANTE
            daemon=False,
            name=f"SumControlListener{ID}",
        )
```

Cuando un thread ejecuta `start_consuming()`:
1. Se bloquea esperando mensajes de RabbitMQ (system call)
2. El GIL se **libera** durante esta espera
3. Otros threads pueden ejecutar mientras uno está en I/O

**Cronograma de ejecución típico:**

```
Thread DataListener:   [I/O WAIT] ← GIL liberado
                       [Procesa DATA msg] [I/O WAIT]
                       
Thread ControlListener: [I/O WAIT] ← GIL liberado
                        [Procesa EOF msg] [I/O WAIT]
                        
Thread Main:           [Ejecuta lógica]
                       [Espera threads join()]
```

Raramente hay **contención de CPU** entre threads porque:
- `input_queue.start_consuming()` usa socket blocking (C code, no bytecode)
- `self.lock.acquire()` también libera el GIL si hay contención
- El procesamiento de cada mensaje es rápido (microsegundos)

#### 4.3 Usos de locks en el sistema

1. **Serializa acceso a estructuras mutables:**
   ```python
   with self.lock:  # ← Exclusión mutua
       self.amount_by_fruit_by_client[client_id] = {...}
   ```

2. **Protege máquinas de estado:**
   ```python
   with self.lock:
       if client_id in self.completed_clients:  # ← No duplicamos procesamiento
           return
       self.finalizing_clients.add(client_id)
   ```

3. **Evita race conditions típicas:**
   - Check-then-act (verificar si está en un set, luego agregarlo)
   - Modificar diccionarios compartidos
   - Contadores (aunque con GIL, la mayoría son atómicas, el lock es defensiva)

#### 4.4 Por Qué Este Patrón es Superior

En lugar de threads CPU-bound (que sufren con GIL), se implementó **threads I/O-bound**:

| Característica | CPU-Bound | I/O-Bound  |
|---|---|---|
| GIL Impact | Alto (competencia por ejecución) | Bajo (GIL liberado durante I/O) |
| Caso de uso | Cálculos pesados | Esperar red/mensajes |
| Escalabilidad | Limitada a #cores | Muy escalable |
| TP-Coord | ❌ | ✅ |

Cada componente (Sum, Aggregation, Join) es fundamentalmente **I/O-bound**:
- Esperan mensajes de RabbitMQ
- Procesan datos rápidamente
- Envían respuestas

---

### 5. Manejo de Señales y Shutdown Ordenado

Todos los worker processes implementan signal handlers para un **graceful shutdown**:

```python
def _setup_signal_handlers(self):
    def handle_signal(signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down SumWorker")
        self.filter.stop()  # ← Detener consumo
    
    signal.signal(signal.SIGTERM, handle_signal)

def run(self):
    try:
        self.filter.start()
    finally:
        self.filter.close()  # ← Cerrar conexiones
```

Esto asegura que:
1. Se reciben señales del orquestador (Docker, supervisores)
2. Se detiene el consumo de nuevos mensajes
3. Se cierran conexiones a RabbitMQ
4. Se liberan recursos

---

### Conclusión

La implementación logra coordinación eficiente mediante:

1. **Dual-channel messaging** para sincronizar todas las instancias Sum
2. **Consistent hashing** para distribuir carga sin redundancia
3. **Coordination barriers** en Join para consolidar resultados
4. **Thread-safe state machines** con locks explícitos
5. **I/O-bound concurrency** que funciona correctamente con Python GIL
6. **Escalabilidad lineal** en clientes e instancias de procesamiento

El sistema está preparado para cambios dinámicos en la configuración sin reimplentar la lógica de negocio.
