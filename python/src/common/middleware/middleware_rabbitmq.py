import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,  
    MessageMiddlewareMessageError,       
    MessageMiddlewareCloseError          
)
class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.host = host
            self.queue_name = queue_name
            self._consuming = False
            self.consumer_tag = None
            # Establece conexión y canal. Agrega parámetros de heartbeat y timeout para detectar desconexiones.
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, heartbeat=600, blocked_connection_timeout=300))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)  # Para asegurar que un consumidor no reciba más de un mensaje a la vez
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error al conectar a RabbitMQ en {self.host}: {str(e)}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar MessageMiddlewareQueueRabbitMQ {self.queue_name}: {str(e)}") from e



    def send(self, message):
        try:
            if isinstance(message, str):
                message = message.encode()

            if not self.connection.is_open or not self.channel.is_open:
                raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
                
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)  
            )
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al enviar mensaje a RabbitMQ: {str(e)}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje a RabbitMQ: {str(e)}") from e

    
    def start_consuming(self, callback):
        try:
            if not self.connection.is_open or not self.channel.is_open:
                raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
            self._consuming = True
            def on_message(channel, method, _, body):
                try:
                    def ack():
                        try:
                            channel.basic_ack(delivery_tag=method.delivery_tag)
                        except Exception as e:
                            raise MessageMiddlewareMessageError(f"Error al hacer ack del mensaje: {str(e)}") from e
                    def nack():
                        try:
                            channel.basic_nack(delivery_tag=method.delivery_tag)
                        except Exception as e:
                            raise MessageMiddlewareMessageError(f"Error al hacer nack del mensaje: {str(e)}") from e
                    callback(body, ack, nack)

                    if not self._consuming:
                        channel.stop_consuming()
                except Exception as e:
                    try:
                        channel.basic_nack(delivery_tag=method.delivery_tag)
                    except:
                        pass
                    raise MessageMiddlewareMessageError(
                        f"Error en callback de consumidor: {str(e)}"
                    ) from e
                

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message,
                auto_ack=False
            )
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                pass

        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Desconectado de RabbitMQ durante consumo: {str(e)}"
            ) from e
        except Exception as e:
            if not self._consuming:
                return 
            raise MessageMiddlewareMessageError(
                f"Error en start_consuming: {str(e)}"
            ) from e
        finally:
            self._consuming = False    


    def stop_consuming(self):
        # Señal para terminar el loop de consumo
        try:
            self._consuming = False
            if self.consumer_tag is None:
                return
            if not self.connection.is_open or not self.channel.is_open:
                    raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
            self.channel.basic_cancel(self.consumer_tag)
            self.channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Desconectado de RabbitMQ durante stop_consuming: {str(e)}"
            ) from e
        except Exception as e:
            # Lanzar excepcion aqui puede causar bloqueo si el consumidor 
            # no esta activo. Solo logear
            pass
        

    def close(self):
        try:
            # Evitar lanzar excepciones durante el cierre 
            # para asegurar que se intenten cerrar todos los recursos
            if self._consuming:
                try:
                    self.stop_consuming()
                except:
                    pass
                    
            if hasattr(self, 'channel') and self.channel is not None and self.channel.is_open:
                try:
                    self.channel.close()
                except pika.exceptions.AMQPError:
                    pass  # Ya está desconectado, no hay nada que cerrar

            if hasattr(self, 'connection') and self.connection is not None and self.connection.is_open:
                try:
                    self.connection.close()
                except pika.exceptions.AMQPError:
                    pass  
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar conexión con RabbitMQ: {str(e)}") from e
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.host = host
            self.exchange_name = exchange_name
            self._consuming = False
            self.consumer_tag = None
            self.routing_keys = routing_keys
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,heartbeat=600, blocked_connection_timeout=300))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)  # Para asegurar que un consumidor no reciba más de un mensaje a la vez
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
            
            # Declarar una cola anónima para recibir mensajes
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue
            
            # Vincular la cola a cada routing key
            if isinstance(self.routing_keys, list):
                for key in self.routing_keys:
                    self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=key)
            else:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.routing_keys)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error al conectar a RabbitMQ en {self.host}: {str(e)}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar MessageMiddlewareExchangeRabbitMQ {self.exchange_name}: {str(e)}") from e


    def send(self, message):
        try:
            # Usar la primera routing_key de la instancia
            if isinstance(self.routing_keys, list):
                routing_key = self.routing_keys[0]
            else:
                routing_key = self.routing_keys
            # Asegurar que el mensaje es bytes
            if isinstance(message, str):
                message = message.encode()

            if not self.connection.is_open or not self.channel.is_open:
                raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Desconectado al enviar mensaje a RabbitMQ: {str(e)}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje a {self.exchange_name}: {str(e)}") from e



    def start_consuming(self, callback):
        try:
            if not self.connection.is_open or not self.channel.is_open:
                raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
            self._consuming = True
            def on_message(channel, method, _, body):
                try:
                    def ack():
                        try:
                            channel.basic_ack(delivery_tag=method.delivery_tag)
                        except Exception as e:
                            raise MessageMiddlewareMessageError(f"Error al hacer ack del mensaje: {str(e)}") from e
                    def nack():
                        try:
                            channel.basic_nack(delivery_tag=method.delivery_tag)
                        except Exception as e:
                            raise MessageMiddlewareMessageError(f"Error al hacer nack del mensaje: {str(e)}") from e
                    callback(body, ack, nack) if callback.__code__.co_argcount == 3 else callback(body, method.routing_key, ack, nack)
                    if not self._consuming:
                        channel.stop_consuming()
                except Exception as e:
                    try:
                        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    except:
                        pass
                    raise MessageMiddlewareMessageError(f"Error en callback de consumidor: {str(e)}") from e
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message,
                auto_ack=False
            )
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                pass
        except MessageMiddlewareDisconnectedError:
            raise
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Desconectado de RabbitMQ durante consumo: {str(e)}") from e
        except Exception as e:
            if not self._consuming:
                return
            raise MessageMiddlewareMessageError(f"Error en start_consuming: {str(e)}") from e
        finally:            self._consuming = False
    
    def stop_consuming(self):
        try: 
            self._consuming = False
            if self.consumer_tag is None:
                return
            if not self.connection.is_open or not self.channel.is_open:
                    raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ está cerrada")
            self.channel.basic_cancel(self.consumer_tag)
            self.channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except Exception:
            pass # Lanzar una excepción aquí puede causar bloqueo 
                 # si el consumidor no está activo. Solo loguear.

    def close(self):
        try:
            # Evitar lanzar excepciones durante el cierre 
            # para asegurar que se intenten cerrar todos los recursos
            if self._consuming:
                try:
                    self.stop_consuming()
                except:
                    pass
            
            if hasattr(self, 'channel') and self.channel is not None and self.channel.is_open:
                try:
                    self.channel.close()
                except pika.exceptions.AMQPError:
                    pass  # Ya está desconectado, no hay nada que cerrar
            
           
            if hasattr(self, 'connection') and self.connection is not None and self.connection.is_open:
                try:
                    self.connection.close()
                except pika.exceptions.AMQPError:
                    pass  # Ya está desconectado, no hay nada que cerrar
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar conexión con RabbitMQ: {str(e)}") from e