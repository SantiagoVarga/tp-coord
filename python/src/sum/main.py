import os
import logging
import signal
from threading import Lock
import time
import zlib

import threading
from collections import defaultdict

from common import middleware, fruit_item
from common.message_protocol.internal import Message,MessageError, MessageType
from gateway.message_handler.message_handler import MessageHandler

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_CONTROL_ROUTING_KEY = f"{SUM_CONTROL_EXCHANGE}.broadcast"

class SumFilter:
    def __init__(self, message_handler):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )
        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )
        self.message_handler = message_handler
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client = {}
        self.active_data_messages_by_client = defaultdict(int)
        self.finished_clients = set()
        self.finalizing_clients = set()
        self.lock = Lock()
        self.logger = logging.getLogger("SumFilter")

    def _target_aggregation_index(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _process_data(self, fruit, amount,client_id):
        with self.lock:
            if client_id in self.finished_clients:
                return
            if client_id not in self.amount_by_fruit_by_client:
                self.amount_by_fruit_by_client[client_id] = {}
                self.logger.info(f"New client data stream [client={client_id}]")
            
            fruits_dict = self.amount_by_fruit_by_client[client_id]
            
            if fruit in fruits_dict:
                fruits_dict[fruit] = fruits_dict[fruit] + fruit_item.FruitItem(fruit, int(amount))
            else:
                fruits_dict[fruit] = fruit_item.FruitItem(fruit, int(amount))
            
    def _wait_for_client_idle(self, client_id):
        while True:
            with self.lock:
                if self.active_data_messages_by_client.get(client_id, 0) == 0:
                    return
            time.sleep(0.01)

    def _finalize_client(self, client_id):
        with self.lock:
            if client_id in self.finished_clients or client_id in self.finalizing_clients:
                return False
            self.finalizing_clients.add(client_id)
            fruits_dict = dict(self.amount_by_fruit_by_client.get(client_id, {}))

        self.logger.info(f"Broadcasting {len(fruits_dict)} aggregated items to Aggregations [client={client_id}]")

        try:
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

            self.logger.info(f"Broadcasting EOF to Aggregations [client={client_id}]")
            for data_output_exchange in self.data_output_exchanges:
                 data_output_exchange.send(
                    self.message_handler.serialize_eof_message(
                        None,
                        client_id,
                        correlation_id=str(ID),
                    )
                 )
        except Exception:
            with self.lock:
                self.finalizing_clients.discard(client_id)
            raise

        with self.lock:
            self.finalizing_clients.discard(client_id)
            self.finished_clients.add(client_id)
            self.amount_by_fruit_by_client.pop(client_id, None)
        
        return True

    def _process_eof(self,client_id,from_control=False):
        if from_control:
            self._wait_for_client_idle(client_id)
            self._finalize_client(client_id)
            return
        
        self._wait_for_client_idle(client_id)
        if self._finalize_client(client_id):
            self.control_output_exchange.send(
                self.message_handler.serialize_eof_message(
                    None,
                    client_id,
                    correlation_id=str(ID),
                )
            )
       

            

    def process_data_message(self, message, ack, nack):
        try:
            msg_obj = self.message_handler.deserialize_data_message(message)
            
            if msg_obj is None:
                ack()
                return
            
            client_id = msg_obj.client_id
            
            if msg_obj.message_type == MessageType.DATA:
                with self.lock:
                    if client_id in self.finished_clients:
                        ack()
                        return
                    self.active_data_messages_by_client[client_id] += 1
                try:
                    if not isinstance(msg_obj.payload, (list, tuple)) or len(msg_obj.payload) != 2:
                        raise MessageError(f"Invalid DATA payload: {msg_obj.payload}")
                    
                    fruit, amount = msg_obj.payload
                    self.logger.debug(f"Sum {ID} received DATA [{fruit}, {amount}] [client={client_id}]")
                    self._process_data(fruit, amount, client_id)
                finally:
                    with self.lock:
                        self.active_data_messages_by_client[client_id] -= 1
                        if self.active_data_messages_by_client[client_id] <= 0:
                            self.active_data_messages_by_client.pop(client_id, None)
            
            elif msg_obj.message_type == MessageType.EOF:
                self.logger.info(f"Sum {ID} received EOF [client={client_id}]")
                self._process_eof(client_id)
            else:
                self.logger.warning(f"Sum {ID} received unknown message type: {msg_obj.message_type} [client={client_id}]")
            ack()
        
        except Exception as e:
            self.logger.error(f"Error processing message on Sum {ID}: {e}")
            nack()


    def process_control_message(self, message, ack, nack):
        try:
            msg_obj = self.message_handler.deserialize_control_message(message)
            
            if msg_obj is None:
                ack()
                return
            
            client_id = msg_obj.client_id
            
            if msg_obj.message_type == MessageType.EOF:
                self.logger.info(f"Sum {ID} received control EOF [client={client_id}]")
                self._process_eof(client_id, from_control=True)
            
            ack()
        
        except Exception as e:
            self.logger.error(f"Error processing control message on Sum {ID}: {e}")
            nack()

    def start(self):
        def on_data_message(message, ack, nack):
            self.process_data_message(message, ack, nack)
        def on_control_message(message, ack, nack):
            self.process_control_message(message, ack, nack)
        data_thread = threading.Thread(
            target=self.input_queue.start_consuming,
            args=(on_data_message,),
            daemon=False,
            name=f"SumDataListener{ID}",
        )
        control_thread = threading.Thread(
            target=self.control_input_exchange.start_consuming,
            args=(on_control_message,),
            daemon=False,
            name=f"SumControlListener{ID}",
        )
        data_thread.start()
        control_thread.start()
        data_thread.join()
        control_thread.join()

    def stop(self):
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            self.logger.error(f"Error stopping input queue consumption: {e}")
        try:
            self.control_input_exchange.stop_consuming()
        except Exception as e:
            self.logger.error(f"Error stopping control input exchange consumption: {e}")
     
    
    def close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            self.logger.error(f"Error closing input queue: {e}")
        try:
            self.control_input_exchange.close()
        except Exception as e:
            self.logger.error(f"Error closing control input exchange: {e}")
        try:
            self.control_output_exchange.close()
        except Exception as e:
            self.logger.error(f"Error closing control output exchange: {e}")
        for i, exchange in enumerate(self.data_output_exchanges):
            try:
                exchange.close()
            except Exception as e:
                self.logger.error(f"Error closing data output exchange {i}: {e}")

class SumWorker:
    def __init__(self, sum_id):
        self.sum_id = sum_id
        self.message_handler = MessageHandler()
        self.filter = SumFilter(self.message_handler)
        self.logger = logging.getLogger(f"SumWorker{sum_id}")
        self._setup_signal_handlers()

    
    def _setup_signal_handlers(self):
        def handle_signal(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down SumWorker {self.sum_id}")
            try:
                self.filter.stop()
            except Exception as e:
                self.logger.error(f"Error closing filter: {e}")
        signal.signal(signal.SIGTERM, handle_signal)

    def run(self):
        self.logger.info(f"Starting SumWorker {self.sum_id}")
        
        try:
            self.filter.start()
            
            self.logger.info(f"SumWorker {self.sum_id} finished")
            return 0
        
        except Exception as e:
            self.logger.error(f"Error in SumWorker {self.sum_id}: {e}")
            return 1
        
        finally:
            self.logger.info(f"SumWorker {self.sum_id} cleaning up")
            self.filter.close()

        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_worker = SumWorker(ID)
    sum_worker.run()
    return 0


if __name__ == "__main__":
    main()
