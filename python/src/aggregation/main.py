import os
import logging
import bisect
import signal
from collections import defaultdict
from threading import Lock
from common import middleware,  fruit_item

from gateway.message_handler.message_handler import MessageHandler
from common.message_protocol.internal import MessageError, MessageType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

AGGREGATION_CONTROL_EXCHANGE = "AGGREGATION_CONTROL_EXCHANGE"
ALL_SUMS_FINISHED = "ALL_SUMS_FINISHED"
JOINER_COORDINATION_QUEUE = "JOINER_COORDINATION_QUEUE"


class AggregationFilter:

    def __init__(self,message_handler):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.joiner_coordination_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{JOINER_COORDINATION_QUEUE}_{ID}"
        )

        self.message_handler = message_handler

        self.fruit_top_by_client = {}
        self.sums_received_by_client = {}
        self.received_partial_sums_by_client = defaultdict(set)
        self.received_eofs_by_client = defaultdict(set)
        self.completed_clients = set()  
        self.finalizing_clients = set()
        self.lock = Lock()

    def _process_data(self, fruit, amount, client_id, message_id=None):
        with self.lock:
            if client_id in self.completed_clients:
                return
            if message_id is not None:
                if message_id in self.received_partial_sums_by_client[client_id]:
                    return
                self.received_partial_sums_by_client[client_id].add(message_id)
            if client_id not in self.fruit_top_by_client:
                self.fruit_top_by_client[client_id] = []
                logging.info(f"New client stream [client={client_id}] on Aggregation {ID}")
            
            fruit_list = self.fruit_top_by_client[client_id]
            
            found = False
            for i, item in enumerate(fruit_list):
                if item.fruit == fruit:
                    fruit_list[i] = item + fruit_item.FruitItem(fruit, amount)
                    found = True
                    break
            
            if not found:
                bisect.insort(fruit_list, fruit_item.FruitItem(fruit, amount))
            

    def _process_eof(self, client_id, source_id=None):
        logging.info(f"Received EOF [client={client_id}]")
        with self.lock:
            if client_id in self.completed_clients:
                return
            if source_id is not None:
                if source_id in self.received_eofs_by_client[client_id]:
                    return
                
            if client_id not in self.sums_received_by_client:
                self.sums_received_by_client[client_id] = 0

            self.sums_received_by_client[client_id] += 1
            should_send = self.sums_received_by_client[client_id] >= SUM_AMOUNT

        if should_send:
            self._send_top(client_id)
            logging.info(f"Finished processing client stream [client={client_id}] on Aggregation {ID}")
        self.received_eofs_by_client[client_id].add(source_id)
        

    def _send_top(self, client_id):
        with self.lock:
            if client_id in self.completed_clients or client_id in self.finalizing_clients:
                return False
            self.finalizing_clients.add(client_id)
            fruit_list = list(self.fruit_top_by_client.get(client_id, []))
            if not fruit_list:
                logging.warning(f"No fruit data for [client={client_id}]")
                

            top_items = sorted(fruit_list, key= lambda item: (item.amount, item.fruit), reverse=True)[:TOP_SIZE]
            payload = [(item.fruit, item.amount) for item in top_items]

        try:
            self.output_queue.send(
                self.message_handler.serialize_partial_top(
                    payload,
                    client_id,
                    correlation_id=str(ID),
                )
            )
            logging.info(f"Sent partial top for [client={client_id}] with {len(payload)} items")
            self._send_coordination(client_id)
        except Exception:
            with self.lock:
                self.finalizing_clients.discard(client_id)
            raise

        with self.lock:
            self.finalizing_clients.discard(client_id)
            self.completed_clients.add(client_id)
            self.fruit_top_by_client.pop(client_id, None)
            self.sums_received_by_client.pop(client_id, None)
            self.received_partial_sums_by_client.pop(client_id, None)
            self.received_eofs_by_client.pop(client_id, None)

        return True

    def _send_coordination(self, client_id):    
        self.joiner_coordination_queue.send(
            self.message_handler.serialize_coordination(
                ALL_SUMS_FINISHED,
                client_id,
                correlation_id=str(ID),
            )
        )
        logging.info(f"Sent coordination message to Join [client={client_id}]")

    def process_messsage(self, message, ack, nack):
        try:
            msg_obj = self.message_handler.deserialize_sum_message(message)

            if msg_obj is None:
                logging.warning("Received empty message, ignoring")
                ack()
                return
            client_id = msg_obj.client_id
            if msg_obj.message_type in (MessageType.DATA, MessageType.PARTIAL_SUM):
                if not isinstance(msg_obj.payload, list) or len(msg_obj.payload) != 2:
                    raise MessageError(f"Invalid DATA payload: {msg_obj.payload}")
                
                fruit, amount = msg_obj.payload
                logging.debug(
                    f"Aggregation {ID} received {msg_obj.message_type.value} [{fruit}, {amount}] [client={client_id}]"
                )
                self._process_data(fruit, amount, client_id, msg_obj.correlation_id)
            elif msg_obj.message_type == MessageType.EOF:
                logging.debug(f"Aggregation {ID} received EOF [client={client_id}]")
                self._process_eof(client_id, msg_obj.correlation_id)
            else:
                logging.warning(f"Received unexpected message type: {msg_obj.message_type} [client={client_id}]")
            ack()
        except MessageError as e:
            logging.error(f"Message processing error: {e}")
            nack()
            

    def start(self):
        def on_message(message, ack, nack):
            self.process_messsage(message, ack, nack)
        self.input_exchange.start_consuming(on_message)

    def close(self):
        try:
            self.input_exchange.close()
        except Exception as e:
            logging.error(f"Error closing input_exchange: {e}")

        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing output_queue: {e}")

        try:
            self.joiner_coordination_queue.close()
        except Exception as e:
            logging.error(f"Error closing joiner_coordination_queue: {e}")


class AggregationWorker:
    def __init__(self, id):
        self.agg_id = id
        self.message_handler = MessageHandler()
        self.filter = AggregationFilter(message_handler=self.message_handler)
        self.logger = logging.getLogger(f"AggregationWorker{self.agg_id}")
        self.should_stop = False
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        def signal_handler(sig, frame):
            self.logger.info(f"Received signal {sig}, shutting down AggregationWorker{self.agg_id}")
            self.should_stop = True
            try:
                self.filter.close()
            except Exception as e:
                self.logger.error(f"Error closing filter: {e}")
        signal.signal(signal.SIGTERM, signal_handler)

    def run(self):
        self.logger.info(f"Starting AggregationWorker{self.agg_id}")
        try:
            # Start consuming - BLOCKING until stop_consuming() is called
            self.filter.start()
            
            self.logger.info(f"AggregationWorker {self.agg_id} finished")
            return 0
        
        except Exception as e:
            self.logger.error(f"Error in AggregationWorker {self.agg_id}: {e}")
            return 1
        
        finally:
            self.logger.info(f"AggregationWorker {self.agg_id} cleaning up")
            self.filter.close()

    def _broadcast_coordination_message(self):
        self.logger.info(f"AggregationWorker{self.agg_id} broadcasting coordination message")
        self.joiner_coordination_queue.send(self.message_handler.serialize_coordination(ALL_SUMS_FINISHED, self.agg_id))        

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_worker = AggregationWorker(ID)
    aggregation_worker.run()
    return 0


if __name__ == "__main__":
    main()
