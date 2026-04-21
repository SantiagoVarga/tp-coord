import os
import logging
import threading
import signal
import sys
import time
from threading import Lock

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware import (MessageMiddlewareDisconnectedError)

from common.message_protocol.internal import  Message, MessageType
from gateway.message_handler.message_handler import MessageHandler

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

JOINER_COORDINATION_QUEUE_PREFIX = "JOINER_COORDINATION_QUEUE"


class JoinFilter:

    def __init__(self, message_handler):
        self.input_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.message_handler = message_handler
        self.partial_tops_by_client = {}
        self.aggs_received_by_client = {}
        self.completed_clients = set()
        self.finalizing_clients = set()
        self.lock = Lock()
        self.logger = logging.getLogger("JoinFilter")

    def get_aggs_received_count(self, client_id):
        return len(self.aggs_received_by_client.get(client_id, set()))

    
    def _process_partial_top(self, fruit_top, client_id, source_id=None):
        self.logger.info(f"Processing partial top with {len(fruit_top)} items [client={client_id}]")
        with self.lock:
            if client_id in self.completed_clients:
                return
            if client_id not in self.partial_tops_by_client:
                self.partial_tops_by_client[client_id] = {}
                self.aggs_received_by_client[client_id] = set()

            if source_id is not None and source_id in self.aggs_received_by_client[client_id]:
                return

            source_key = source_id if source_id is not None else f"legacy-{len(self.aggs_received_by_client[client_id])}"
            self.partial_tops_by_client[client_id][source_key] = list(fruit_top)
            self.aggs_received_by_client[client_id].add(source_key)
    
    def _send_final_top(self, client_id):
        self.logger.info(f"All {AGGREGATION_AMOUNT} partial tops received, computing final top [client={client_id}]")
        with self.lock:
            if client_id in self.completed_clients or client_id in self.finalizing_clients:
                return
            self.finalizing_clients.add(client_id)
            partial_tops = list(self.partial_tops_by_client.get(client_id, {}).values())
            amount_by_fruit = {}
            for partial_top in partial_tops:
                for fruit, amount in partial_top:
                    current_amount = amount_by_fruit.get(fruit, 0)
                    amount_by_fruit[fruit] = current_amount + amount
            payload = sorted(
                [(fruit, amount) for fruit, amount in amount_by_fruit.items()],
                key=lambda fruit_record: (fruit_record[1], fruit_record[0]),
                reverse=True,
            )[:TOP_SIZE]
        final_msg = self.message_handler.serialize_final_top(payload, client_id)
        try:
            self.output_queue.send(final_msg)
        except Exception:
            with self.lock:
                self.finalizing_clients.discard(client_id)
            raise

        with self.lock:
            self.finalizing_clients.discard(client_id)
            self.completed_clients.add(client_id)
            self.partial_tops_by_client.pop(client_id, None)
            self.aggs_received_by_client.pop(client_id, None)

    
    def process_message(self, message, ack, nack):
        try:
            deserialized = self.message_handler.deserialize_partial_top_message(message)
            
            if isinstance(deserialized, Message):
                client_id = deserialized.client_id
                if deserialized.message_type == MessageType.PARTIAL_TOP:
                    payload = deserialized.payload
                    self.logger.info(f"Received PARTIAL_TOP with {len(payload)} items [client={client_id}]")
                    if isinstance(payload, list):
                        self._process_partial_top(payload, client_id, deserialized.correlation_id)
                else:
                    self.logger.warning(f"Unexpected message type: {deserialized.message_type} [client={client_id}]")
            else:
                fruit_top = deserialized
                self.logger.info(f"Received partial top (legacy format) with {len(fruit_top)} items")
                if isinstance(fruit_top, list):
                    self._process_partial_top(fruit_top, "unknown")
            
            ack()
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            nack()

    def close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            self.logger.error(f"Error closing input queue: {e}")
        
        try:
            self.output_queue.close()
        except Exception as e:
            self.logger.error(f"Error closing output queue: {e}")

    def start(self):
        self.input_queue.start_consuming(self.process_message)


class JoinWorker:
    def __init__(self):
        self.message_handler = MessageHandler()
        self.filter = JoinFilter(message_handler=self.message_handler)
        self.logger = logging.getLogger("JoinWorker")
        self.aggs_finished_by_client = {}
        self.coordination_ready_by_client = {}


        self.coordination_queues = []
        for i in range(AGGREGATION_AMOUNT):
            coord_queue = MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{JOINER_COORDINATION_QUEUE_PREFIX}_{i}"
            )
            self.coordination_queues.append(coord_queue)

        self.lock = Lock()
        self.should_exit = False
        self.data_thread = None
        self.coordination_threads = []
        self._setup_signal_handlers()
    
    def _request_queue_stop(self, queue):
        try:
            connection = getattr(queue, "connection", None)
            if connection is not None and connection.is_open:
                connection.add_callback_threadsafe(queue.stop_consuming)
            else:
                queue.stop_consuming()
        except Exception as e:
            self.logger.debug(f"Error requesting queue stop: {e}")

    def _stop_all_consumers(self):
        self.should_exit = True
        self._request_queue_stop(self.filter.input_queue)
        for queue in self.coordination_queues:
            self._request_queue_stop(queue)

    def _join_listener_threads(self):
        if self.data_thread is not None:
            try:
                self.data_thread.join(timeout=5)
                if self.data_thread.is_alive():
                    self.logger.warning("Data listener thread did not exit within timeout")
            except Exception as e:
                self.logger.error(f"Error joining data thread: {e}")
        for thread in self.coordination_threads:
            try:
                thread.join(timeout=5)
                if thread.is_alive():
                    self.logger.warning(f"Coordination listener thread {thread.name} did not exit within timeout")
            except Exception as e:
                self.logger.error(f"Error joining coordination thread {thread.name}: {e}")
    
    def _setup_signal_handlers(self):
        def handle_sigterm(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down JoinWorker")
            self._stop_all_consumers()
        
        signal.signal(signal.SIGTERM, handle_sigterm)

    def run(self):
        self.logger.info("Starting JoinWorker")

        try:
            self.coordination_threads = []
            for i in range(AGGREGATION_AMOUNT):
                thread = threading.Thread(
                    target=self._listen_coordination,
                    args=(i,),
                    daemon=False,
                    name=f"JoinerCoordinationListener_{i}"
                )
                thread.start()
                self.coordination_threads.append(thread)

            self.data_thread = threading.Thread(
                target=self._listen_data,
                daemon=False,
                name="JoinerDataListener"
            )
            self.data_thread.start()

            monitor_thread = threading.Thread(
                target=self._monitor_completion,
                daemon=False,
                name="JoinerMonitor"
            )
            monitor_thread.start()

            monitor_thread.join()
            self.logger.info("Monitor finished, stopping all consumers")
            
            self._stop_all_consumers()
            self.logger.info("All consumers stopped, joining listener threads")
            self._join_listener_threads()
            
            self.logger.info("JoinWorker finished")
            return 0
        except Exception as e:
            self.logger.error(f"Error in JoinWorker run: {e}")
            return 1
        finally:
            self.logger.info("Cleaning up JoinWorker resources")
            try:
                self._stop_all_consumers()
            except Exception as e:
                self.logger.error(f"Error stopping consumers during cleanup: {e}")
            try:
                self._join_listener_threads()
            except Exception as e:
                self.logger.error(f"Error joining threads during cleanup: {e}")
            try:
                self.filter.close()
            except Exception as e:
                self.logger.error(f"Error closing filter: {e}")
            
            for i, queue in enumerate(self.coordination_queues):
                try:
                    queue.close()
                except Exception as e:
                    self.logger.error(f"Error closing coordination queue {i}: {e}")

    def _listen_data(self):
        self.logger.info("Starting data listener")

        try:
            self.filter.start()
        except MessageMiddlewareDisconnectedError as e:
            self.logger.error(f"Disconnected from RabbitMQ: {e}")
        except Exception as e:
            self.logger.error(f"Error in data listener: {e}")
    
    def _listen_coordination(self, agg_id):
        self.logger.info(f"Coordination listener started for Aggregation {agg_id}")
        
        try:
            def coordination_callback(message, ack, nack):
                try:
                    deserialized = self.message_handler.deserialize_coordination_message(message)
                    if isinstance(deserialized, Message):
                        client_id = deserialized.client_id
                        self.logger.info(f"Received coordination signal from Aggregation {agg_id} [client={client_id}]")
                        with self.lock:
                            if client_id not in self.aggs_finished_by_client:
                                self.aggs_finished_by_client[client_id] = set()
                            source_id = deserialized.correlation_id if deserialized.correlation_id is not None else str(agg_id)
                            self.aggs_finished_by_client[client_id].add(source_id)
                            if len(self.aggs_finished_by_client[client_id]) == AGGREGATION_AMOUNT:
                                self.logger.info(f"All Aggregations coordinated [client={client_id}], ready to process")
                                self.coordination_ready_by_client[client_id] = True
                    ack()
                except Exception as e:
                    self.logger.error(f"Error in coordination callback: {e}")
                    nack()
            
            self.coordination_queues[agg_id].start_consuming(coordination_callback)
        except MessageMiddlewareDisconnectedError as e:
            self.logger.error(f"Disconnected from RabbitMQ in coordination: {e}")
        except Exception as e:
            self.logger.error(f"Error in coordination listener: {e}")
    
    def _monitor_completion(self):
        while not self.should_exit:
            clients_to_send = []

            with self.lock:
                for client_id in list(self.coordination_ready_by_client.keys()):
                    if self.coordination_ready_by_client[client_id] and \
                        self.filter.get_aggs_received_count(client_id) >= AGGREGATION_AMOUNT:
                        clients_to_send.append(client_id)
                        self.coordination_ready_by_client.pop(client_id, None)
                self.logger.info(f"Monitoring completion: {len(self.coordination_ready_by_client)} clients ready, {len(clients_to_send)} clients to send final top")

            for client_id in clients_to_send:
                try:
                    self.filter._send_final_top(client_id)
                except Exception:
                    with self.lock:
                        self.coordination_ready_by_client[client_id] = True

            time.sleep(0.1)
    


def main():
    logging.basicConfig(level=logging.INFO)
    join_worker = JoinWorker()
    
    try:
        return join_worker.run()
    except Exception as e:
        logging.error(f"Error in JoinWorker: {e}")
        return 2
    finally:
        logging.info("JoinWorker exiting")


if __name__ == "__main__":
    sys.exit(main())
