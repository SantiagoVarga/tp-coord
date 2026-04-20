from common import message_protocol
import logging
from typing import Any, Optional, Dict, List
from collections import defaultdict
import uuid

from common import message_protocol
from common.message_protocol.internal import (
    Message, MessageType, MessageError, serialize, deserialize
)

logger = logging.getLogger(__name__)


class _TruthyList(list):
    def __bool__(self):
        return True

class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.client_queues_in = dict()
        self.client_queues_out = dict()

        logger.info("MessageHandler initialized")
    
    def serialize_data_message(self, message,client_id: Optional[str] = None):
        try:
            if not isinstance(message, (list, tuple)) or len(message) != 2:
                raise MessageError(f"Datos inválidos para mensaje de tipo DATA: {message}")
            
            fruit, amount = message

            if not isinstance(fruit, str) or not isinstance(amount, int):
                raise MessageError(f"Datos de tipo incorrecto para mensaje de tipo DATA: {message}")
            
            target_client_id = self.client_id if client_id is None else client_id
            logger.debug(f"Serializando mensaje DATA: {message} [client={target_client_id}]")
            return message_protocol.internal.serialize_data(message, target_client_id)
        
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")

    def serialize_eof_message(self, message, client_id: Optional[str] = None, correlation_id: Optional[str] = None):
        try:
            target_client_id = self.client_id if client_id is None else client_id
            logger.debug(f"Serializando EOF [client={target_client_id}]")
            msg = Message(message_type=MessageType.EOF, payload=[])
            msg.client_id = target_client_id
            msg.correlation_id = correlation_id
            return serialize(msg)
        
        except Exception as e:
            logger.error(f"Error serializando EOF: {e}")
            raise MessageError(f"Error serializando EOF: {e}")
        

    def serialize_sum_message(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            msg = Message(message_type=MessageType.PARTIAL_SUM, payload=message)
            msg.client_id = client_id
            msg.correlation_id = correlation_id
            return serialize(msg)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        

    def serialize_partial_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            msg = Message(message_type=MessageType.PARTIAL_TOP, payload=message)
            msg.client_id = client_id
            msg.correlation_id = correlation_id
            return serialize(msg)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        
    def serialize_final_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            msg = Message(message_type=MessageType.FINAL_TOP, payload=message)
            msg.client_id = client_id
            msg.correlation_id = correlation_id
            return serialize(msg)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        
    def serialize_coordination(self, signal, client_id, correlation_id: Optional[str] = None):
        try:
            msg = Message(message_type=MessageType.COORDINATION, payload=signal)
            msg.client_id = client_id
            msg.correlation_id = correlation_id
            return serialize(msg)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")

    def deserialize_sum_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)

            if isinstance(deserialized, Message):
                if deserialized.message_type not in (MessageType.PARTIAL_SUM, MessageType.EOF):
                    logger.warning(
                        f"Se esperaba PARTIAL_SUM o EOF, obtuvo {deserialized.message_type.value}"
                    )
                return deserialized

            logger.debug(f"Deserializado mensaje parcial de suma (legacy): {deserialized}")
            return deserialized

        except MessageError as e:
            logger.error(f"Error deserializando mensaje parcial de suma: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando mensaje parcial de suma: {e}")
            raise MessageError(f"Error deserializando mensaje parcial de suma: {e}")


    def deserialize_data_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                if deserialized.message_type != MessageType.DATA:
                    logger.warning(
                        f"Se esperaba DATA, obtuvo {deserialized.message_type.value}"
                    )
                payload = deserialized.payload
                client_id = deserialized.client_id
                logger.debug(f"Deserializado DATA: {payload} [client={client_id}]")
                return deserialized
            
            else:
                logger.debug(f"Deserializado mensaje (legacy): {deserialized}")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializando mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando mensaje: {e}")
            raise MessageError(f"Error inesperado deserializando mensaje: {e}")

    def deserialize_control_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                if deserialized.message_type != MessageType.EOF:
                    logger.warning(
                        f"Se esperaba EOF, obtuvo {deserialized.message_type.value}"
                    )
                payload = deserialized.payload
                client_id = deserialized.client_id
                logger.debug(f"Deserializado control EOF: {payload} [client={client_id}]")
                return deserialized
            
            else:
                logger.debug(f"Deserializado mensaje de control (legacy): {deserialized}")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializando mensaje de control: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando mensaje de control: {e}")
            raise MessageError(f"Error inesperado deserializando mensaje de control: {e}")

    def deserialize_result_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)

            if isinstance(deserialized, Message):
                if deserialized.message_type != MessageType.FINAL_TOP:
                    logger.warning(
                        f"Se esperaba FINAL_TOP, obtuvo {deserialized.message_type.value}"
                    )
                    return None

                if deserialized.client_id is not None and deserialized.client_id != self.client_id:
                    logger.debug(
                        "Deserializado FINAL_TOP para otro cliente "
                        f"[expected={self.client_id}, actual={deserialized.client_id}]"
                    )
                    return None

                payload = deserialized.payload or []
                logger.debug(f"Deserializado FINAL_TOP: {len(payload)} items [client={self.client_id}]")

                return _TruthyList(payload)
            else:
                if isinstance(deserialized, list):
                    logger.debug(f"Deserializado resultado (legacy): {len(deserialized)} items")
                    return _TruthyList(deserialized)

                logger.debug(f"Deserializado resultado (legacy no listado): {deserialized}")
                return None
        
        except MessageError as e:
            logger.error(f"Error deserializando resultado: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando: {e}")
            raise MessageError(f"Error deserializando resultado: {e}")
        
    def deserialize_partial_top_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                if deserialized.message_type != MessageType.PARTIAL_TOP:
                    logger.warning(
                        f"Se esperaba PARTIAL_TOP, obtuvo {deserialized.message_type.value}"
                    )
                return deserialized
            else:
                logger.debug(f"Deserializado resultado (legacy): {len(deserialized)} items")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializando resultado: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando: {e}")
            raise MessageError(f"Error deserializando resultado: {e}")
    
    def deserialize_coordination_message(self, message):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                if deserialized.message_type != MessageType.COORDINATION:
                    logger.warning(
                        f"Se esperaba COORDINATION, obtuvo {deserialized.message_type.value}"
                    )
                return deserialized
            else:
                logger.debug(f"Deserializado mensaje de coordinación (legacy): {deserialized}")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializando mensaje de coordinación: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando mensaje de coordinación: {e}")
            raise MessageError(f"Error deserializando mensaje de coordinación: {e}")