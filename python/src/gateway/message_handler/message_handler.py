from common import message_protocol
import logging
from typing import Any, Optional, Dict, List
import uuid

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
            return message_protocol.internal.serialize_eof(target_client_id, correlation_id)
        
        except Exception as e:
            logger.error(f"Error serializando EOF: {e}")
            raise MessageError(f"Error serializando EOF: {e}")
        

    def serialize_sum_message(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_partial_sum(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        

    def serialize_partial_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            
            return message_protocol.internal.serialize_partial_top(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        
    def serialize_final_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_final_top(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        
    def serialize_coordination(self, signal, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_coordination(signal, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error al serializar mensaje: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al serializar mensaje: {e}")
            raise MessageError(f"Error inesperado al serializar mensaje: {e}")
        
    def _deserialize_and_validate(self, message: bytes, expected_type: MessageType, 
                              allow_types: Optional[List[MessageType]] = None):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                types_to_check = allow_types if allow_types else [expected_type]
                if deserialized.message_type not in types_to_check:
                    logger.warning(
                        f"Se esperaba {expected_type.value}, obtuvo {deserialized.message_type.value}"
                    )
                return deserialized
            else:
                logger.debug(f"Deserializado mensaje (legacy): {deserialized}")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializando {expected_type.value}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado deserializando {expected_type.value}: {e}")
            raise MessageError(f"Error deserializando {expected_type.value}: {e}")
        
    def deserialize_sum_message(self, message):
        return self._deserialize_and_validate(
            message, 
            MessageType.PARTIAL_SUM,
            allow_types=[MessageType.PARTIAL_SUM, MessageType.EOF]
        )

    def deserialize_data_message(self, message):
        return self._deserialize_and_validate(message, MessageType.DATA)

    def deserialize_control_message(self, message):
        return self._deserialize_and_validate(message, MessageType.EOF)

    def deserialize_partial_top_message(self, message):
        return self._deserialize_and_validate(message, MessageType.PARTIAL_TOP)

    def deserialize_coordination_message(self, message):
        return self._deserialize_and_validate(message, MessageType.COORDINATION)

   
    # Comportamiento diferente al resto de deserializadores
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
        
    