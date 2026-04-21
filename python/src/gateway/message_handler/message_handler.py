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
                raise MessageError(f"Invalid data for DATA message type: {message}")
            
            fruit, amount = message

            if not isinstance(fruit, str) or not isinstance(amount, int):
                raise MessageError(f"Incorrect data type for DATA message type: {message}")
            
            target_client_id = self.client_id if client_id is None else client_id
            logger.debug(f"Serializing DATA message: {message} [client={target_client_id}]")
            return message_protocol.internal.serialize_data(message, target_client_id)
        
        except MessageError as e:
            logger.error(f"Error serializing message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error serializing message: {e}")
            raise MessageError(f"Unexpected error serializing message: {e}")

    def serialize_eof_message(self, message, client_id: Optional[str] = None, correlation_id: Optional[str] = None):
        try:
            target_client_id = self.client_id if client_id is None else client_id
            logger.debug(f"Serializing EOF [client={target_client_id}]")
            return message_protocol.internal.serialize_eof(target_client_id, correlation_id)
        
        except Exception as e:
            logger.error(f"Error serializing EOF: {e}")
            raise MessageError(f"Error serializing EOF: {e}")
        

    def serialize_sum_message(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_partial_sum(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error serializing message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error serializing message: {e}")
            raise MessageError(f"Unexpected error serializing message: {e}")
        

    def serialize_partial_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            
            return message_protocol.internal.serialize_partial_top(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error serializing message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error serializing message: {e}")
            raise MessageError(f"Unexpected error serializing message: {e}")
        
    def serialize_final_top(self, message, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_final_top(message, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error serializing message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error serializing message: {e}")
            raise MessageError(f"Unexpected error serializing message: {e}")
        
    def serialize_coordination(self, signal, client_id, correlation_id: Optional[str] = None):
        try:
            return message_protocol.internal.serialize_coordination(signal, client_id, correlation_id)
        except MessageError as e:
            logger.error(f"Error serializing message: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error serializing message: {e}")
            raise MessageError(f"Unexpected error serializing message: {e}")
        
    def _deserialize_and_validate(self, message: bytes, expected_type: MessageType, 
                              allow_types: Optional[List[MessageType]] = None):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            
            if isinstance(deserialized, Message):
                types_to_check = allow_types if allow_types else [expected_type]
                if deserialized.message_type not in types_to_check:
                    logger.warning(
                        f"Expected {expected_type.value}, got {deserialized.message_type.value}"
                    )
                return deserialized
            else:
                logger.debug(f"Deserialized message (legacy): {deserialized}")
                return deserialized
        
        except MessageError as e:
            logger.error(f"Error deserializing {expected_type.value}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deserializing {expected_type.value}: {e}")
            raise MessageError(f"Error deserializing {expected_type.value}: {e}")
        
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
                        f"Expected FINAL_TOP, got {deserialized.message_type.value}"
                    )
                    return None

                if deserialized.client_id is not None and deserialized.client_id != self.client_id:
                    logger.debug(
                        "Deserialized FINAL_TOP for another client "
                        f"[expected={self.client_id}, actual={deserialized.client_id}]"
                    )
                    return None

                payload = deserialized.payload or []
                logger.debug(f"Deserialized FINAL_TOP: {len(payload)} items [client={self.client_id}]")

                return _TruthyList(payload)
            else:
                if isinstance(deserialized, list):
                    logger.debug(f"Deserialized result (legacy): {len(deserialized)} items")
                    return _TruthyList(deserialized)

                logger.debug(f"Deserialized result (legacy non-listed): {deserialized}")
                return None
        
        except MessageError as e:
            logger.error(f"Error deserializing result: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deserializing: {e}")
            raise MessageError(f"Error deserializing result: {e}")
        
    