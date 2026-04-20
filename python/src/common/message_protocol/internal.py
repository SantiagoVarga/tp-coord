import json
import logging
from enum import Enum
from typing import Any, Dict, Optional, List, Union
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


class MessageType(Enum):
    # Gateway to Sum
    DATA = "DATA"
    EOF = "EOF"
    
    # Sum to Aggregation
    PARTIAL_SUM = "PARTIAL_SUM"


    # Aggregation to Join
    PARTIAL_TOP = "PARTIAL_TOP"

    # Join to Output
    FINAL_TOP = "FINAL_TOP"


    # Coordination
    COORDINATION = "COORDINATION"
    ERROR = "ERROR"
    HEARTBEAT = "HEARTBEAT"

class MessageError(Exception):
    pass

class Message:
    def __init__(self, message_type: MessageType, payload: Optional[Union[Dict[str, Any], List[Any]]] = None):
        self.message_type = message_type
        self.payload = payload 
        self.timestamp = datetime.now().isoformat()
        self.client_id = None 
        self.correlation_id = None

    def to_dict(self):
        return {
            "message_type": self.message_type.value,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "client_id": self.client_id,
            "correlation_id": self.correlation_id
        }
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Message':
        try:
            msg_type = MessageType(data["message_type"])
            msg = Message(message_type=msg_type, 
                           payload=data.get("payload"))
            if "timestamp" in data:
                msg.timestamp = data.get("timestamp")
            msg.client_id = data.get("client_id")
            msg.correlation_id = data.get("correlation_id")
            return msg
        except KeyError as e:
            raise MessageError(f"Campo requerido faltante en mensaje: {e}")
        except ValueError as e:
            raise MessageError(f"msg_type inválido: {e}")


def serialize(message):
    try:
        if isinstance(message, Message):
            data = message.to_dict()
        else: 
            data = message
        json_str = json.dumps(data,default=str)
        return json_str.encode("utf-8")
    except (TypeError, ValueError) as e:
        raise MessageError(f"Error al serializar mensaje: {e}")


def deserialize(message_bytes):
    try:
        if not isinstance(message_bytes, bytes):
            raise MessageError("El mensaje a deserializar debe ser de tipo bytes")
        data = json.loads(message_bytes.decode("utf-8"))

        if isinstance(data, dict) and "message_type" in data and "payload" in data:
            return Message.from_dict(data)
        else: 
            return data
    except (json.JSONDecodeError, MessageError) as e:
        raise MessageError(f"Error al deserializar mensaje: {e}")
    
def serialize_data(payload,client_id):
    msg = Message(message_type= MessageType.DATA,
                  payload=payload)
    msg.client_id = client_id
    return serialize(msg)

def serialize_eof(client_id):
    msg = Message(message_type = MessageType.EOF,
                  payload = [])
    msg.client_id = client_id
    return serialize(msg)

def serialize_partial_sum(payload, client_id):
    msg = Message(
        message_type=MessageType.PARTIAL_SUM,
        payload=payload)
    msg.client_id = client_id
    return serialize(msg)


def serialize_partial_top(payload, client_id):
    msg = Message(
        message_type=MessageType.PARTIAL_TOP,
        payload=payload)
    msg.client_id = client_id
    return serialize(msg)

def serialize_final_top(payload, client_id):
    msg = Message(
        message_type=MessageType.FINAL_TOP,
        payload=payload)
    msg.client_id = client_id
    return serialize(msg)


def serialize_coordination(signal, client_id) :
    msg = Message(
        message_type=MessageType.COORDINATION,
        payload=signal)
    msg.client_id = client_id
    return serialize(msg)


def serialize_error(error_msg, client_id):
    msg = Message(
        message_type=MessageType.ERROR,
        payload={'error': error_msg})
    msg.client_id = client_id
    return serialize(msg)