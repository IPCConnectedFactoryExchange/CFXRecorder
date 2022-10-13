"""Generic message ensapsulation.

This is a wrapper class around the basic functionality provided by
amqp.Message that attempts to decouple downstream consumers of messages from
the exact details of pyamqp's message class as well as providing some
convenience functions for decoding json messages and decompressing gzipped
data.
"""

from typing import Dict, Optional
import datetime
import logging
import json
import hashlib
import gzip
import base64
import struct
import amqp
import dateutil.parser


LOG = logging.getLogger(__name__)


class DataMessage:
    """Generic representation of a message received or sent inside the broker.

    This class maps closely to the structure of AMQP messages and provides a
    thin convenience wrapper around the underlying functionality of
    ``amqp.Message`` while enforcing some of the required conventions in how
    broker messages are composed.

    Args:
        body: The opaque body of the message
        topic: The amqp routing key of the message
        timestamp: The time the message was constructed or the event it refers
            to happened.  This will be encoded to millisecond precision in the
            ``timestamp_in_ms`` application header when transported inside the
            broker.
        headers: Optional application headers for the message.  If the message
            is compressed, then ``content_encoding`` must be set
            appropriately. ``content_type`` should be set if it is known.
        delivery_tag: If this message was received from an amqp broker, this
            should be set correctly with the delivery tag to support
            acknowledging receipt back to the broker.
    """

    topic: str
    body: bytes
    delivery_tag: Optional[int]
    headers: Dict[str, str]

    timestamp: datetime.datetime

    def __init__(self, body: bytes, topic: str, timestamp: datetime.datetime, *,
                 headers: Optional[Dict[str, str]] = None, delivery_tag: Optional[int] = None):
        self.body = body
        self.topic = topic
        self.delivery_tag = delivery_tag

        if headers is None:
            headers = {}

        self.headers = headers
        self.timestamp = timestamp

        self._decoded_body: Optional[dict] = None
        self._hash: Optional[str] = None

    def hash(self) -> str:
        """Compute a stable hash string of this message's contents.

        The hash function is a string representation of sha256.
        """

        if self._hash is None:
            digest = hashlib.sha256(self.serialize().encode('utf-8')).hexdigest()
            self._hash = digest

        return self._hash

    def to_amqp(self) -> amqp.Message:
        """Convert to an amqp message for transmission to a broker.

        This is the inverse operation of ``DataMessage.FromAMQP091`` and the
        result of applying both should be idempotent.

        Returns:
            The amqp message that can be sent in a BackgroundAMQPConnection.
        """

        properties = {}
        headers = self.headers.copy()

        content_type = headers.pop('content_type', None)
        if content_type is not None:
            properties['content_type'] = content_type

        content_encoding = headers.pop('content_encoding', None)
        if content_encoding is not None:
            properties['content_encoding'] = content_encoding

        headers['timestamp_in_ms'] = _pack_message_time_ms(self.timestamp)

        if len(headers) > 0:
            properties['application_headers'] = headers

        return amqp.Message(self.body, **properties)

    def serialize(self) -> str:
        """Serialize to a json-encoded string.

        This serialized string is an exact representation of this message
        allowing it to be stored persistently and exactly reconstructed later.

        Returns:
            An opaque string serialized representation of this message.
        """

        message = {
            'format': 'json--1-0',
            'topic': self.topic,
            'body': base64.b64encode(self.body).decode('utf-8'),
            'delivery_tag': self.delivery_tag,
            'headers': self.headers,
            'timestamp': self.timestamp.isoformat()
        }

        return json.dumps(message)

    @classmethod
    def Deserialize(cls, serialized: str) -> 'DataMessage':
        """Deserialize a message from a string.

        This is the inverse operation of ``DataMessage.serialize()`` and
        the result of applying both is idempotent.

        Returns:
            The deserialized DataMessage instance.
        """

        message = json.loads(serialized)

        if message.get('format') != 'json--1-0':
            raise ValueError(f"Invalid format in serialize message: {message.get('format')}")

        body = base64.b64decode(message['body'])
        timestamp = dateutil.parser.parse(message['timestamp'])

        return cls(body, message['topic'], timestamp,
                   headers=message.get('headers'), delivery_tag=message.get('delivery_tag'))

    @classmethod
    def FromAMQP(cls, message: amqp.Message) -> 'DataMessage':
        """Create a DataMessage from a received AMQP Message.

        This method creates a DataMessage instance from an amqp message
        received from ``BackgroundAMQPConnection``

        It supports both AMQP 0.9.1 messages and AMQP 1.0 messages
        encapsulated and transported inside of RabbitMQ.  The correct decoding
        for amqp 1.0 messages is automatically detected and applied.
        """

        if message.properties.get('type') == 'amqp-1.0':
            return cls.FromAMQP10(message)

        return cls.FromAMQP091(message)

    @classmethod
    def FromAMQP091(cls, message: amqp.Message) -> 'DataMessage':
        """Create a DataMessage from a received AMQP 0.9.1 Message."""

        if message.properties.get('type') == 'amqp-1.0':
            raise ValueError("Invalid AMQP 0.9.1 message that is marked as 1.0")

        timestamp = _extract_message_time(message)
        headers = cls._extract_amqp_headers(message)

        topic = message.delivery_info['routing_key']

        return cls(message.body, topic, timestamp, headers=headers, delivery_tag=message.delivery_tag)

    @classmethod
    def FromAMQP10(cls, message: amqp.Message) -> 'DataMessage':
        """Create a DataMessage from a received AMQP 1.0 Message.

        This still assumes that the message is received via a connection to
        RabbitMQ so that it has the amqp 1.0 properties encoded and a ``type``
        property set that identifies the message as amqp 1.0 encoded.
        """

        if message.properties.get('type') != 'amqp-1.0':
            raise ValueError("Invalid AMQP 1.0 message missing type property")

        timestamp = _extract_message_time(message)

        headers = cls._extract_amqp_headers(message)

        _amqp_10_props = headers.pop('x-amqp-1.0-properties', None)
        # TODO: Support decoding and extracting these headers
        # Currently nothing of note is inside of the headers but
        # a future version of IPC-CFX messages will include default
        # compression and the ``content_encoding`` header to determine this
        # will be amqp 1.0 encoded here

        topic = message.delivery_info['routing_key']
        body = extract_amqp10_body(message.body)

        return cls(body, topic, timestamp, headers=headers, delivery_tag=message.delivery_tag)

    @classmethod
    def FromDict(cls, topic: str, data: dict,
                 timestamp: datetime.datetime, schema=None, gzip=False) -> 'DataMessage':
        """Create a standard JSON encoded DataMesage from a dictionary of data.

        The message can optionally be gzip encoded and the schema header set, if it is known to
        conform to a standard schema.

        This method is the standard way of creating a new message to be sent
        to the broker via a BackgroundAMQPConnection.

        Args:
            topic: The topic string that will be used as the message routing key
            data: The actual dictionary that will be json encoded as the body of the message
            timestamp: The timestamp that will be sent as when the message was constructed or
                refers to.
            schema: If included, this will be set as the ``x-archfx-schema`` header to indicate
                what kind of data is included in this message
            gzip: If set, the message will be gzip compressed and the appropriate header set
                for automatic decompression.

        Returns:
            The constructed DataMessage.
        """

        body_str = json.dumps(data)
        body_bin = body_str.encode('utf-8')

        headers = {}
        headers['content_type'] = "application/json"

        if schema is not None:
            headers['x-archfx-schema'] = schema

        if gzip:
            body_bin = gzip.compress(body_bin)
            headers['content_encoding'] = "application/gzip"

        return cls(body_bin, topic, timestamp, headers=headers)

    def decompress(self) -> bytes:
        """Automatically decompress the message body if needed.

        This method is a no-op unless the ``content_encoding`` header is
        correctly set, in which case the message body is decompressed.

        The decompressed result is not cached anywhere so care should be taken
        calling this method repeatedly as the message body will be
        decompressed each time this method is called.

        Returns:
            The decompresed message body.

            If no compression is used, the body itself will be returned.
        """

        encoding = self.headers.get("content_encoding")
        if encoding in ["application/gzip", "gzip"]:
            return gzip.decompress(self.body)

        if self.body[:2] == b'\x1f\x8b':
            LOG.warning("Unknown gzip-based encoding: '%s'! Trying to unzip..", encoding)
            return gzip.decompress(self.body)

        if encoding is None:
            return self.body

        raise ValueError(f"Unsupported encoding/compression type: {encoding}")

    def json(self) -> dict:
        """Decompress, if needed and decode the message body as json.

        This is a convenience method for deocde a json-encoded message with
        automatic gzip-decompression.

        The resulting ``dict`` is cached so it is efficient to call this
        method multiple times expecting that the decoding will only happen
        once.

        Returns:
            The message body interpreted as a json encoded object.
        """

        if self._decoded_body is None:
            body = self.decompress()
            self._decoded_body = json.loads(body)

        return self._decoded_body

    @staticmethod
    def _extract_amqp_headers(message: amqp.Message) -> dict:
        headers = message.properties.get("application_headers", {}).copy()
        content_type = message.properties.get("content_type")
        if content_type is not None:
            headers['content_type'] = content_type

        content_encoding = message.properties.get("content_encoding")
        if content_encoding is not None:
            if not ("content_encoding" in headers and content_encoding == "binary"):
                # This check is needed to avoid overwriting an encoding more narrowly defined
                # with simply `binary`. Some KohYoung messages have this issue.
                headers['content_encoding'] = content_encoding

        return headers



def _extract_message_time(message: amqp.Message) -> datetime.datetime:
    if "application_headers" in message.properties:
        timestamp = message.properties["application_headers"]["timestamp_in_ms"] / 1000
        LOG.log(5, "Using application headers for timestamp: %d", timestamp)

        return  datetime.datetime.utcfromtimestamp(timestamp)

    LOG.warning("No timestamp information in headers, using now()")
    return datetime.datetime.utcnow()


def _pack_message_time_ms(timestamp: datetime.datetime) -> int:
    return int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()*1000)


_AMQP_VALUE_TYPE = 0x77

_ULONG_8BIT = 0x53
_BINARY_8BIT_LENGTH = 0xa0
_STRING_8BIT_LENGTH = 0xa1
_BINARY_32BIT_LENGTH = 0xb0
_STRING_32BIT_LENGTH = 0xb1

_COMPLEX_CONSTRUCTOR = 0


def extract_amqp10_body(body: bytes) -> bytes:
    """Extract an AMQP 1.0 type encoded message body.

    This is a simple, standalone implementation of the basic components of the
    AMQP 1.0 type system in order to decode the ``amqp-value`` section of the
    message, which corresponds to the message body in AMQP 0.9.1.

    Args:
        body: The AMQP 1.0 encoded ``amqp-value`` section of an AMQP 1.0 message.

    Returns:
        The decoded body as binary bytes with the amqp 1.0 header stripped away.
    """

    if len(body) < 4:
        raise ValueError(f"Body of length {len(body)} too short to be valid AMQP 1.0 encoded")

    if body[0] != _COMPLEX_CONSTRUCTOR or body[1] != _ULONG_8BIT or body[2] != _AMQP_VALUE_TYPE:
        raise ValueError(f"AMQP 1.0 body not encoded with amqp 1.0 constructor: '{repr(body[:4])}'")

    body_type = body[3]
    if body_type in (_BINARY_8BIT_LENGTH, _STRING_8BIT_LENGTH):
        if len(body) < 5:
            raise ValueError(f"AMQP 1.0 body with 1-octet length is too short")

        length = body[4]
        body = body[5:]
    elif body_type in (_BINARY_32BIT_LENGTH, _STRING_32BIT_LENGTH):
        if len(body) < 8:
            raise ValueError(f"AMQP 1.0 body with 4-octet length is too short")

        length, = struct.unpack(">L", body[4:8])
        body = body[8:]
    else:
        raise ValueError(f"Unknown AMQP 1.0 body type: {body_type}")

    if len(body) != length:
        raise ValueError(f"AMQP 1.0 body written length {length} did not match actual length {len(body)}")

    return body
