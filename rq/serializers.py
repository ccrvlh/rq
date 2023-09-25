import json
import pickle

try:
    import dill
except ImportError:
    dill = None

try:
    import orjson
except ImportError:
    orjson = None

from typing import Optional
from typing import Protocol
from typing import Type
from typing import Union

from rq import utils


class SerializerProtocol(Protocol):
    """Interface for serializer objects."""

    @staticmethod
    def dumps(*args, **kwargs):
        """Serialize an object into a string."""
        raise NotImplementedError

    @staticmethod
    def loads(s, *args, **kwargs):
        """Deserialize a string into an object."""
        raise NotImplementedError


class PikleSerializer:
    """Serializer that uses pickle to serialize objects."""
    @staticmethod
    def dumps(*args, **kwargs):
        return pickle.dumps(*args, **kwargs, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def loads(s, *args, **kwargs):
        return pickle.loads(s, *args, **kwargs)


class JSONSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        return json.dumps(*args, **kwargs).encode('utf-8')

    @staticmethod
    def loads(s, *args, **kwargs):
        return json.loads(s.decode('utf-8'), *args, **kwargs)


class ORJSONSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        if not orjson:
            raise RuntimeError('`orjson` library is not installed.')
        return orjson.dumps(*args, **kwargs).encode('utf-8')

    @staticmethod
    def loads(s, *args, **kwargs):
        if not orjson:
            raise RuntimeError('`orjson` library is not installed.')
        return orjson.loads(s.decode('utf-8'), *args, **kwargs)


class DillSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        if not dill:
            raise RuntimeError('`dill` library is not installed.')
        return dill.dumps(*args, **kwargs).encode('utf-8')

    @staticmethod
    def loads(s, *args, **kwargs):
        if not dill:
            raise RuntimeError('`dill` library is not installed.')
        return dill.loads(s.decode('utf-8'), *args, **kwargs)






def resolve_serializer(serializer: Optional[Union[Type[SerializerProtocol], str]] = None) -> Type[SerializerProtocol]:
    """This function checks the user defined serializer for ('dumps', 'loads') methods
    It returns a default pickle serializer if not found else it returns a MySerializer
    The returned serializer objects implement ('dumps', 'loads') methods
    Also accepts a string path to serializer that will be loaded as the serializer.

    Args:
        serializer (Callable): The serializer to resolve.

    Returns:
        serializer (Callable): An object that implements the SerializerProtocol
    """
    if not serializer:
        return PikleSerializer

    if isinstance(serializer, str):
        serializer = utils.import_attribute(serializer)

    default_serializer_methods = ('dumps', 'loads')

    for instance_method in default_serializer_methods:
        if not hasattr(serializer, instance_method):
            raise NotImplementedError('Serializer should implement (dumps, loads) methods.')

    return serializer


DefaultSerializer = PikleSerializer
