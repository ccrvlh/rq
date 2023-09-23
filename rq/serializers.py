import json
import pickle

from abc import abstractmethod

from typing import Optional
from typing import Type
from typing import Union

from rq import utils


class SerializerInterface:
    """Interface for serializer objects."""

    @abstractmethod
    @staticmethod
    def dumps(*args, **kwargs):
        """Serialize an object into a string."""
        raise NotImplementedError

    @abstractmethod
    @staticmethod
    def loads(s, *args, **kwargs):
        """Deserialize a string into an object."""
        raise NotImplementedError


class PikleSerializer(SerializerInterface):
    """Serializer that uses pickle to serialize objects."""
    @staticmethod
    def dumps(*args, **kwargs):
        return pickle.dumps(*args, **kwargs, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def loads(s, *args, **kwargs):
        return pickle.loads(s, *args, **kwargs)


DefaultSerializer = PikleSerializer


class JSONSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        return json.dumps(*args, **kwargs).encode('utf-8')

    @staticmethod
    def loads(s, *args, **kwargs):
        return json.loads(s.decode('utf-8'), *args, **kwargs)


def resolve_serializer(serializer: Optional[Union[Type[SerializerInterface], str]] = None) -> Type[SerializerInterface]:
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
        return DefaultSerializer

    if isinstance(serializer, str):
        _serializer = utils.import_attribute(serializer)

    default_serializer_methods = ('dumps', 'loads')

    for instance_method in default_serializer_methods:
        if not hasattr(serializer, instance_method):
            raise NotImplementedError('Serializer should implement (dumps, loads) methods.')

    return _serializer
