from abc import ABCMeta, abstractmethod
from collections.abc import Mapping, Sequence


class BaseGenerator(metaclass=ABCMeta):
    """Base class all generators inherit from.

    **Warning**: This class should not be used directly.
    Use derived classes instead.
    """

    def __init__(self):
        pass

    @abstractmethod
    def __iter__(self):
        pass


class Importer(BaseGenerator, metaclass=ABCMeta):
    """Special type of `BaseGenerator`, that is always the initial element
    producing block.

    **Warning**: This class should not be used directly.
    Use derived classes instead.
    """

    def __init__(self):
        super(Importer, self).__init__()

    @abstractmethod
    def __next__(self):
        pass

class Event(Mapping):
    """Base event object.

    Represents an object that closely resemples the XES standard
    definition [1]_.

    References
    ----------
    .. [1] Xes-standard.org. (2020). start | XES. [online] Available 
        at: http://xes-standard.org/ [Accessed 8 Apr. 2020].
    """

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return '{}, Event({})'.format(super(Event, self).__repr__(),
                                      self.__dict__)


class Trace(Sequence):
    """Base trace object.

    Represents an object that closely resemples the XES standard
    definition [1]_.

    Parameters
    ----------
    *args: iterable
        Event list.

    attributes: dict, default `None`
        Set of attributes, specific to the trace.

    Attributes
    ----------
    attributes: dict
        Set of attributes, specific to the trace.

    References
    ----------
    .. [1] Xes-standard.org. (2020). start | XES. [online] Available 
        at: http://xes-standard.org/ [Accessed 8 Apr. 2020].
    """

    def __init__(self, *args, attributes=None):
        self.__list = list(args)
        self.__attributes = {} if attributes is None else attributes

    def __getitem__(self, index):
        return self.__list[index]

    def __len__(self):
        return len(self.__list)

    def __str__(self):
        return "Trace(attributes={},\nnumber_of_events={}\n)".format(str(self.__attributes), len(self))

    def __repr__(self):
        return "Trace(events={})".format(len(self))

    @property
    def attributes(self):
        """Return trace attributes.
        """
        return self.attributes_
