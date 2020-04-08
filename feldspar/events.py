from collections.abc import Mapping


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
        """Echoes class, id, & reproducible representation in the REPL
        """
        return '{}, Event({})'.format(super(Event, self).__repr__(),
                                      self.__dict__)
