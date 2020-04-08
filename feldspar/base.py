from abc import ABCMeta


class Feeder(metaclass=ABCMeta):
    """Base class all feeders inherit from.
    """

    def __init__(self):
        pass


class Importer(Feeder, metaclass=ABCMeta):
    """Base class for all importing. 
    """

    def __init__(self, one_shot):
        super(Importer, self).__init__()
        self.one_shot = one_shot 

    