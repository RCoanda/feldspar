import gzip
import warnings
import zipfile
from abc import ABCMeta
from collections.abc import Sequence, Iterator

from dateutil.parser import parse
from lxml import etree

from .base import Feeder, Importer
from .events import Event
from .utils import infer_compression

PARSABLE_COMPRESSIONS = ['gz', 'zip']


class Trace(Sequence):
    """Base trace object.

    Represents an object that closely resemples the XES standard
    definition [1]_.

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
        return self.attributes_



class TraceImporter(Importer, metaclass=ABCMeta):
    def __init__(self, one_shot=False):
        super(TraceImporter, self).__init__(one_shot)


class XESImporter(TraceImporter):
    """
    Parameters
    ----------
    fielpath: str

    compression: str, default None
        XES file compression. Can be:

        * `None` - no compression is used
        * "infer" - will try to infer compression. If unsuccessfull will try
            no compression.
        * "gz" - gunzip compression
        * "zip" - zip file compression. We assume that there is only one file
            inside the zip, namely the .xes file.
    """

    def __init__(self, filepath, compression=None):
        super(XESImporter, self).__init__()

        guess = infer_compression(filepath)
        if guess is not None:
            if compression == "infer" and guess.extension not in PARSABLE_COMPRESSIONS:
                raise ValueError(
                    "File compression not recognized or not supported. File \
                    compression: {}.".format(guess))
                compression = guess

            if compression is None:
                warnings.warn(Warning("The file might not be a '.xes' file."))

        self.__compression = compression
        self.__filepath = filepath
        self.__source = None
        self.__archive = None
        self.__context = None

    def __iter__(self):
        self._initialize_resources(tag="{*}trace")
        return self

    def __next__(self):
        try:
            _, elem = next(self.__context)
            trace = self._parse_trace(elem)
            elem.clear()

            while elem.getprevious() is not None:
                del elem.getparent()[0]

            return trace

        except StopIteration:
            self._clean_resources()
            raise StopIteration

    def _initialize_resources(self, events=('end',), tag=None):
        if self.__compression is None:
            self.__source = self.__filepath
        elif self.__compression == "gz":
            self.__source = gzip.open(self.__filepath)
        elif self.__compression == "zip":
            self.__archive = zipfile.ZipFile(self.__filepath)
            self.__source = self.__archive.open(self.namelist()[0])

        self.__context = etree.iterparse(
            self.__source, events=events, tag=tag)

    def _clean_resources(self):
        # Close the lxml context
        del self.__context

        # Close archive
        if self.__compression is not None:
            self.__source.close()

            if self.__archive is not None:
                self.__archive.close()

    def _extract_meta(self):
        self._initialize_resources(
            self, tag=["classifier", "extension", "global"])

        attributes, classifiers, extensions = dict(), dict(), dict()
        omni = {"trace": dict(), "event": dict()}

        for _, elem in self.__context:
            if "classifier" in elem.tag:
                classifiers[elem.attrib["name"]] = elem.attrib
            if "extension" in elem.tag:
                extensions[elem.attrib["name"]] = elem.attrib
            if "global" in elem.tag:
                for child in elem.iterchildren():
                    omni[elem.attrib["scope"]][child.attrib["key"]
                                               ] = self._parse_attribute(child)
            # TODO: Add asumption in documentation.
            # ASSUMPTION: All trace tags lie at the end of the XES file, and
            # after and in-between them there are no other tag types.
            if "trace" in elem.tag:
                break

        self._clean_resources(self)

        return {
            'attributes': attributes, 
            'classifiers': classifiers, 
            'extensions': extensions, 
            'omni': omni
        }

    def _parse_trace(self, elem):
        events = list()
        attrib = dict()

        for child in elem.iterchildren():
            if "event" in child.tag:
                e = Event()
                for attribute in child.iterchildren():
                    e[attribute.attrib["key"]
                      ] = self._parse_attribute(attribute)
                events.append(e)
            else:
                attrib[child.attrib["key"]] = self._parse_attribute(child)

        return Trace(*events, attributes=attrib)

    def _parse_attribute(self, attribute):
        value = attribute.attrib["value"]

        try:
            if "int" in attribute.tag:
                value = int(value)
            if "float" in attribute.tag:
                value = float(value)
            if "date" in attribute.tag:
                value = parse(value)
        except:
            pass

        return value


class TraceFeeder(Feeder):
    pass
