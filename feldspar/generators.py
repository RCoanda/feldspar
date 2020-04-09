import gzip
import warnings
import zipfile
from abc import ABCMeta
from collections.abc import Iterator
from itertools import islice

from dateutil.parser import parse
from lxml import etree

from .base import BaseGenerator, Event, Importer, Trace
from .utils import infer_compression

PARSABLE_COMPRESSIONS = ['gz', 'zip']


class ElementGenerator(BaseGenerator, metaclass=ABCMeta):
    """Base class for element generating objects such as event and trace 
    generators.

    **Warning**: This class should not be used directly.
    Use derived classes instead.

    Parameters
    ----------
    *args: iterable
        Underlying generator.

    attributes: dict
        Set of attributs 

    Attributes
    ----------
    attributes: dict
        Set of attributes specific to the generator. Might be passed down from
        an underlying generator.
    """

    def __init__(self, *args, attributes=None):
        self._gen = args
        self.attributes_ = attributes

    def cache(self, buffer_size=None):
        return _CacheGenerator(*self._gen, buffer_size=buffer_size)

    @property
    def attributes(self):
        """Return generator attributes.
        """
        return self.attributes_


class XESImporter(Importer):
    """Importer of XES files. 

    Attempts to implements as close as possible the XES standard
    definition [1]_. 

    **Warning**: We currently assume that all meta information lies before the 
    first occurence of a trace and none after.

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

    References
    ----------
    .. [1] Xes-standard.org. (2020). start | XES. [online] Available 
        at: http://xes-standard.org/ [Accessed 8 Apr. 2020].
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
        self.__initialize_resources(tag="{*}trace")
        return self

    def __next__(self):
        try:
            _, elem = next(self.__context)
            trace = self.__parse_trace(elem)
            elem.clear()

            while elem.getprevious() is not None:
                del elem.getparent()[0]

            return trace

        except StopIteration:
            self.__clean_resources()
            raise StopIteration

    def __initialize_resources(self, events=('end',), tag=None):
        """Setup up source resources, primarily the `lxml` iterator.

        Parameters
        ----------
        events: tuple
            Parses XML into a tree and generates tuples (event, element)
            in a SAX-like fashion. `event` is any of 'start', 'end', 
            'start-ns', 'end-ns'.

        tag: str or sequence
            The additional tag argument restricts the 'start' and 'end' 
            events to those elements that match the given tag. The tag 
            argument can also be a sequence of tags to allow matching more 
            than one tag. By default, events are generated for all elements. 
            Note that the 'start-ns' and 'end-ns' events are not impacted 
            by this restriction.
        """
        if self.__compression is None:
            self.__source = self.__filepath
        elif self.__compression == "gz":
            self.__source = gzip.open(self.__filepath)
        elif self.__compression == "zip":
            self.__archive = zipfile.ZipFile(self.__filepath)
            self.__source = self.__archive.open(self.namelist()[0])

        self.__context = etree.iterparse(
            self.__source, events=events, tag=tag)

    def __clean_resources(self):
        """Close resources opened in a previous step, such as releasing opened
        files and archives.
        """
        # Close the lxml context
        del self.__context

        # Close archive
        if self.__compression is not None:
            self.__source.close()

            if self.__archive is not None:
                self.__archive.close()

    def _extract_meta(self):
        """Extract XES log meta informations such as classifiers, extensions, 
        attributes and global definitions.
        """
        self.__initialize_resources()

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
                                               ] = self.__parse_attribute(child)
            # TODO: Add asumption in documentation.
            # ASSUMPTION: All trace tags lie at the end of the XES file, and
            # after and in-between them there are no other tag types.
            if "trace" in elem.tag:
                break

        self.__clean_resources()

        return {
            'attributes': attributes,
            'classifiers': classifiers,
            'extensions': extensions,
            'omni': omni
        }

    def __parse_trace(self, trace):
        """Parse trace as described in the XES standard
        definition [1]_.

        Parameters
        ----------
        trace: lxml.etree.Element
            XML element representing a trace.

        References
        ----------
        .. [1] Xes-standard.org. (2020). start | XES. [online] Available 
            at: http://xes-standard.org/ [Accessed 8 Apr. 2020]. 
        """
        events = list()
        attrib = dict()

        for child in trace.iterchildren():
            if "event" in child.tag:
                e = Event()
                for attribute in child.iterchildren():
                    e[attribute.attrib["key"]
                      ] = self.__parse_attribute(attribute)
                events.append(e)
            else:
                attrib[child.attrib["key"]] = self.__parse_attribute(child)

        return Trace(*events, attributes=attrib)

    def __parse_attribute(self, attribute):
        """Parse an attribute as described in the XES standard
        definition [1]_.

        Parameters
        ----------
        attribute: lxml.etree.Element
            XML element representing a attribute.

        References
        ----------
        .. [1] Xes-standard.org. (2020). start | XES. [online] Available 
            at: http://xes-standard.org/ [Accessed 8 Apr. 2020]. 
        """
        value = attribute.attrib["value"]

        try:
            if "int" in attribute.tag:
                value = int(value)
            if "float" in attribute.tag:
                value = float(value)
            if "date" in attribute.tag:
                value = parse(value)
            # TODO: Extend for id, lists, containers
        except:
            pass

        return value


class TraceGenerator(ElementGenerator):
    """Generating trace class, representation for an event log.

    Parameters
    ----------
    *args: iterable
        Underlying generator.

    attributes: dict, default `None`
        Set of attributs, such as event log attributes, classifiers, 
        extensions and global trace and event definitions.

    Examples
    --------
    First a simple example on how to construct (although very unlikely,
    you'll ever do it this way) and iterate over a `TraceGenerator`. 
    >>> traces = [ Trace(Event(), Event()), Trace(Event())]
    >>> L = TraceGenerator(traces)
    >>> for t in L:
    ...     print(t)
    ... Trace( attributes={}, number_of_events=2 )
    ... Trace( attributes={}, number_of_events=1 )

    Now onto a more pragmatic example.
    >>> filepath = "https://raw.githubusercontent.com/xcavation/feldspar/feature/base-setup/data/running-example.xes"
    >>> L = TraceFeeder.from_file(filepath)

    >>> L = L.map(lambda trace: tuple(event["concept:name"] for event in trace))
    >>> L = L.filter(lambda trace: len(trace) < 5)
    >>> L = L.shuffle() 
    """

    def __init__(self, *args, attributes=None):
        if attributes is None:
            attributes, classifiers, extensions = dict(), dict(), dict()
            omni = {"trace": dict(), "event": dict()}
            attributes = {
                'attributes': attributes,
                'classifiers': classifiers,
                'extensions': extensions,
                'omni': omni
            }
        super(TraceGenerator, self).__init__(*args, attributes=attributes)

    def __iter__(self):
        if self._gen is None:
            raise ValueError(
                "No underlying generator has yet been initialized.")
        return iter(*self._gen)

    @staticmethod
    def from_file(filepath, compression=None):
        """Parse an xes-file iteratively. 

        **Warning**: We currently assume that all meta information lies before the 
        first occurence of a trace in the xes-file and none after.

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
        gen = XESImporter(filepath, compression)
        attributes = gen._extract_meta()

        return TraceGenerator(gen, attributes=attributes)

class _CacheGenerator(ElementGenerator):

    def __init__(self, *args, buffer_size=None):
        super(_CacheGenerator, self).__init__()
        self.__cache_generator(*args, buffer_size=buffer_size)

    def __iter__(self):
        if self._gen is None:
            raise ValueError(
                "No underlying generator has yet been initialized.")
        return iter(*self._gen)

    def __cache_generator(self, *args, buffer_size=None):
        l = list(islice(iter(*args), buffer_size))
        self._gen = l 

    @property
    def attributes(self):
        if self._gen is None:
            raise ValueError(
                "No underlying generator has yet been initialized.")
        return self._gen.attributes