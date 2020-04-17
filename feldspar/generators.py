import gzip
import os
import pickle
import warnings
import zipfile
from abc import ABCMeta
from collections.abc import Iterator
import multiprocessing

from dateutil.parser import parse
from lxml import etree

from .base import BaseGenerator, Event, Importer, Trace
from .utils import infer_compression, validate_filepath

PARSABLE_COMPRESSIONS = ['gz', 'zip']


class ElementGenerator(BaseGenerator, metaclass=ABCMeta):
    """Base class for element generating objects such as event and trace 
    generators.

    .. warning:: 
        This class should not be used directly. Use derived classes instead.

    Parameters
    ----------
    source: iterable
        Underlying generator.

    attributes: dict
        Set of attributs 

    Attributes
    ----------
    attributes: dict
        Set of attributes specific to the generator. Might be passed down from
        an underlying generator.
    """

    def __init__(self, source, attributes=None):
        self._source = source
        self.__attributes = attributes

    def cache(self, filepath=None):
        """Caches the elements in this dataset.

        The first time the dataset is iterated over, its elements will 
        be cached either in the specified file or in memory. Subsequent 
        iterations will use the cached data.

        When caching to a file, the cached data will persist across runs. 
        Even the first iteration through the data will read from the cache 
        file. Changing the input pipeline before the call to .cache() will 
        have no effect until the cache file is removed or the filename is 
        changed.

        .. note::
            For the cache to be finalized, the input dataset must be 
            iterated through in its entirety. Otherwise, subsequent 
            iterations will not use cached data.

            We would also like to mention and thank the tensorflow team,
            as this has been a great inspiration.[1]_

        Parameters
        ----------
        filepath: path-like
            A `str` representing the name of a file on the filesystem 
            to use for caching elements in this Dataset. If a filename is not 
            provided, the dataset will be cached in memory.

        Returns
        -------
        `ElementGenerator`

        Examples
        --------
        >>> L = TraceGenerator.from_file('/path/to/file.xes')
        >>> L = L.filter(lambda trace: len(trace) <= 5)
        >>> L = L.cache() 
        >>> # The first time reading through the data will generate the data using 
        >>> # `from_file` and `filter`.
        >>> len(list(L))
        6 
        >>> # Subsequent iterations read from the cache.
        >>> len(list(L))
        6

        Similarly, you can also cache to a file.
        >>> L = TraceGenerator.from_file('/path/to/file.xes')
        >>> L = L.filter(lambda trace: len(trace) <= 5)
        >>> L = L.cache(/path/to/cache_file.dat)

        References
        ----------
        .. [1] TensorFlow. (2020). tf.data.Dataset  |  TensorFlow Core v2.1.0. 
            [online] Available at: 
            https://www.tensorflow.org/api_docs/python/tf/data/Dataset#cache 
            [Accessed 11 Apr. 2020].
        """
        return _CacheGenerator(self, filepath=filepath)

    def filter(self, predicate):
        """Filters this dataset according to `predicate`.

        .. note::
            We would also like to mention and thank the tensorflow team,
            as this has been a great inspiration.[1]_

        Parameters
        ----------
        predicate: function
            A function mapping a `ElementGenerator` element to a boolean.

        Returns
        -------
        `ElementGenerator`

        Examples
        --------
        >>> L = TraceGenerator.from_file('/path/to/file.xes')
        >>> len(L)
        6
        >>> L = L.filter(lambda trace: len(trace) <= 5)
        >>> len(list(L))
        4

        References
        ----------
        .. [1] TensorFlow. (2020). tf.data.Dataset  |  TensorFlow Core v2.1.0. 
            [online] Available at: 
            https://www.tensorflow.org/api_docs/python/tf/data/Dataset#cache 
            [Accessed 11 Apr. 2020].
        """
        return _FilterGenerator(self, predicate=predicate)

    def map(self, map_func):
        """Maps `map_func` across the elements of this dataset.

        This transformation applies `map_func` to each element of this dataset, 
        and returns a new dataset containing the transformed elements, in the 
        same order as they appeared in the input. `map_func` can be used to 
        change both the values and the structure of a dataset's elements. For 
        example, adding 1 to each element, or projecting a subset of element 
        components.

         .. note::
            We would also like to mention and thank the tensorflow team,
            as this has been a great inspiration.[1]_

        Parameters
        ----------
        map_func: function
            A function mapping a 'ElementGenerator' element to another 
            `ElementGenerator` element.

        Returns
        -------
        `ElementGenerator`

        Examples
        --------
        >>> L = TraceGenerator.from_file('/path/to/file.xes')
        >>> first = next(iter(L))
        >>> first[0]
        {
            "concept:name": "register request"
            "org:resource": "Pete"
        }
        >>> def label_initial(trace):
        ...     for event in trace:
        ...         event["concept:name"] = event["concept:name"][0]
        ...     return trace
        >>> L = L.map(label_initial)
        >>> first = next(iter(L))
        >>> first[0]
        {
            "concept:name": "r"
            "org:resource": "Pete"
        }

        We can also change the structure of the elements.
        >>> L = TraceGenerator.from_file('/path/to/file.xes')
        >>> next(iter(L))
        Trace(number_of_events=26)
        >>> def label_sequence(trace):
        ...     return tuple(e["concept:name"] for e in trace)
        >>> L = L.map(label_sequence)
        >>> next(iter(L))
        ('A_SUBMITTED', 'A_PARTLYSUBMITTED', 'A_PREACCEPTED', ...)

        References
        ----------
        .. [1] TensorFlow. (2020). tf.data.Dataset  |  TensorFlow Core v2.1.0. 
            [online] Available at: 
            https://www.tensorflow.org/api_docs/python/tf/data/Dataset#cache 
            [Accessed 11 Apr. 2020].
        """
        return _MapGenerator(self, map_func)

    @property
    def attributes(self):
        """Return generator attributes.
        """
        return self.__attributes


class XESImporter(Importer):
    """Importer of XES files. 

    Attempts to implements as close as possible the XES standard
    definition [1]_. 

    .. warning:: 
        This class should not be used directly. Use derived classes instead.

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


class _PickleImporter(Importer):
    """Importer for pickled elements.

    Elements are expected to have been inserted one-by-one,
    as the `_PickleImporter` still iterates over the file, 
    instead of reading all into memory.

    Parameters
    ----------
    filepath: path-like
        A `str` representing the name of an existing file on the 
        filesystem.
    """

    def __init__(self, filepath):
        self.__filepath = filepath

    def __iter__(self):
        self.__source = open(self.__filepath, 'rb')
        return self

    def __next__(self):
        try:
            trace = pickle.load(self.__source)
            return trace
        except EOFError:
            self.__source.close()
            raise StopIteration


class TraceGenerator(ElementGenerator):
    """Generating trace class, representation for an event log.

    Parameters
    ----------
    source: iterable or list-like
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

    def __init__(self, source, attributes=None):
        if attributes is None:
            attributes, classifiers, extensions = dict(), dict(), dict()
            omni = {"trace": dict(), "event": dict()}
            attributes = {
                'attributes': attributes,
                'classifiers': classifiers,
                'extensions': extensions,
                'omni': omni
            }
        super(TraceGenerator, self).__init__(source, attributes=attributes)

    def __iter__(self):
        if self._source is None:
            raise ValueError(
                "No underlying generator has yet been initialized.")
        return iter(self._source)

    @staticmethod
    def from_file(filepath, compression=None):
        """Parse an xes-file iteratively. 

        .. warning::
            We currently assume that all meta information lies before the 
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
    """A `ElementGenerator` that caches the elements of it's source.
    """

    def __init__(self, source, filepath=None):
        self.__cached = False
        self.__cache = []

        # Reference to original generator
        self.__original = source

        if filepath is not None:
            if not validate_filepath(filepath):
                raise ValueError(
                    "Make sure filepath exists. Path: {}.".format(filepath))
            if os.path.getsize(filepath) == 0:
                self.__pickle_generator_to_file(source, filepath)
            self.__cache = _PickleImporter(filepath)
            source = self.__cache
            self.__cached = True

        super(_CacheGenerator, self).__init__(source)

    def __iter__(self):
        if self.__cached:
            self._source = self.__cache
        self._source = iter(self._source)
        return self

    def __next__(self):
        try:
            trace = next(self._source)
            if not self.__cached:
                self.__cache.append(trace)
            return trace
        except StopIteration:
            if not self.__cached:
                self.__cached = True
                self._source = self.__cache
            raise StopIteration

    def __pickle_generator_to_file(self, source, filepath):
        with open(filepath, 'wb') as handler:
            for trace in source:
                pickle.dump(trace, handler)

    @property
    def attributes(self):
        """Returns underlying generator attributes.
        """
        if self._source is None:
            raise ValueError(
                "No underlying generator has yet been initialized.")
        if self.__cached:
            return self.__original.attributes
        return self._source.attributes


class _FilterGenerator(ElementGenerator):
    """A `ElementGenerator` that filters the elements of it's source.
    """

    def __init__(self, source, predicate):
        super(_FilterGenerator, self).__init__(source)
        self.__predicate = predicate

    def __iter__(self):
        return filter(self.__predicate, self._source)


class _MapGenerator(ElementGenerator):
    """An `ElementGenerator` that maps a function over elements in its input.
    """
    def __init__(self, source, map_func):
        super(_MapGenerator, self).__init__(source)
        self.__map_func = map_func

    def __iter__(self):
        return map(self.__map_func, self._source)