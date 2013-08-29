#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2013, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
An implementation of URIs and URI Templates from RFC 3986 (URI Generic Syntax)
and RFC 6570 (URI Template) respectively.
"""


from __future__ import unicode_literals

from collections import OrderedDict
import re


__all__ = ["percent_encode", "percent_decode", "Authority", "Path", "Query",
           "URI", "URITemplate"]


# RFC 3986 § 2.2.
general_delimiters = ":/?#[]@"
subcomponent_delimiters = "!$&'()*+,;="
reserved = general_delimiters + subcomponent_delimiters

# RFC 3986 § 2.3.
unreserved = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
              "abcdefghijklmnopqrstuvwxyz"
              "0123456789-._~")


def percent_encode(data, safe=None):
    """ Percent encode a string of data, optionally keeping certain characters
    unencoded.

    """
    if data is None:
        return None
    if not safe:
        safe = ""
    try:
        chars = list(data)
    except TypeError:
        chars = list(str(data))
    for i, char in enumerate(chars):
        if char == "%" or (char not in unreserved and char not in safe):
            chars[i] = "".join("%" + hex(b)[2:].upper().zfill(2)
                               for b in bytearray(char, "utf-8"))
    return "".join(chars)


def percent_decode(data):
    """ Percent decode a string of data.

    """
    if data is None:
        return None
    percent_code = re.compile("(%[0-9A-Fa-f]{2})")
    try:
        bits = percent_code.split(data)
    except TypeError:
        bits = percent_code.split(str(data))
    out = bytearray()
    for bit in bits:
        if bit.startswith("%"):
            out.extend(bytearray([int(bit[1:], 16)]))
        else:
            out.extend(bytearray(bit, "utf-8"))
    return out.decode("utf-8")


class _Part(object):
    """ Internal base class for all URI component parts.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.string))

    def __str__(self):
        return self.string or ""

    def __bool__(self):
        return bool(self.string)

    def __nonzero__(self):
        return bool(self.string)

    def __len__(self):
        return len(str(self))

    def __iter__(self):
        return iter(self.string)

    @property
    def string(self):
        raise NotImplementedError()


class Authority(_Part):
    """ A host name plus optional port and user information detail.

    **Syntax**
        ``authority := [ user_info "@" ] host [ ":" port ]``

    .. seealso::
        `RFC 3986 § 3.2`_

    .. _`RFC 3986 § 3.2`: http://tools.ietf.org/html/rfc3986#section-3.2
    """

    @classmethod
    def _cast(cls, obj):
        if obj is None:
            return cls(None)
        elif isinstance(obj, cls):
            return obj
        else:
            return cls(str(obj))

    def __init__(self, string):
        super(Authority, self).__init__()
        if string is None:
            self._user_info = None
            self._host = None
            self._port = None
        else:
            if ":" in string:
                string, self._port = string.rpartition(":")[0::2]
                self._port = int(self._port)
            else:
                self._port = None
            if "@" in string:
                self._user_info, self._host = map(percent_decode,
                                                  string.rpartition("@")[0::2])
            else:
                self._user_info, self._host = None, percent_decode(string)

    def __eq__(self, other):
        other = self._cast(other)
        return (self._user_info == other._user_info and
                self._host == other._host and
                self._port == other._port)

    def __ne__(self, other):
        other = self._cast(other)
        return (self._user_info != other._user_info or
                self._host != other._host or
                self._port != other._port)

    def __hash__(self):
        return hash(self.string)

    @property
    def host(self):
        """ The host part of this authority component, an empty string if host
        is empty or :py:const:`None` if undefined.

        ::

            >>> Authority(None).host
            None
            >>> Authority("").host
            ''
            >>> Authority("example.com").host
            'example.com'
            >>> Authority("example.com:8080").host
            'example.com'
            >>> Authority("bob@example.com").host
            'example.com'
            >>> Authority("bob@example.com:8080").host
            'example.com'

        :return:
        """
        return self._host

    @property
    def host_port(self):
        """ The host and port parts of this authority component or
        :py:const:`None` if undefined.

        ::

            >>> Authority(None).host_port
            None
            >>> Authority("").host_port
            ''
            >>> Authority("example.com").host_port
            'example.com'
            >>> Authority("example.com:8080").host_port
            'example.com:8080'
            >>> Authority("bob@example.com").host_port
            'example.com'
            >>> Authority("bob@example.com:8080").host_port
            'example.com:8080'

        :return:
        """
        u = [self._host]
        if self._port is not None:
            u += [":", str(self._port)]
        return "".join(u)

    @property
    def port(self):
        """ The port part of this authority component or :py:const:`None` if
        undefined.

        ::

            >>> Authority(None).port
            None
            >>> Authority("").port
            None
            >>> Authority("example.com").port
            None
            >>> Authority("example.com:8080").port
            8080
            >>> Authority("bob@example.com").port
            None
            >>> Authority("bob@example.com:8080").port
            8080

        :return:
        """
        return self._port

    @property
    def string(self):
        """ The full string value of this authority component or
        :`py:const:`None` if undefined.

        ::

            >>> Authority(None).string
            None
            >>> Authority("").string
            ''
            >>> Authority("example.com").string
            'example.com'
            >>> Authority("example.com:8080").string
            'example.com:8080'
            >>> Authority("bob@example.com").string
            'bob@example.com'
            >>> Authority("bob@example.com:8080").string
            'bob@example.com:8080'

        :return:
        """
        if self._host is None:
            return None
        u = []
        if self._user_info is not None:
            u += [percent_encode(self._user_info), "@"]
        u += [self._host]
        if self._port is not None:
            u += [":", str(self._port)]
        return "".join(u)
        
    @property
    def user_info(self):
        """ The user information part of this authority component or
        :py:const:`None` if undefined.

        ::

            >>> Authority(None).user_info
            None
            >>> Authority("").user_info
            None
            >>> Authority("example.com").user_info
            None
            >>> Authority("example.com:8080").user_info
            None
            >>> Authority("bob@example.com").user_info
            'bob'
            >>> Authority("bob@example.com:8080").user_info
            'bob'

        :return:
        """
        return self._user_info


class Path(_Part):

    @classmethod
    def _cast(cls, obj):
        if obj is None:
            return cls(None)
        elif isinstance(obj, cls):
            return obj
        else:
            return cls(str(obj))

    def __init__(self, string):
        super(Path, self).__init__()
        if string is None:
            self._segments = None
        else:
            self._segments = list(map(percent_decode, string.split("/")))

    def __eq__(self, other):
        other = self._cast(other)
        return self._segments == other._segments

    def __ne__(self, other):
        other = self._cast(other)
        return self._segments != other._segments

    def __hash__(self):
        return hash(self.string)

    @property
    def string(self):
        if self._segments is None:
            return None
        return "/".join(map(percent_encode, self._segments))

    @property
    def segments(self):
        if self._segments is None:
            return []
        else:
            return list(self._segments)

    def __iter__(self):
        return iter(self._segments)

    def remove_dot_segments(self):
        """ Implementation of RFC3986, section 5.2.4
        """
        inp = self.string
        out = ""
        while inp:
            if inp.startswith("../"):
                inp = inp[3:]
            elif inp.startswith("./"):
                inp = inp[2:]
            elif inp.startswith("/./"):
                inp = inp[2:]
            elif inp == "/.":
                inp = "/"
            elif inp.startswith("/../"):
                inp = inp[3:]
                out = out.rpartition("/")[0]
            elif inp == "/..":
                inp = "/"
                out = out.rpartition("/")[0]
            elif inp in (".", ".."):
                inp = ""
            else:
                if inp.startswith("/"):
                    inp = inp[1:]
                    out += "/"
                seg, slash, inp = inp.partition("/")
                out += seg
                inp = slash + inp
        return Path(out)

    def with_trailing_slash(self):
        if self._segments is None:
            return self
        s = self.string
        if s.endswith("/"):
            return self
        else:
            return Path(s + "/")

    def without_trailing_slash(self):
        if self._segments is None:
            return self
        s = self.string
        if s.endswith("/"):
            return Path(s[:-1])
        else:
            return self


class Query(_Part):

    @classmethod
    def _cast(cls, obj):
        if obj is None:
            return cls(None)
        elif isinstance(obj, cls):
            return obj
        else:
            return cls(str(obj))

    @classmethod
    def encode(cls, iterable):
        if iterable is None:
            return None
        bits = []
        if isinstance(iterable, dict):
            for key, value in iterable.items():
                if value is None:
                    bits.append(percent_encode(key))
                else:
                    bits.append(percent_encode(key) + "=" + percent_encode(value))
        else:
            for item in iterable:
                bits.append(percent_encode(item))
        return "&".join(bits)

    @classmethod
    def decode(cls, string):
        if string is None:
            return None
        data = OrderedDict()
        if string:
            bits = string.split("&")
            for bit in bits:
                if "=" in bit:
                    key, value = map(percent_decode, bit.partition("=")[0::2])
                else:
                    key, value = percent_decode(bit), None
                data[key] = value
        return data

    def __init__(self, string):
        super(Query, self).__init__()
        self._query = Query.decode(string)

    def __eq__(self, other):
        other = self._cast(other)
        return self._query == other._query

    def __ne__(self, other):
        other = self._cast(other)
        return self._query != other._query

    def __hash__(self):
        return hash(self.string)

    @property
    def string(self):
        return Query.encode(self._query)

    def __iter__(self):
        if self._query is None:
            return iter(())
        else:
            return iter(self._query.items())

    def __getitem__(self, key):
        if self._query is None:
            raise KeyError(key)
        else:
            return self._query[key]


class URI(_Part):
    """ Uniform Resource Identifier.

    .. seealso::
        `RFC 3986`_

    .. _`RFC 3986`: http://tools.ietf.org/html/rfc3986
    """

    @classmethod
    def _cast(cls, obj):
        if obj is None:
            return cls(None)
        elif isinstance(obj, cls):
            return obj
        else:
            return cls(str(obj))

    def __init__(self, value):
        super(URI, self).__init__()
        try:
            if value.__uri__ is None:
                self._scheme = None
                self._authority = None
                self._path = None
                self._query = None
                self._fragment = None
                return
        except AttributeError:
            pass
        if value is None:
            self._scheme = None
            self._authority = None
            self._path = None
            self._query = None
            self._fragment = None
        else:
            try:
                value = str(value.__uri__)
            except AttributeError:
                value = str(value)
            # scheme
            if ":" in value:
                self._scheme, value = value.partition(":")[0::2]
                self._scheme = percent_decode(self._scheme)
            else:
                self._scheme = None
            # fragment
            if "#" in value:
                value, self._fragment = value.partition("#")[0::2]
                self._fragment = percent_decode(self._fragment)
            else:
                self._fragment = None
            # query
            if "?" in value:
                value, self._query = value.partition("?")[0::2]
                self._query = Query(self._query)
            else:
                self._query = None
            # hierarchical part
            if value.startswith("//"):
                value = value[2:]
                slash = value.find("/")
                if slash >= 0:
                    self._authority = Authority(value[:slash])
                    self._path = Path(value[slash:])
                else:
                    self._authority = Authority(value)
                    self._path = Path("")
            else:
                self._authority = None
                self._path = Path(value)

    def __eq__(self, other):
        other = self._cast(other)
        return (self._scheme == other._scheme and
                self._authority == other._authority and
                self._path == other._path and
                self._query == other._query and
                self._fragment == other._fragment)

    def __ne__(self, other):
        other = self._cast(other)
        return (self._scheme != other._scheme or
                self._authority != other._authority or
                self._path != other._path or
                self._query != other._query or
                self._fragment != other._fragment)

    def __hash__(self):
        return hash(self.string)

    @property
    def __uri__(self):
        return self.string

    @property
    def string(self):
        """ The full percent-encoded string value of this URI or
        :py:const:`None` if undefined.

        ::

            >>> URI(None).string
            None
            >>> URI("").string
            ''
            >>> URI("http://example.com").string
            'example.com'
            >>> URI("foo/bar").string
            'foo/bar'
            >>> URI("http://bob@example.com:8080/data/report.html?date=2000-12-25#summary").string
            'http://bob@example.com:8080/data/report.html?date=2000-12-25#summary'

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
            \___________________________________________________________________/
                                             |
                                           string

        :rtype: percent-encoded string or :py:const:`None`

        .. note::
            Unlike ``string``, the ``__str__`` method will always return a
            string, even when the URI is undefined; in this case, an empty
            string is returned instead of :py:const:`None`.
        """
        if self._path is None:
            return None
        u = []
        if self._scheme is not None:
            u += [percent_encode(self._scheme), ":"]
        if self._authority is not None:
            u += ["//", str(self._authority)]
        u += [str(self._path)]
        if self._query is not None:
            u += ["?", str(self._query)]
        if self._fragment is not None:
            u += ["#", percent_encode(self._fragment)]
        return "".join(u)

    @property
    def scheme(self):
        """ The scheme part of this URI or :py:const:`None` if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
            \___/
              |
            scheme

        :rtype: unencoded string or :py:const:`None`
        """
        return self._scheme

    @property
    def authority(self):
        """ The authority part of this URI or :py:const:`None` if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                    \__________________/
                             |
                         authority

        :rtype: :py:class:`Authority <httpstream.uri.Authority>` instance or
            :py:const:`None`
        """
        return self._authority

    @property
    def user_info(self):
        """ The user information part of this URI or :py:const:`None` if
        undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                    \_/
                     |
                 user_info

        :return: string value of user information part or :py:const:`None`
        :rtype: unencoded string or :py:const:`None`
        """
        if self._authority is None:
            return None
        else:
            return self._authority.user_info

    @property
    def host(self):
        """ The *host* part of this URI or :py:const:`None` if undefined.

        ::

            >>> URI(None).host
            None
            >>> URI("").host
            None
            >>> URI("http://example.com").host
            'example.com'
            >>> URI("http://example.com:8080/data").host
            'example.com'

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                        \_________/
                             |
                            host

        :return:
        :rtype: unencoded string or :py:const:`None`
        """
        if self._authority is None:
            return None
        else:
            return self._authority.host
        
    @property
    def port(self):
        """ The *port* part of this URI or :py:const:`None` if undefined.

        ::

            >>> URI(None).port
            None
            >>> URI("").port
            None
            >>> URI("http://example.com").port
            None
            >>> URI("http://example.com:8080/data").port
            8080

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                                    \__/
                                     |
                                    port

        :return:
        :rtype: integer or :py:const:`None`
        """
        if self._authority is None:
            return None
        else:
            return self._authority.port

    @property
    def host_port(self):
        """ The *host* and *port* parts of this URI separated by a colon or
        :py:const:`None` if both are undefined.

        ::

            >>> URI(None).host_port
            None
            >>> URI("").host_port
            None
            >>> URI("http://example.com").host_port
            'example.com'
            >>> URI("http://example.com:8080/data").host_port
            'example.com:8080'
            >>> URI("http://bob@example.com:8080/data").host_port
            'example.com:8080'

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                        \______________/
                               |
                           host_port

        :return:
        :rtype: percent-encoded string or :py:const:`None`
        """
        if self._authority is None:
            return None
        else:
            return self._authority.host_port

    @property
    def path(self):
        """ The *path* part of this URI or :py:const:`None` if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                                        \_______________/
                                                |
                                               path

        :return:
        :rtype: :py:class:`Path <httpstream.uri.Path>` instance or
            :py:const:`None`
        """
        return self._path

    @property
    def query(self):
        """ The *query* part of this URI or :py:const:`None` if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                                                          \_____________/
                                                                 |
                                                               query

        :rtype: :py:class:`Query <httpstream.uri.Query>` instance or
            :py:const:`None`
        """
        return self._query

    @property
    def fragment(self):
        """ The *fragment* part of this URI or :py:const:`None` if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                                                                          \_____/
                                                                             |
                                                                          fragment

        :return:
        :rtype: unencoded string or :py:const:`None`
        """
        return self._fragment

    @property
    def hierarchical_part(self):
        """ The authority and path parts of this URI or :py:const:`None` if
        undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                    \___________________________________/
                                      |
                              hierarchical_part

        :return: combined string values of authority and path parts or
            :py:const:`None`
        :rtype: percent-encoded string or :py:const:`None`
        """
        if self._path is None:
            return None
        u = []
        if self._authority is not None:
            u += ["//", str(self._authority)]
        u += [str(self._path)]
        return "".join(u)

    @property
    def absolute_path_reference(self):
        """ The path, query and fragment parts of this URI or :py:const:`None`
        if undefined.

        **Component Definition:**
        ::

            https://bob@example.com:8080/data/report.html?date=2000-12-25#summary
                                        \_______________________________________/
                                                            |
                                                  absolute_path_reference

        :return: combined string values of path, query and fragment parts or
            :py:const:`None`
        :rtype: percent-encoded string or :py:const:`None`
        """
        if self._path is None:
            return None
        u = [str(self._path)]
        if self._query is not None:
            u += ["?", str(self._query)]
        if self._fragment is not None:
            u += ["#", percent_encode(self._fragment)]
        return "".join(u)

    def _merge_path(self, relative_path_reference):
        relative_path_reference = Path._cast(relative_path_reference)
        if self._authority is not None and not self._path:
            return Path("/" + str(relative_path_reference))
        else:
            if "/" in self._path.string:
                segments = self._path.segments
                segments[-1] = ""
                return Path("/".join(segments) + str(relative_path_reference))
            else:
                return relative_path_reference

    def resolve(self, reference, strict=True):
        """ Transform a reference relative to this URI to produce a full target
        URI.

        .. seealso::
            `RFC 3986 § 5.2.2`_

        .. _`RFC 3986 § 5.2.2`: http://tools.ietf.org/html/rfc3986#section-5.2.2
        """
        if reference is None:
            return None
        reference = self._cast(reference)
        target = URI(None)
        if not strict and reference._scheme == self._scheme:
            reference_scheme = None
        else:
            reference_scheme = reference._scheme
        if reference_scheme is not None:
            target._scheme = reference_scheme
            target._authority = reference._authority
            target._path = reference._path.remove_dot_segments()
            target._query = reference._query
        else:
            if reference._authority is not None:
                target._authority = reference._authority
                target._path = reference._path.remove_dot_segments()
                target._query = reference._query
            else:
                if not reference.path:
                    target._path = self._path
                    if reference._query is not None:
                        target._query = reference._query
                    else:
                        target._query = self._query
                else:
                    if str(reference._path).startswith("/"):
                        target._path = reference._path.remove_dot_segments()
                    else:
                        target._path = self._merge_path(reference._path)
                        target._path = target._path.remove_dot_segments()
                    target._query = reference._query
                target._authority = self._authority
            target._scheme = self._scheme
        target._fragment = reference._fragment
        return target


class URITemplate(_Part):
    """A URI Template is a compact sequence of characters for describing a
    range of Uniform Resource Identifiers through variable expansion.
    
    This class exposes a full implementation of RFC6570.
    """

    @classmethod
    def _cast(cls, obj):
        if obj is None:
            return cls(None)
        elif isinstance(obj, cls):
            return obj
        else:
            return cls(str(obj))

    class _Expander(object):

        _operators = set("+#./;?&")

        def __init__(self, values):
            self.values = values

        def collect(self, *keys):
            """ Fetch a list of all values matching the keys supplied,
            returning (key, value) pairs for each.
            """
            items = []
            for key in keys:
                if key.endswith("*"):
                    key, explode = key[:-1], True
                else:
                    explode = False
                if ":" in key:
                    key, max_length = key.partition(":")[0::2]
                    max_length = int(max_length)
                else:
                    max_length = None
                value = self.values.get(key)
                if isinstance(value, dict):
                    if not value:
                        items.append((key, None))
                    elif explode:
                        items.extend((key, _) for _ in value.items())
                    else:
                        items.append((key, value))
                elif isinstance(value, (tuple, list)):
                    if explode:
                        items.extend((key, _) for _ in value)
                    else:
                        items.append((key, list(value)))
                elif max_length is not None:
                    items.append((key, value[:max_length]))
                else:
                    items.append((key, value))
            return [(key, value) for key, value in items if value is not None]

        def _expand(self, expression, safe=None, prefix="", separator=",",
                    with_keys=False, trim_empty_equals=False):
            items = self.collect(*expression.split(","))
            encode = lambda x: percent_encode(x, safe)
            for i, (key, value) in enumerate(items):
                if isinstance(value, tuple):
                    items[i] = "=".join(map(encode, value))
                else:
                    if isinstance(value, dict):
                        items[i] = ",".join(",".join(map(encode, item))
                                            for item in value.items())
                    elif isinstance(value, list):
                        items[i] = ",".join(map(encode, value))
                    else:
                        items[i] = encode(value)
                    if with_keys:
                        if items[i] is None or (items[i] == "" and
                                                trim_empty_equals):
                            items[i] = encode(key)
                        else:
                            items[i] = encode(key) + "=" + (items[i] or "")
            out = []
            for i, item in enumerate(items):
                out.append(prefix if i == 0 else separator)
                out.append(item)
            return "".join(out)

        def expand(self, expression):
            """ Dispatch to the correct expansion method.
            """
            if not expression:
                return ""
            if expression[0] in self._operators:
                operator, expression = expression[0], expression[1:]
                if operator == "+":
                    return self._expand(expression, reserved)
                elif operator == "#":
                    return self._expand(expression, reserved, prefix="#")
                elif operator == ".":
                    return self._expand(expression, prefix=".", separator=".")
                elif operator == "/":
                    return self._expand(expression, prefix="/", separator="/")
                elif operator == ";":
                    return self._expand(expression, prefix=";", separator=";",
                                        with_keys=True, trim_empty_equals=True)
                elif operator == "?":
                    return self._expand(expression, prefix="?", separator="&",
                                        with_keys=True)
                elif operator == "&":
                    return self._expand(expression, prefix="&", separator="&",
                                        with_keys=True)
            else:
                return self._expand(expression)

    _tokeniser = re.compile("(\{)([^{}]*)(\})")

    def __init__(self, template):
        super(URITemplate, self).__init__()
        self._template = template

    def __eq__(self, other):
        other = self._cast(other)
        return self._template == other._template

    def __ne__(self, other):
        other = self._cast(other)
        return self._template != other._template

    def __hash__(self):
        return hash(self.string)

    @property
    def string(self):
        if self._template is None:
            return None
        return str(self._template)

    def expand(self, **values):
        """ Expand into a URI using the values supplied
        """
        if self._template is None:
            return URI(None)
        tokens = self._tokeniser.split(self._template)
        expander = URITemplate._Expander(values)
        out = []
        while tokens:
            token = tokens.pop(0)
            if token == "{":
                expression = tokens.pop(0)
                tokens.pop(0)
                out.append(expander.expand(expression))
            else:
                out.append(token)
        return URI("".join(out))
