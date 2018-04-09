#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2018, Nigel Small
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


from io import StringIO

from py2neo.cypher.writing import cypher_str, cypher_repr
from py2neo.internal.compat import integer_types, numeric_types, string_types, ustr


def html_escape(s):
    return (s.replace(u"&", u"&amp;")
             .replace(u"<", u"&lt;")
             .replace(u">", u"&gt;")
             .replace(u'"', u"&quot;")
             .replace(u"'", u"&#039;"))


class DataList(list):
    """ Immutable list of data records.
    """

    def __init__(self, records, keys=None):
        super(DataList, self).__init__(map(tuple, records))
        if keys is None:
            try:
                k = records.keys()
            except AttributeError:
                raise ValueError("Missing keys")
        else:
            k = list(map(ustr, keys))
        width = len(k)
        t = [set() for _ in range(width)]
        o = [False] * width
        for record in self:
            for i, value in enumerate(record):
                if value is None:
                    o[i] = True
                else:
                    t[i].add(type(value))
        f = []
        for i, _ in enumerate(k):
            f.append({
                "type": t[i].copy().pop() if len(t[i]) == 1 else tuple(t[i]),
                "numeric": all(t_ in numeric_types for t_ in t[i]),
                "optional": o[i],
            })
        self._keys = k
        self._fields = f

    def __repr__(self):
        s = StringIO()
        self.write(file=s, header=True)
        return s.getvalue()

    def _repr_html_(self):
        s = StringIO()
        self.write_html(file=s, header=True)
        return s.getvalue()

    def keys(self):
        return list(self._keys)

    def field(self, key):
        if isinstance(key, integer_types):
            return self._fields[key]
        elif isinstance(key, string_types):
            try:
                index = self._keys.index(key)
            except ValueError:
                raise KeyError(key)
            else:
                return self._fields[index]
        else:
            raise TypeError(key)

    def _range(self, skip, limit):
        if skip is None:
            skip = 0
        if limit is None or skip + limit > len(self):
            return range(skip, len(self))
        else:
            return range(skip, skip + limit)

    def write(self, file=None, header=None, skip=None, limit=None, auto_align=True,
              padding=1, separator=u"|", newline=u"\r\n"):
        """ Write data to a human-readable table.

        :param file:
        :param header:
        :param skip:
        :param limit:
        :param auto_align:
        :param padding:
        :param separator:
        :param newline:
        :return:
        """
        from click import secho

        space = u" " * padding
        widths = [0] * len(self._keys)
        header_styles = {}
        if header and isinstance(header, dict):
            header_styles.update(header)

        def calc_widths(values, **_):
            strings = [cypher_str(value).splitlines(False) for value in values]
            for i, s in enumerate(strings):
                w = max(map(len, s)) if s else 0
                if w > widths[i]:
                    widths[i] = w

        def write_line(values, underline=None, **styles):
            strings = [cypher_str(value).splitlines(False) for value in values]
            height = max(map(len, strings)) if strings else 1
            for y in range(height):
                line_text = u""
                for x, _ in enumerate(values):
                    try:
                        text = strings[x][y]
                    except IndexError:
                        text = u""
                    if auto_align and self._fields[x]["numeric"]:
                        text = space + text.rjust(widths[x]) + space
                    else:
                        text = space + text.ljust(widths[x]) + space
                    if x > 0:
                        text = separator + text
                    line_text += text
                if underline:
                    line_text += newline + underline * len(line_text)
                line_text += newline
                secho(line_text, file, nl=False, **styles)

        def apply(f):
            count = 0
            for count, index in enumerate(self._range(skip, limit), start=1):
                if count == 1 and header:
                    f(self.keys(), underline=u"-", **header_styles)
                f(self[index])
            return count

        apply(calc_widths)
        return apply(write_line)

    def write_html(self, file=None, header=None, skip=None, limit=None, auto_align=True):
        """ Write data to an HTML table.

        :param file:
        :param header:
        :param skip:
        :param limit:
        :param auto_align:
        :return:
        """
        from click import echo

        def write_tr(values, tag):
            echo(u"<tr>", file, nl=False)
            for i, value in enumerate(values):
                if auto_align and self._fields[i]["numeric"]:
                    template = u'<{} style="text-align:right">{}</{}>'
                else:
                    template = u'<{} style="text-align:left">{}</{}>'
                echo(template.format(tag, html_escape(cypher_str(value)), tag), file, nl=False)
            echo(u"</tr>", file, nl=False)

        count = 0
        echo(u"<table>", file, nl=False)
        for count, index in enumerate(self._range(skip, limit), start=1):
            if count == 1 and header:
                write_tr(self.keys(), u"th")
            write_tr(self[index], u"td")
        echo(u"</table>", file, nl=False)
        return count

    def write_separated_values(self, separator, file=None, header=None, skip=None, limit=None,
                               newline=u"\r\n", quote=u"\""):
        """ Write data to a delimiter-separated file.

        :param separator:
        :param file
        :param header:
        :param skip:
        :param limit:
        :param newline:
        :param quote:
        :return:
        """
        from click import secho

        escaped_quote = quote + quote
        quotable = separator + newline + quote
        header_styles = {}
        if header and isinstance(header, dict):
            header_styles.update(header)

        def write_value(value, **styles):
            if value is None:
                return
            if isinstance(value, string_types):
                value = ustr(value)
                if any(ch in value for ch in quotable):
                    value = quote + value.replace(quote, escaped_quote) + quote
            else:
                value = cypher_repr(value)
            secho(value, file, nl=False, **styles)

        def write_line(values, **styles):
            for i, value in enumerate(values):
                if i > 0:
                    secho(separator, file, nl=False, **styles)
                write_value(value, **styles)
            secho(newline, file, nl=False, **styles)

        def apply(f):
            count = 0
            for count, index in enumerate(self._range(skip, limit), start=1):
                if count == 1 and header:
                    f(self.keys(), underline=u"-", **header_styles)
                f(self[index])
            return count

        return apply(write_line)

    def write_csv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as RFC4180-compatible comma-separated values.

        :param file
        :param header:
        :param skip:
        :param limit:
        :return:
        """
        return self.write_separated_values(u",", file, header, skip, limit)

    def write_tsv(self, file=None, header=None, skip=None, limit=None):
        """ Write the data as tab-separated values.

        :param file
        :param header:
        :param skip:
        :param limit:
        :return:
        """
        return self.write_separated_values(u"\t", file, header, skip, limit)
