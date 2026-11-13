"""Byte-level, streaming CSV logical-record scanner.

The scanner never holds more than one chunk plus one logical record, so a
multi-line quoted note is assembled as bytes and carries the exact half-open
byte interval [start, end) including its terminator.  Physical line numbers
are 1-based and count newline sequences everywhere, including inside quoted
fields.

Quote, delimiter and newline bytes are ASCII.  Markup bytes are chosen so
that they cannot appear as continuation bytes of a multi-byte character in
either supported source encoding:

* UTF-8 continuation bytes are 0x80-0xBF, never ASCII.
* GB18030 trail bytes are 0x40-0xFE, two-byte leads 0x81-0xFE, and the four
  byte form uses digits 0x30-0x39 in positions 2 and 4.

So `,` (0x2C), `;` (0x3B) and TAB (0x09) are safe in both.  These are the
default candidates.  A byte in 0x40-0xFE such as `|` (0x7C) can be a GB18030
trail byte; if one is explicitly configured as the delimiter, a split inside a
character surfaces as a strict decode failure (isolation evidence), never as
silently corrupted text.
"""

from dataclasses import dataclass


CHUNK_SIZE = 64 * 1024
QUOTE = 0x22
CR = 0x0D
LF = 0x0A
NEWLINES = (CR, LF)


@dataclass(frozen=True)
class RecordSlice:
    ordinal: int
    start: int
    end: int
    line_start: int
    line_end: int
    raw: bytes
    terminated: bool
    balanced: bool


class _Reader:
    """One-byte view over a chunked stream with absolute offset tracking."""

    __slots__ = ('_stream', '_size', '_buffer', '_index', '_offset')

    def __init__(self, stream, chunk_size=CHUNK_SIZE):
        if chunk_size < 1:
            raise ValueError('chunk size must be positive')
        self._stream = stream
        self._size = chunk_size
        self._buffer = b''
        self._index = 0
        self._offset = stream.tell() if hasattr(stream, 'tell') else 0

    @property
    def offset(self):
        return self._offset

    def _fill(self):
        if self._index < len(self._buffer):
            return True
        chunk = self._stream.read(self._size)
        if not chunk:
            self._buffer = b''
            self._index = 0
            return False
        self._buffer = chunk
        self._index = 0
        return True

    def peek(self):
        return self._buffer[self._index] if self._fill() else None

    def get(self):
        if not self._fill():
            return None
        value = self._buffer[self._index]
        self._index += 1
        self._offset += 1
        return value


def scan(stream, delimiter, chunk_size=CHUNK_SIZE, first_ordinal=1, start_line=1):
    """Yield logical records from ``stream`` positioned at ``start_line``.

    ``delimiter`` is a single ASCII byte.  The caller is responsible for
    seeking past a leading BOM; the first yielded record then starts at that
    offset.
    """
    if not isinstance(delimiter, int):
        delimiter = delimiter.encode('ascii')
        if len(delimiter) != 1:
            raise ValueError('delimiter must be a single ASCII byte')
        delimiter = delimiter[0]
    reader = _Reader(stream, chunk_size)
    line = start_line
    ordinal = first_ordinal
    while reader.peek() is not None:
        start = reader.offset
        line_start = line
        buffer = bytearray()
        field_start = True
        in_quotes = False
        terminated = False
        while True:
            value = reader.get()
            if value is None:
                break
            buffer.append(value)
            if value in NEWLINES:
                if value == CR and reader.peek() == LF:
                    buffer.append(reader.get())
                line += 1
                if not in_quotes:
                    terminated = True
                    break
                continue
            if in_quotes:
                if value == QUOTE:
                    if reader.peek() == QUOTE:
                        buffer.append(reader.get())
                    else:
                        in_quotes = False
                continue
            if value == QUOTE and field_start:
                in_quotes = True
                field_start = False
                continue
            if value == delimiter:
                field_start = True
                continue
            field_start = False
        line_end = line - 1 if terminated else line
        yield RecordSlice(ordinal, start, reader.offset, line_start, line_end,
                          bytes(buffer), terminated, not in_quotes)
        ordinal += 1
