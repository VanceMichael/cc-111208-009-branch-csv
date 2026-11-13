"""Source identity, byte storage and candidate dialect resolution.

Sources are content addressed: the identity is the SHA-256 of the complete
received bytes, never a path or filename.  Bytes are streamed into the state
directory before any decoding, so a review can always ask for a byte range of
the exact bytes that were received.

Dialect resolution reads a bounded sample only.  A candidate (encoding,
delimiter) pair is legal only when the sample decodes strictly (no
replacement, no ignored error) and its first record splits into at least two
fields.  A unique legal candidate is selected; more than one legal candidate
is reported as ambiguity instead of guessing.
"""

import codecs
import csv
import hashlib
import io
import json
import os
from dataclasses import dataclass
from pathlib import Path
import re
import tempfile

from .scan import scan


CHUNK_SIZE = 1024 * 1024
SAMPLE_BYTES = 4096
CANDIDATE_ENCODINGS = ('utf-8-sig', 'gb18030')
CANDIDATE_DELIMITERS = (',', ';', '\t')
QUOTE = '"'
REQUIRED_FIELDS = ('bookingId', 'date', 'region', 'note')
_FIELD_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


@dataclass(frozen=True)
class Source:
    digest: str
    size: int
    path: Path

    def read(self, start, end):
        if start < 0 or end < start or end > self.size:
            raise ValueError('Byte range [%d,%d) is outside [0,%d)' % (start, end, self.size))
        with open(self.path, 'rb') as stream:
            stream.seek(start)
            data = stream.read(end - start)
        if len(data) != end - start:
            raise ValueError('Short read for byte range [%d,%d)' % (start, end))
        return data


def ingest(path, root):
    """Stream a received file into the content-addressed blob store.

    Digest and stored bytes come from the same chunked pass; the file is never
    loaded whole.  Re-receiving identical bytes is stable.
    """
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    descriptor, name = tempfile.mkstemp(dir=root)
    try:
        with os.fdopen(descriptor, 'wb') as target:
            with open(path, 'rb') as stream:
                while True:
                    chunk = stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    digest.update(chunk)
                    size += len(chunk)
                    target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        hexdigest = digest.hexdigest()
        destination = root / hexdigest
        if destination.exists():
            os.unlink(name)
        else:
            os.replace(name, destination)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return Source(digest=hexdigest, size=size, path=destination)


def open_source(database, root, digest):
    row = database.execute('SELECT size FROM sources WHERE digest = ? LIMIT 1',
                           (digest,)).fetchone()
    if row is None:
        raise ValueError('Source digest is not registered: %s' % digest)
    path = Path(root) / digest
    if not path.is_file():
        raise ValueError('Source bytes are missing for digest: %s' % digest)
    size = path.stat().st_size
    if size != row[0]:
        raise ValueError('Source size mismatch for digest: %s' % digest)
    return Source(digest=digest, size=size, path=path)


@dataclass(frozen=True)
class Selection:
    encoding: str
    delimiter: str
    header_bytes: bytes
    header_fields: tuple
    header_start: int
    header_end: int
    header_line_end: int
    bom_length: int = 0
    mapping_version: str = ''

    @property
    def base_encoding(self):
        # Record decoding must not strip a BOM that appears mid-file.
        return 'utf-8' if self.encoding == 'utf-8-sig' else self.encoding

    @property
    def data_start(self):
        return self.header_end

    @property
    def data_line(self):
        return self.header_line_end + 1

    @property
    def field_index(self):
        return {name: self.header_fields.index(name) for name in REQUIRED_FIELDS}


def mapping_identity(encoding, delimiter, header_fields):
    """Identity of one confirmed reading of a source's logical schema.

    Built from the confirmed field names, the codec and the delimiter.  Byte
    formatting of the header line (CRLF vs LF, quoting) does not change the
    logical mapping; field order and spelling do.
    """
    material = (encoding + '\x00' + delimiter + '\x00'
                + '\x1f'.join(header_fields)).encode('utf-8')
    return 'v1:' + hashlib.sha256(material).hexdigest()


def read_sample(path, limit=SAMPLE_BYTES):
    """Bounded start-of-file sample plus whether it is the complete file."""
    with open(path, 'rb') as stream:
        sample = stream.read(limit)
        complete = stream.read(1) == b''
    return sample, complete


def _decode_sample(raw, encoding, complete):
    try:
        return codecs.getincrementaldecoder(encoding)(errors='strict').decode(raw, complete)
    except UnicodeDecodeError as error:
        raise ValueError('decode_error at byte %d: %s' % (error.start, error.reason))


def _bom(raw, encoding):
    if encoding == 'utf-8-sig' and raw.startswith(codecs.BOM_UTF8):
        return len(codecs.BOM_UTF8)
    return 0


def _split_header(text, delimiter):
    try:
        record = next(csv.reader([text], delimiter=delimiter, quotechar=QUOTE,
                                 doublequote=True, strict=True))
    except StopIteration:
        return None, 'no first record'
    except csv.Error as error:
        return None, 'csv_error in header: %s' % error
    if len(record) < 2:
        return None, 'delimiter %r does not split the header' % (delimiter,)
    return record, None


def validate_header(fields):
    """Mapping v1 requires exactly the four known columns, each once."""
    if len(fields) != len(REQUIRED_FIELDS):
        raise ValueError('mapping_unresolved: header has %d fields, expected %s'
                         % (len(fields), ', '.join(REQUIRED_FIELDS)))
    duplicates = sorted({name for name in fields if fields.count(name) > 1})
    if duplicates:
        raise ValueError('mapping_unresolved: duplicate header field(s) %s'
                         % ', '.join(duplicates))
    for name in REQUIRED_FIELDS:
        if name not in fields:
            raise ValueError('mapping_unresolved: missing header field %s' % name)
    for name in fields:
        if not _FIELD_NAME.match(name):
            raise ValueError('mapping_unresolved: invalid header field %r' % name)
    return {name: fields.index(name) for name in REQUIRED_FIELDS}


def _probe(payload, complete, encoding, delimiter):
    """Return the header record for one candidate, or a rejection reason."""
    try:
        _decode_sample(payload, encoding, complete)
    except ValueError as error:
        return None, str(error)
    bom = _bom(payload, encoding)
    if not payload[bom:].strip(b'\r\n\x00'):
        return None, 'empty document'
    reader = io.BytesIO(payload)
    reader.seek(bom)
    scanned = scan(reader, delimiter, first_ordinal=1, start_line=1, chunk_size=8192)
    record = next(scanned, None)
    if record is None:
        return None, 'no header record'
    # The scanner starts after the BOM, so its offsets are already absolute in
    # the sample; a reader of the original file lands on the same bytes.
    start = record.start
    end = record.end
    if not record.terminated and not complete:
        return None, ('header_line_too_long: header not complete within %d sample bytes'
                      % len(payload))
    if not record.balanced:
        return None, 'unbalanced_quote in header'
    try:
        text = record.raw.decode(encoding, errors='strict')
    except UnicodeDecodeError as error:
        return None, ('decode_error at byte %d: %s'
                      % (start + error.start, error.reason))
    fields, reason = _split_header(text, delimiter)
    if fields is None:
        return None, reason
    return {'encoding': encoding, 'delimiter': delimiter,
            'header_bytes': record.raw,
            'header_fields': tuple(fields),
            'header_start': bom,
            'header_end': end,
            'header_line_end': record.line_end,
            'bom_length': bom}, None


def resolve(payload, encodings=None, delimiters=None, complete=None):
    """Choose the unique legal interpretation of a bounded sample, or refuse.

    No default comma and no decoder replacement is ever applied.  If more than
    one (encoding, delimiter) pair is legal the call raises; ASCII data
    decoding under both UTF-8 and GB18030 is the common ambiguity.
    complete=False means `payload` is a truncated sample: only a header record
    that terminates inside it is accepted.
    """
    names = list(encodings) if encodings is not None else list(CANDIDATE_ENCODINGS)
    separates = list(delimiters) if delimiters is not None else list(CANDIDATE_DELIMITERS)
    if complete is None:
        complete = True
    if not names:
        raise ValueError('dialect_unresolved: no encoding candidates supplied')
    if not separates:
        raise ValueError('dialect_unresolved: no delimiter candidates supplied')
    for name in names:
        codecs.lookup(name)
    for delimiter in separates:
        if not isinstance(delimiter, str) or len(delimiter) != 1 or delimiter in (QUOTE, '\r', '\n'):
            raise ValueError('unsupported delimiter: %r' % (delimiter,))
        if not 0x20 <= ord(delimiter) <= 0x7E and delimiter != '\t':
            raise ValueError('unsupported delimiter: %r is not printable ASCII or TAB'
                             % (delimiter,))
    legal, rejected = [], []
    for delimiter in separates:
        for encoding in names:
            found, reason = _probe(payload, complete, encoding, delimiter)
            if found:
                legal.append(found)
            else:
                rejected.append({'encoding': encoding, 'delimiter': delimiter,
                                 'reason': reason})
    if not legal:
        detail = '; '.join('%s/%s: %s' % (item['encoding'], item['delimiter'],
                                            item['reason']) for item in rejected[:6])
        raise ValueError('dialect_unresolved: %s' % detail)
    variants = []
    for candidate in legal:
        key = (candidate['encoding'], candidate['delimiter'])
        if not any(key == (variant['encoding'], variant['delimiter']) for variant in variants):
            variants.append(candidate)
    if len(variants) > 1:
        names = sorted({item['encoding'] for item in variants})
        separates = sorted({item['delimiter'] for item in variants})
        kinds = []
        if len(names) > 1:
            kinds.append('encoding')
        if len(separates) > 1:
            kinds.append('delimiter')
        label = '+'.join(kinds) + '_ambiguous'
        details = {'encodings': names, 'delimiters': separates,
                   'candidates': [{'encoding': item['encoding'],
                                   'delimiter': item['delimiter']}
                                  for item in variants]}
        raise ValueError('%s: %d legal candidates; specify encoding and delimiter: %s'
                         % (label, len(variants), json.dumps(details, sort_keys=True)))
    chosen = variants[0]
    return Selection(encoding=chosen['encoding'], delimiter=chosen['delimiter'],
                     header_bytes=chosen['header_bytes'],
                     header_fields=chosen['header_fields'],
                     header_start=chosen['header_start'], header_end=chosen['header_end'],
                     header_line_end=chosen['header_line_end'],
                     bom_length=chosen['bom_length'],
                     mapping_version=mapping_identity(chosen['encoding'],
                                                      chosen['delimiter'],
                                                      chosen['header_fields']))


def resolve_file(path, encodings=None, delimiters=None):
    """Resolve on a bounded sample without loading the whole file."""
    sample, complete = read_sample(path)
    return resolve(sample, encodings, delimiters, complete=complete)
