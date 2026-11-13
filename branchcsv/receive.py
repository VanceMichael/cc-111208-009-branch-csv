"""Formal incremental reception: records, isolation, review and checkpoints.

Invariants:

* Original bytes are never modified.  A source is the SHA-256 of its complete
  bytes; replacing the file makes it a different source with a different
  identity.
* Every logical record keeps its half-open byte interval [start, end) and its
  1-based physical line range.  A checkpoint stores only a *complete* logical
  record boundary, so a crash can never resume inside a quoted multi-line
  field.
* A review decision is an immutable field overlay keyed by (source digest,
  record byte interval, mapping version).  Changing any of those makes the old
  decision inapplicable; it is never silently reused.
* Same booking id with identical canonical content is idempotent; different
  content is isolated as a conflict rather than last-write-wins.
* One logical record, its business or isolation outcome and its checkpoint are
  written in a single SQLite transaction.
"""

import codecs
import csv
import datetime
import hashlib
import io
import json
from pathlib import Path
import sqlite3

from .scan import scan
from .source import QUOTE, ingest, open_source, resolve_file
from .source import REQUIRED_FIELDS, validate_header


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    size INTEGER NOT NULL,
    encoding TEXT NOT NULL,
    delimiter TEXT NOT NULL,
    header_json TEXT NOT NULL,
    PRIMARY KEY (digest, mapping_version)
);
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    resumed_from INTEGER,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accepted (
    run_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    booking_id TEXT NOT NULL,
    date TEXT NOT NULL,
    region TEXT NOT NULL,
    note TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    line_start INTEGER NOT NULL,
    line_end INTEGER NOT NULL,
    duplicate INTEGER NOT NULL,
    PRIMARY KEY (run_id, ordinal)
);
CREATE TABLE IF NOT EXISTS bookings (
    booking_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    date TEXT NOT NULL,
    region TEXT NOT NULL,
    note TEXT NOT NULL,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS isolated (
    run_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    line_start INTEGER NOT NULL,
    line_end INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    candidate_json TEXT NOT NULL,
    PRIMARY KEY (run_id, ordinal)
);
CREATE INDEX IF NOT EXISTS isolated_identity
    ON isolated(digest, mapping_version, start, end);
CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    field TEXT NOT NULL,
    value TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    reason TEXT NOT NULL,
    original_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (digest, mapping_version, start, end, field)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    run_id INTEGER PRIMARY KEY,
    digest TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    next_start INTEGER NOT NULL,
    next_line INTEGER NOT NULL,
    next_ordinal INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _now():
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def canonical(values):
    """Canonical content of one booking; stable for equal business content."""
    payload = json.dumps({name: values[name] for name in REQUIRED_FIELDS},
                         ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _valid_date(value):
    if len(value) != 10:
        return False
    try:
        return datetime.date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _parse(raw, start, balanced, terminated, encoding, delimiter):
    """Return (values or None, (reason, detail) or None).

    A final record without a trailing newline is still a legal CSV record; an
    unterminated quoted field is not, because its extent cannot be proven.
    """
    if not balanced:
        return None, ('unbalanced_quote', 'quoted field does not close before end of source')
    try:
        text = raw.decode(encoding, errors='strict')
    except UnicodeDecodeError as error:
        return None, ('decode_error',
                      'byte offset %d: %s' % (start + error.start, error.reason))
    try:
        rows = list(csv.reader(io.StringIO(text, newline=''), delimiter=delimiter,
                               quotechar=QUOTE, doublequote=True, strict=True))
    except csv.Error as error:
        return None, ('csv_error', str(error))
    if len(rows) != 1:
        return None, ('record_span', 'record parsed as %d CSV records' % len(rows))
    return rows[0], None


class _Transaction:
    """Explicit single-transaction scope for one complete logical record."""

    def __init__(self, database):
        self.database = database

    def __enter__(self):
        self.database.execute('BEGIN IMMEDIATE')
        return self.database

    def __exit__(self, kind, value, traceback):
        if kind is None:
            self.database.execute('COMMIT')
            return False
        self.database.execute('ROLLBACK')
        return False


class Receiver:
    def __init__(self, root, database=None, chunk_size=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.blobs = self.root / 'blobs'
        self.blobs.mkdir(exist_ok=True)
        if database is None:
            database = sqlite3.connect(self.root / 'receive.sqlite3', isolation_level=None)
        self.database = database
        self.database.execute('PRAGMA journal_mode = WAL')
        self.database.execute('PRAGMA synchronous = FULL')
        self.database.execute('PRAGMA foreign_keys = ON')
        if self.database.execute('PRAGMA busy_timeout').fetchone()[0] == 0:
            self.database.execute('PRAGMA busy_timeout = 5000')
        self.database.executescript(SCHEMA)
        self.chunk_size = chunk_size

    def _transaction(self):
        return _Transaction(self.database)

    def close(self):
        self.database.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- source registration -------------------------------------------

    def register(self, path, encodings=None, delimiters=None):
        """Ingest bytes and pin (encoding, delimiter, header) for this digest.

        The dialect sample is taken from the stored blob, not from `path`, so
        the mapping always describes the exact bytes the digest names.  The
        same bytes may be read under more than one mapping; each is stored as
        its own row and yields its own mapping version.
        """
        source = ingest(path, self.blobs)
        selection = resolve_file(source.path, encodings, delimiters)
        validate_header(list(selection.header_fields))
        with self._transaction():
            self.database.execute(
                'INSERT OR IGNORE INTO sources VALUES (?,?,?,?,?,?)',
                (source.digest, selection.mapping_version, source.size,
                 selection.encoding, selection.delimiter,
                 json.dumps(list(selection.header_fields))))
        row = self.database.execute(
            'SELECT size, encoding, delimiter, header_json FROM sources '
            'WHERE digest = ? AND mapping_version = ?',
            (source.digest, selection.mapping_version)).fetchone()
        if row[0] != source.size:
            raise ValueError('source_mapping_conflict: digest %s is already registered as '
                             'size %d, not %d' % (source.digest, row[0], source.size))
        return source, selection

    def source(self, digest):
        return open_source(self.database, self.blobs, digest)

    def mappings(self, digest):
        rows = self.database.execute(
            'SELECT mapping_version, encoding, delimiter, header_json FROM sources '
            'WHERE digest = ? ORDER BY mapping_version', (digest,)).fetchall()
        return [{'mapping_version': row[0], 'encoding': row[1], 'delimiter': row[2],
                 'header': json.loads(row[3])} for row in rows]

    def selection(self, digest, mapping_version=None):
        rows = self.database.execute(
            'SELECT mapping_version, encoding, delimiter FROM sources WHERE digest = ? '
            + ('AND mapping_version = ? ' if mapping_version else '')
            + 'ORDER BY mapping_version',
            (digest, mapping_version) if mapping_version else (digest,)).fetchall()
        if not rows:
            raise ValueError('Source digest is not registered: %s' % digest)
        if mapping_version is None and len(rows) > 1:
            raise ValueError('mapping_ambiguous: digest %s has %d mappings; specify one'
                             % (digest, len(rows)))
        version, encoding, delimiter = rows[0]
        source = self.source(digest)
        selection = resolve_file(source.path, [encoding], [delimiter])
        if selection.mapping_version != version:
            raise ValueError('source_mapping_conflict: stored mapping %s no longer matches '
                             'the source bytes' % version)
        return selection

    # ---- reception -------------------------------------------------------

    def run(self, digest, resume=False, stop_after=None, after_record=None,
            mapping_version=None):
        if mapping_version is None:
            rows = self.database.execute(
                'SELECT mapping_version FROM sources WHERE digest = ? '
                'ORDER BY mapping_version', (digest,)).fetchall()
            if not rows:
                raise ValueError('Source digest is not registered: %s' % digest)
            if len(rows) > 1:
                raise ValueError('mapping_ambiguous: digest %s has %d mappings; specify one'
                                 % (digest, len(rows)))
            mapping_version = rows[0][0]
        selection = self.selection(digest, mapping_version)
        source = self.source(digest)
        checkpoint = None
        if resume:
            # A killed process leaves its run row as 'running'; an explicit
            # stop leaves 'interrupted'.  Both name a complete-record boundary.
            checkpoint = self.database.execute(
                'SELECT c.run_id, c.next_start, c.next_line, c.next_ordinal '
                'FROM checkpoints AS c JOIN runs AS r ON r.run_id = c.run_id '
                'WHERE c.digest = ? AND c.mapping_version = ? '
                'AND r.status IN (?, ?) '
                'ORDER BY c.run_id DESC LIMIT 1',
                (digest, mapping_version, 'interrupted', 'running')).fetchone()
            if checkpoint is None:
                raise ValueError('checkpoint_missing: no unfinished checkpoint for this '
                                 'source and mapping version')
        with self._transaction():
            run_id = self.database.execute(
                'INSERT INTO runs (digest, mapping_version, resumed_from, status, created_at) '
                'VALUES (?,?,?,?,?)',
                (digest, mapping_version, checkpoint[0] if checkpoint else None,
                 'running', _now())).lastrowid
        next_start, next_line, next_ordinal = selection.data_start, selection.data_line, 2
        if checkpoint:
            next_start, next_line, next_ordinal = checkpoint[1], checkpoint[2], checkpoint[3]
        counts = {'accepted': 0, 'duplicate': 0, 'isolated': 0, 'header_skipped': 0,
                  'reviewed': 0}
        consumed = 0
        interrupted = False
        with open(source.path, 'rb') as stream:
            stream.seek(next_start)
            records = scan(stream, selection.delimiter, first_ordinal=next_ordinal,
                           start_line=next_line,
                           chunk_size=self.chunk_size or 64 * 1024)
            for record in records:
                # Business row or isolation evidence and the check point share
                # one transaction: either all of this record happened or none.
                with self._transaction():
                    outcome = self._consume(run_id, source.digest, mapping_version,
                                            selection, record, counts)
                    self._checkpoint(run_id, digest, mapping_version, record)
                consumed += 1
                if after_record is not None:
                    after_record(record, outcome)
                if outcome is None:
                    break
                if stop_after is not None and consumed >= stop_after:
                    interrupted = True
                    break
        with self._transaction():
            self.database.execute(
                'UPDATE runs SET status = ? WHERE run_id = ?',
                ('interrupted' if interrupted else 'complete', run_id))
        return {'run_id': run_id, 'digest': digest, 'mapping_version': mapping_version,
                'status': 'interrupted' if interrupted else 'complete',
                'counts': counts, 'resumed_from': checkpoint[0] if checkpoint else None}

    def _consume(self, run_id, digest, mapping_version, selection, record, counts):
        """Write one record's outcome into the open transaction.

        Returns the outcome name, or None when the record is structurally
        broken (unbalanced quote) and no further record can be trusted.
        """
        if not record.balanced:
            self._isolate(run_id, record, digest, mapping_version, 'unbalanced_quote',
                          'quoted field does not close before end of source',
                          _preview(record.raw, selection))
            counts['isolated'] += 1
            return None
        header_state, header_detail = self._header_like(record, selection)
        if header_state is True:
            counts['header_skipped'] += 1
            return 'header_skipped'
        if header_state is False:
            self._isolate(run_id, record, digest, mapping_version, 'header_mismatch',
                          header_detail, _preview(record.raw, selection))
            counts['isolated'] += 1
            return 'header_mismatch'
        if codecs.BOM_UTF8 in record.raw:
            # Records start after the header, so a UTF-8 BOM here is mid-file.
            self._isolate(run_id, record, digest, mapping_version, 'mid_file_bom',
                          'BOM is only skipped at the start of the file',
                          _preview(record.raw, selection))
            counts['isolated'] += 1
            return 'mid_file_bom'
        values, problem = _parse(record.raw, record.start, record.balanced,
                                 record.terminated, selection.base_encoding,
                                 selection.delimiter)
        if problem:
            self._isolate(run_id, record, digest, mapping_version, problem[0], problem[1],
                          _preview(record.raw, selection))
            counts['isolated'] += 1
            return 'isolated'
        values, reviewed = self._overlay(digest, mapping_version, selection, record, values)
        if reviewed:
            counts['reviewed'] += 1
        if len(values) != len(selection.header_fields):
            self._isolate(run_id, record, digest, mapping_version, 'width_mismatch',
                          'record has %d fields, header has %d'
                          % (len(values), len(selection.header_fields)), values)
            counts['isolated'] += 1
            return 'width_mismatch'
        mapping = dict(zip(selection.header_fields, values))
        if not mapping['bookingId']:
            self._isolate(run_id, record, digest, mapping_version, 'missing_booking_id',
                          'bookingId is empty', mapping)
            counts['isolated'] += 1
            return 'missing_booking_id'
        if not _valid_date(mapping['date']):
            self._isolate(run_id, record, digest, mapping_version, 'bad_date',
                          'date %r is not ISO YYYY-MM-DD' % mapping['date'], mapping)
            counts['isolated'] += 1
            return 'bad_date'
        content = canonical(mapping)
        existing = self.database.execute(
            'SELECT content_hash, start, end FROM bookings WHERE booking_id = ?',
            (mapping['bookingId'],)).fetchone()
        if existing is not None and existing[0] == content:
            self._accept(run_id, record, digest, mapping_version, mapping, content, True)
            counts['duplicate'] += 1
            return 'duplicate'
        if existing is not None:
            self._isolate(run_id, record, digest, mapping_version, 'conflict',
                          'bookingId %s already has different canonical content at [%d,%d)'
                          % (mapping['bookingId'], existing[1], existing[2]), mapping)
            counts['isolated'] += 1
            return 'conflict'
        self._accept(run_id, record, digest, mapping_version, mapping, content, False)
        counts['accepted'] += 1
        return 'accepted'

    def _header_like(self, record, selection):
        """(True, None) exact duplicate header; (False, detail) look-alike."""
        if not record.terminated or not record.balanced:
            return None, None
        try:
            text = record.raw.decode(selection.base_encoding, errors='strict')
            rows = list(csv.reader(io.StringIO(text, newline=''),
                                   delimiter=selection.delimiter, quotechar=QUOTE,
                                   doublequote=True, strict=True))
        except (UnicodeDecodeError, csv.Error):
            return None, None
        if len(rows) != 1:
            return None, None
        fields = rows[0]
        if tuple(fields) == tuple(selection.header_fields):
            return True, None
        known = sum(1 for name in selection.header_fields if name in fields)
        if known >= 2 and fields and fields[0] == selection.header_fields[0]:
            return False, ('header_mismatch: mid-file header-like record at %d/%d known '
                           'fields is not identical to the confirmed header'
                           % (known, len(selection.header_fields)))
        return None, None

    def _overlay(self, digest, mapping_version, selection, record, values):
        rows = self.database.execute(
            'SELECT field, value FROM decisions WHERE digest = ? AND mapping_version = ? '
            'AND start = ? AND end = ? ORDER BY decision_id',
            (digest, mapping_version, record.start, record.end)).fetchall()
        if not rows:
            return list(values), False
        index = selection.field_index
        values = list(values)
        applied = False
        for field, value in rows:
            position = index.get(field)
            if position is None or position >= len(values):
                # The record is too narrow to carry this field; a decision
                # cannot invent a column, so it stays isolated.
                continue
            values[position] = value
            applied = True
        return values, applied

    def _accept(self, run_id, record, digest, mapping_version, mapping, content, duplicate):
        if not duplicate:
            self.database.execute(
                'INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?,?)',
                (mapping['bookingId'], content, mapping['date'], mapping['region'],
                 mapping['note'], digest, mapping_version, record.start, record.end))
        self.database.execute(
            'INSERT OR REPLACE INTO accepted VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (run_id, record.ordinal, digest, mapping_version, mapping['bookingId'],
             mapping['date'], mapping['region'], mapping['note'], content, record.start,
             record.end, record.line_start, record.line_end, int(duplicate)))

    def _isolate(self, run_id, record, digest, mapping_version, reason, detail, candidate):
        if isinstance(candidate, str):
            candidate = {'text': candidate}
        elif isinstance(candidate, list):
            candidate = {'fields': candidate}
        self.database.execute(
            'INSERT OR REPLACE INTO isolated VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (run_id, record.ordinal, digest, mapping_version, record.start, record.end,
             record.line_start, record.line_end, reason, detail,
             json.dumps(candidate, ensure_ascii=False, sort_keys=True)))

    def _checkpoint(self, run_id, digest, mapping_version, record):
        """Write the next-record boundary inside the record's transaction."""
        self.database.execute(
            'INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?,?,?)',
            (run_id, digest, mapping_version, record.end, record.line_end + 1,
             record.ordinal + 1, _now()))

    # ---- evidence and review --------------------------------------------

    def evidence(self, run_id):
        """Isolated records of one run, with byte/line ranges and raw slice."""
        rows = self.database.execute(
            'SELECT ordinal, digest, mapping_version, start, end, line_start, line_end, '
            'reason, detail, candidate_json FROM isolated WHERE run_id = ? ORDER BY ordinal',
            (run_id,)).fetchall()
        result = []
        for row in rows:
            source = self.source(row[1])
            result.append({
                'ordinal': row[0], 'digest': row[1], 'mapping_version': row[2],
                'byte_start': row[3], 'byte_end': row[4], 'line_start': row[5],
                'line_end': row[6], 'reason': row[7], 'detail': row[8],
                'slice_text': _display(source.read(row[3], row[4]),
                                       self._encoding(row[1], row[2])),
                'candidate': json.loads(row[9]) if row[9] else None,
                'decision': self.decision_status(row[1], row[2], row[3], row[4])})
        return result

    def _encoding(self, digest, mapping_version):
        row = self.database.execute(
            'SELECT encoding FROM sources WHERE digest = ? AND mapping_version = ?',
            (digest, mapping_version)).fetchone()
        if row is None:
            raise ValueError('source_mapping_conflict: unknown mapping %s for %s'
                             % (mapping_version, digest))
        return 'utf-8' if row[0] == 'utf-8-sig' else row[0]

    def slice(self, digest, start, end, display=True):
        """Original bytes for a half-open interval, exactly as received."""
        raw = self.source(digest).read(start, end)
        if not display:
            return raw
        row = self.database.execute(
            'SELECT encoding FROM sources WHERE digest = ? ORDER BY mapping_version',
            (digest,)).fetchone()
        if row is None:
            raise ValueError('Source digest is not registered: %s' % digest)
        return {'digest': digest, 'byte_start': start, 'byte_end': end,
                'hex': raw.hex(), 'text': _display(raw, 'utf-8' if row[0] == 'utf-8-sig'
                                                   else row[0])}

    def decision_status(self, digest, mapping_version, start, end):
        row = self.database.execute(
            'SELECT COUNT(*) FROM decisions WHERE digest = ? AND mapping_version = ? '
            'AND start = ? AND end = ?', (digest, mapping_version, start, end)).fetchone()
        return 'reviewed' if row[0] else 'pending'

    def submit_decision(self, digest, mapping_version, start, end, field, value,
                        reviewer, reason):
        """Record an immutable corrected field; original bytes are untouched.

        The decision applies only to this exact (digest, mapping version, byte
        interval).  A different file, mapping or record boundary yields a
        different key and leaves the record pending.  Re-running the same file
        with the same mapping reuses the decision.
        """
        if not reviewer or not reason:
            raise ValueError('reviewer and reason are required for a decision')
        if field not in REQUIRED_FIELDS:
            raise ValueError('decision_field_unknown: %r' % (field,))
        if not isinstance(value, str):
            raise ValueError('decision value must be a string')
        source = self.source(digest)
        isolated = self.database.execute(
            'SELECT candidate_json FROM isolated WHERE digest = ? AND mapping_version = ? '
            'AND start = ? AND end = ? ORDER BY run_id DESC LIMIT 1',
            (digest, mapping_version, start, end)).fetchone()
        if isolated is None:
            raise ValueError('decision_target_unknown: [%d,%d) of %s is not an isolated '
                             'record under mapping %s' % (start, end, digest, mapping_version))
        raw = source.read(start, end)
        original = self._record_values(raw, digest, mapping_version)
        try:
            with self._transaction():
                self.database.execute(
                    'INSERT INTO decisions (digest, mapping_version, start, end, field, value, '
                    'reviewer, reason, original_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (digest, mapping_version, start, end, field, value, reviewer, reason,
                     json.dumps(original, ensure_ascii=False, sort_keys=True), _now()))
        except sqlite3.IntegrityError:
            raise ValueError('decision_conflict: field %r already has a decision for this '
                             'record and version' % (field,))
        return {'digest': digest, 'mapping_version': mapping_version, 'start': start,
                'end': end, 'field': field, 'value': value, 'reviewer': reviewer,
                'reason': reason, 'original': original}

    def _record_values(self, raw, digest, mapping_version):
        row = self.database.execute(
            'SELECT encoding, delimiter, header_json FROM sources WHERE digest = ? AND '
            'mapping_version = ?', (digest, mapping_version)).fetchone()
        if row is None:
            raise ValueError('source_mapping_conflict: unknown mapping %s for %s'
                             % (mapping_version, digest))
        values, problem = _parse(raw, 0, True, True,
                                 'utf-8' if row[0] == 'utf-8-sig' else row[0], row[1])
        if problem:
            raise ValueError('decision_target_not_a_record: %s (%s)' % (problem[0], problem[1]))
        return values

    def decisions(self, digest=None):
        sql = ('SELECT digest, mapping_version, start, end, field, value, reviewer, reason, '
               'created_at FROM decisions')
        if digest:
            rows = self.database.execute(sql + ' WHERE digest = ? ORDER BY decision_id',
                                         (digest,)).fetchall()
        else:
            rows = self.database.execute(sql + ' ORDER BY decision_id').fetchall()
        return [{'digest': row[0], 'mapping_version': row[1], 'start': row[2], 'end': row[3],
                 'field': row[4], 'value': row[5], 'reviewer': row[6], 'reason': row[7],
                 'created_at': row[8]} for row in rows]

    def accepted(self, run_id):
        rows = self.database.execute(
            'SELECT ordinal, booking_id, date, region, note, content_hash, start, end, '
            'line_start, line_end, duplicate FROM accepted WHERE run_id = ? ORDER BY ordinal',
            (run_id,)).fetchall()
        return [{'ordinal': row[0], 'bookingId': row[1], 'date': row[2], 'region': row[3],
                 'note': row[4], 'content_hash': row[5], 'byte_start': row[6],
                 'byte_end': row[7], 'line_start': row[8], 'line_end': row[9],
                 'duplicate': bool(row[10])} for row in rows]


def _display(raw, encoding, limit=400):
    try:
        return raw.decode(encoding, errors='replace')[:limit]
    except LookupError:
        return raw.hex()[:limit * 2]


def _preview(raw, selection, limit=200):
    """Bounded display text for isolation evidence, never the authoritative bytes."""
    try:
        raw.decode(selection.base_encoding, errors='strict')
    except UnicodeDecodeError:
        return 'unavailable: bytes do not decode under %s' % selection.base_encoding
    return _display(raw, selection.base_encoding, limit)
