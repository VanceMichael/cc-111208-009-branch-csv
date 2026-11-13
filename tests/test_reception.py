import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

from branchcsv.receive import Receiver
from branchcsv.scan import scan
from branchcsv.source import resolve, resolve_file, validate_header
from scripts.materialize import main as materialize


class ReceptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.state = Path(self.directory.name) / 'state'
        self.state.mkdir()

    def tearDown(self):
        self.directory.cleanup()

    def receiver(self):
        return Receiver(self.state)

    def write(self, name, text, encoding='utf-8'):
        path = Path(self.directory.name) / name
        path.write_bytes(text.encode(encoding))
        return path

    # ---- dialect ---------------------------------------------------------

    def test_ascii_is_encoding_ambiguous_without_explicit_choice(self):
        payload = b'bookingId,date\r\nB1,2026-11-12\r\n'
        with self.assertRaisesRegex(ValueError, 'encoding_ambiguous'):
            resolve(payload)
        chosen = resolve(payload, encodings=['utf-8-sig'], delimiters=[','])
        self.assertEqual(chosen.encoding, 'utf-8-sig')

    def test_gb18030_only_candidate_is_selected_when_utf8_fails(self):
        payload = 'bookingId,date,region,note\r\nB1,2026-11-12,East,上门\r\n'.encode('gb18030')
        chosen = resolve(payload)
        self.assertEqual(chosen.encoding, 'gb18030')
        self.assertEqual(chosen.delimiter, ',')

    def test_delimiter_candidate_is_not_defaulted_to_comma(self):
        payload = b'a;b\r\n1;2\r\n'
        with self.assertRaisesRegex(ValueError, 'delimiter_ambiguous|encoding_ambiguous'):
            resolve(payload)
        chosen = resolve(payload, encodings=['utf-8-sig'], delimiters=[';'])
        self.assertEqual(chosen.delimiter, ';')

    def test_undecodable_source_reports_instead_of_replacing(self):
        path = self.write('broken.csv', '')
        path.write_bytes(b'bookingId,date,region,note\r\nB1,2026-11-12,E\xff\xfeast,x\r\n')
        with self.assertRaisesRegex(ValueError, 'dialect_unresolved'):
            resolve_file(path)

    def test_header_mapping_is_exactly_the_four_known_fields(self):
        self.assertEqual(sorted(validate_header(['note', 'region', 'date', 'bookingId'])),
                         ['bookingId', 'date', 'note', 'region'])
        for bad in (['bookingId', 'date', 'region'], ['bookingId', 'date', 'region', 'x'],
                    ['bookingId', 'bookingId', 'region', 'note']):
            with self.assertRaisesRegex(ValueError, 'mapping_unresolved'):
                validate_header(bad)

    # ---- byte ranges -----------------------------------------------------

    def test_multiline_record_byte_range_and_lines(self):
        path = self.write('m.csv',
                          'bookingId,date,region,note\r\n'
                          'B1,2026-11-12,East,"first\r\nsecond"\r\n')
        raw = path.read_bytes()
        with open(path, 'rb') as stream:
            records = list(scan(stream, ','))
        self.assertEqual(records[0].start, 0)
        self.assertEqual(records[0].end, 28)
        self.assertEqual((records[1].line_start, records[1].line_end), (2, 3))
        self.assertEqual(raw[records[1].start:records[1].end],
                         b'B1,2026-11-12,East,"first\r\nsecond"\r\n')

    def test_bom_is_only_skipped_at_file_start(self):
        path = Path(self.directory.name) / 'bom.csv'
        path.write_bytes(b'\xef\xbb\xbfbookingId,date,region,note\r\n'
                         b'B1,2026-11-12,East,\xef\xbb\xbfmid\r\n')
        selection = resolve_file(path, encodings=['utf-8-sig'], delimiters=[','])
        self.assertEqual(selection.header_start, 3)
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            self.assertEqual(receiver.slice(source.digest, 0, 3, display=False),
                             b'\xef\xbb\xbf')
            run = receiver.run(source.digest)
            evidence = receiver.evidence(run['run_id'])
            self.assertEqual(evidence[0]['reason'], 'mid_file_bom')
            self.assertEqual(evidence[0]['byte_start'], selection.header_end)
            self.assertEqual(run['counts']['accepted'], 0)

    def test_gb18030_record_slices_match_original_bytes(self):
        text = ('bookingId,date,region,note\r\n'
                'B1,2026-11-12,East,"上门\r\nsecond"\r\n'
                'B2,bad-date,West,待复核\r\n')
        path = self.write('gb.csv', text, encoding='gb18030')
        payload = path.read_bytes()
        with self.receiver() as receiver:
            source, selection = receiver.register(path, ['gb18030'], [','])
            self.assertEqual(selection.encoding, 'gb18030')
            run = receiver.run(source.digest)
            accepted = receiver.accepted(run['run_id'])[0]
            self.assertEqual(receiver.slice(source.digest, accepted['byte_start'],
                                            accepted['byte_end'], display=False),
                             payload[accepted['byte_start']:accepted['byte_end']])
            self.assertEqual(accepted['note'], '上门\r\nsecond')
            evidence = receiver.evidence(run['run_id'])[0]
            self.assertIn('待复核', evidence['slice_text'])
            self.assertEqual(receiver.slice(source.digest, evidence['byte_start'],
                                            evidence['byte_end'], display=False),
                             payload[evidence['byte_start']:evidence['byte_end']])

    def test_scanning_is_independent_of_chunk_size(self):
        payload = (b'bookingId,date,region,note\r\n'
                   b'B1,2026-11-12,E,"a\r\nb""c\rd"\r\n'
                   b'B2,2026-11-13,W,x\n'
                   b'B3,2026-11-14,W,y')
        import io
        expected = None
        for chunk in (1, 2, 3, 5, 4096):
            records = [(item.start, item.end, item.line_start, item.line_end, item.raw)
                       for item in scan(io.BytesIO(payload), ',', chunk_size=chunk)]
            if expected is None:
                expected = records
            self.assertEqual(records, expected)

    def test_bad_width_not_truncated(self):
        path = self.write('w.csv', 'bookingId,date,region,note\r\nB1,2026-11-12,East\r\n')
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            evidence = receiver.evidence(run['run_id'])[0]
            self.assertEqual(evidence['reason'], 'width_mismatch')
            self.assertEqual(evidence['candidate'], {'fields': ['B1', '2026-11-12', 'East']})
            self.assertEqual(evidence['slice_text'].count('\r\n'), 1)

    def test_unbalanced_quote_stops_without_resuming_mid_field(self):
        path = self.write('open.csv',
                          'bookingId,date,region,note\r\n'
                          'B1,2026-11-12,East,"open\r\n')
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            self.assertEqual(run['counts']['isolated'], 1)
            evidence = receiver.evidence(run['run_id'])[0]
            self.assertEqual(evidence['reason'], 'unbalanced_quote')
            self.assertEqual(evidence['line_start'], 2)
            self.assertEqual(evidence['line_end'], 3)

    def test_final_record_without_newline_is_a_complete_record(self):
        path = self.write('tail.csv',
                          'bookingId,date,region,note\r\nB1,2026-11-12,East,x')
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            self.assertEqual(run['counts']['accepted'], 1)
            accepted = receiver.accepted(run['run_id'])[0]
            self.assertEqual((accepted['byte_start'], accepted['byte_end']), (28, 48))

    def test_roundtrip_every_isolated_record_byte_for_byte(self):
        text = ('bookingId,date,region,note\r\n'
                'A1,2026-12-01,East,"multi\r\nline ""quoted"""\r\n'
                'A2,not-a-date,West,no date\r\n'
                'bookingId,date,region,note \r\n'
                'A3,2026-12-03,North\r\n'
                'A4,2026-12-04,South,"unterminated\r\n')
        path = self.write('round.csv', text)
        payload = path.read_bytes()
        with self.receiver() as receiver:
            source, selection = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            evidence = receiver.evidence(run['run_id'])
            self.assertEqual({item['reason'] for item in evidence},
                             {'bad_date', 'header_mismatch', 'width_mismatch',
                              'unbalanced_quote'})
            for item in evidence:
                self.assertEqual(
                    receiver.slice(source.digest, item['byte_start'], item['byte_end'],
                                   display=False),
                    payload[item['byte_start']:item['byte_end']])
                self.assertEqual(item['slice_text'],
                                 payload[item['byte_start']:item['byte_end']]
                                 .decode('utf-8'))
                header = payload[selection.header_start:selection.header_end]
                self.assertEqual(header, b'bookingId,date,region,note\r\n')
            accepted = receiver.accepted(run['run_id'])[0]
            self.assertEqual((accepted['line_start'], accepted['line_end']), (2, 3))
            self.assertEqual(accepted['note'], 'multi\r\nline "quoted"')

    # ---- reception -------------------------------------------------------

    def test_demo_reception_isolates_conflict_and_review_recovers(self):
        path = self.write('demo.csv',
                          'bookingId,date,region,note\r\n'
                          'D1,2026-11-12,East,"first line\r\nsecond line"\r\n'
                          'D2,2026-13-40,West,needs review\r\n'
                          'bookingId,date,region,note\r\n'
                          'D3,2026-11-14,North,"quoted ""inner"" text"\r\n'
                          'D1,2026-11-15,East,conflicting duplicate\r\n')
        with self.receiver() as receiver:
            source, selection = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            self.assertEqual(run['counts']['accepted'], 2)
            self.assertEqual(run['counts']['header_skipped'], 1)
            reasons = {item['reason'] for item in receiver.evidence(run['run_id'])}
            self.assertEqual(reasons, {'bad_date', 'conflict'})
            conflict = [item for item in receiver.evidence(run['run_id'])
                        if item['reason'] == 'conflict'][0]
            self.assertEqual(receiver.slice(source.digest, conflict['byte_start'],
                                            conflict['byte_end'], display=False),
                             b'D1,2026-11-15,East,conflicting duplicate\r\n')
            bad = [item for item in receiver.evidence(run['run_id'])
                   if item['reason'] == 'bad_date'][0]
            decision = receiver.submit_decision(
                bad['digest'], bad['mapping_version'], bad['byte_start'], bad['byte_end'],
                'date', '2026-11-13', 'reviewer-1', 'confirmed against ops sheet')
            self.assertEqual(decision['original'],
                             ['D2', '2026-13-40', 'West', 'needs review'])
            rerun = receiver.run(source.digest)
            # D1 and D3 are known duplicates; D2 is newly accepted after review.
            self.assertEqual(rerun['counts']['accepted'], 1)
            self.assertEqual(rerun['counts']['duplicate'], 2)
            accepted = {item['bookingId']: item for item in receiver.accepted(rerun['run_id'])}
            self.assertEqual(accepted['D2']['date'], '2026-11-13')
            self.assertEqual(receiver.evidence(rerun['run_id'])[0]['reason'], 'conflict')

    def test_replacing_the_file_does_not_reuse_decisions(self):
        text = ('bookingId,date,region,note\r\n'
                'B1,bad-date,East,first\r\n')
        first = self.write('first.csv', text)
        second = self.write('second.csv', text.replace('bad-date', '2026-11-13'))
        with self.receiver() as receiver:
            source, _ = receiver.register(first, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            bad = receiver.evidence(run['run_id'])[0]
            receiver.submit_decision(bad['digest'], bad['mapping_version'], bad['byte_start'],
                                     bad['byte_end'], 'date', '2026-11-13', 'r', 'why')
            other, _ = receiver.register(second, ['utf-8-sig'], [','])
            self.assertNotEqual(source.digest, other.digest)
            other_run = receiver.run(other.digest)
            self.assertEqual(other_run['counts']['accepted'], 1)
            self.assertEqual(other_run['counts']['reviewed'], 0)

    def test_decision_for_one_mapping_version_does_not_apply_to_another(self):
        path = self.write('m.csv', 'bookingId,date,region,note\r\nB1,bad-date,East,x\r\n')
        with self.receiver() as receiver:
            source, selection = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            bad = receiver.evidence(run['run_id'])[0]
            receiver.submit_decision(bad['digest'], bad['mapping_version'], bad['byte_start'],
                                     bad['byte_end'], 'date', '2026-11-13', 'r', 'why')
            # an unrelated version key cannot see the decision
            self.assertEqual(receiver.decision_status(bad['digest'], 'v1:other',
                                                      bad['byte_start'], bad['byte_end']),
                             'pending')
            # the same bytes read under another encoding produce a different mapping
            other = resolve_file(path, ['gb18030'], [','])
            self.assertNotEqual(selection.mapping_version, other.mapping_version)
            # a decision bound to the UTF-8 mapping is invisible to the GB18030 one
            self.assertEqual(receiver.decision_status(bad['digest'],
                                                      other.mapping_version,
                                                      bad['byte_start'], bad['byte_end']),
                             'pending')

    def test_identical_resend_is_idempotent_different_content_conflicts(self):
        text = ('bookingId,date,region,note\r\n'
                'B1,2026-11-12,East,x\r\n'
                'B1,2026-11-12,East,x\r\n'
                'B1,2026-11-12,East,y\r\n')
        path = self.write('dup.csv', text)
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            self.assertEqual(run['counts']['accepted'], 1)
            self.assertEqual(run['counts']['duplicate'], 1)
            self.assertEqual(run['counts']['isolated'], 1)
            self.assertEqual(receiver.evidence(run['run_id'])[0]['reason'], 'conflict')
            again = receiver.run(source.digest)
            self.assertEqual(again['counts']['duplicate'], 2)
            self.assertEqual(again['counts']['isolated'], 1)

    def test_width_isolation_keeps_evidence_and_does_not_pad(self):
        path = self.write('w.csv', 'bookingId,date,region,note\r\nB1,2026-11-12,East\r\n')
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            evidence = receiver.evidence(run['run_id'])[0]
            self.assertEqual(evidence['reason'], 'width_mismatch')
            self.assertEqual(evidence['candidate'], {'fields': ['B1', '2026-11-12', 'East']})
            self.assertEqual(evidence['slice_text'].count('\r\n'), 1)

    def test_mid_file_header_only_skipped_when_identical(self):
        text = ('bookingId,date,region,note\r\n'
                'B1,2026-11-12,East,x\r\n'
                'bookingId,date,region,note\r\n'
                'bookingId,date,region\r\n'
                'B2,2026-11-13,West\r\n')
        path = self.write('h.csv', text)
        with self.receiver() as receiver:
            source, _ = receiver.register(path, ['utf-8-sig'], [','])
            run = receiver.run(source.digest)
            self.assertEqual(run['counts']['header_skipped'], 1)
            self.assertEqual(run['counts']['accepted'], 1)
            evidence = receiver.evidence(run['run_id'])
            self.assertEqual([item['reason'] for item in evidence],
                             ['header_mismatch', 'width_mismatch'])
            self.assertIn('bookingId,date,region\r\n', evidence[0]['slice_text'])


class RecoveryTests(unittest.TestCase):
    def test_checkpoint_never_resumes_inside_a_multiline_note(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'm.csv'
            payload = (b'bookingId,date,region,note\r\n'
                       b'B1,2026-11-12,East,"a\r\nb"\r\n'
                       b'B2,2026-11-13,West,c\r\n')
            path.write_bytes(payload)
            second_start = payload.index(b'B2')
            state = Path(directory) / 'state'
            with Receiver(state) as receiver:
                source, _ = receiver.register(path, ['utf-8-sig'], [','])
                first = receiver.run(source.digest, stop_after=1)
                self.assertEqual(first['status'], 'interrupted')
                checkpoint = receiver.database.execute(
                    'SELECT next_start, next_line, next_ordinal FROM checkpoints '
                    'WHERE run_id = ?', (first['run_id'],)).fetchone()
                self.assertEqual(checkpoint, (second_start, 4, 3))
                self.assertEqual(receiver.slice(source.digest, 0, second_start,
                                                display=False), payload[:second_start])
                second = receiver.run(source.digest, resume=True)
                self.assertEqual(second['resumed_from'], first['run_id'])
                self.assertEqual(second['counts']['accepted'], 1)
                self.assertEqual([item['bookingId'] for item in
                                  receiver.accepted(second['run_id'])], ['B2'])

    def test_killed_process_resumes_after_last_complete_record(self):
        """Start a real child, SIGKILL it mid-note, then resume from disk."""
        repository = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'big.csv'
            lines = ['bookingId,date,region,note\r\n']
            for index in range(12):
                lines.append('B%d,2026-11-%02d,R,"line one\r\nline two"\r\n'
                             % (index, index + 1))
            path.write_bytes(''.join(lines).encode('utf-8'))
            # Kill inside the fourth logical record, i.e. not on a boundary.
            kill_at = len(''.join(lines[:4]).encode('utf-8')) + 10
            environment = dict(os.environ, PYTHONPATH=str(repository))
            state = Path(directory) / 'state'
            killed = subprocess.run(
                [sys.executable, str(repository / 'tests' / '_crash_child.py'),
                 str(state), str(path), '64', str(kill_at)],
                capture_output=True, text=True, env=environment, timeout=60)
            self.assertEqual(killed.returncode, -signal.SIGKILL, killed.stderr)
            with Receiver(state) as receiver:
                digest = receiver.database.execute('SELECT digest FROM sources').fetchone()[0]
                checkpoint = receiver.database.execute(
                    'SELECT next_start, next_ordinal FROM checkpoints').fetchone()
                self.assertLessEqual(checkpoint[0], kill_at)
                source = receiver.source(digest)
                payload = path.read_bytes()
                self.assertEqual(payload[checkpoint[0] - 1:checkpoint[0] + 1], b'\nB')
                resumed = receiver.run(digest, resume=True)
                self.assertEqual(resumed['status'], 'complete')
                # Every booking is accepted exactly once across both runs.
                total = receiver.database.execute(
                    'SELECT COUNT(*) FROM accepted').fetchone()[0]
                distinct = receiver.database.execute(
                    'SELECT COUNT(*) FROM bookings').fetchone()[0]
                self.assertEqual(total, 12)
                self.assertEqual(distinct, 12)


class CommandLineTests(unittest.TestCase):
    def test_demo_script_finishes_quickly_and_shows_closed_loop(self):
        result = subprocess.run([sys.executable, '-m', 'scripts.demo'],
                                capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('reviewed', result.stdout)
        self.assertIn('slice', result.stdout)


if __name__ == '__main__':
    unittest.main()
