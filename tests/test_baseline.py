import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from branchcsv.parser import inspect
from branchcsv.journal import Journal
from branchcsv.transport import capture
from scripts.materialize import main as materialize
from scripts.source_server import running


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_capture_journal_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = Journal(directory)
            try:
                first = journal.save(b'synthetic', 'source-1')
                second = journal.save(b'synthetic', 'source-1')
                self.assertEqual(first, second)
                self.assertEqual(Path(first['path']).read_bytes(), b'synthetic')
                self.assertEqual(journal.database.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                journal.close()

    def test_remote_transport_is_rejected_before_io(self):
        with self.assertRaises(ValueError):
            capture('https://example.invalid/private')

    def test_cli_help(self):
        result = subprocess.run([sys.executable, '-m', 'branchcsv', '--help'], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b'preview', result.stdout)

    def test_multiline_and_crlf_preserved(self):
        result = inspect(Path('fixtures/bookings.csv').read_bytes())
        self.assertEqual(result['rows'][0][3], 'first\r\nsecond')
        self.assertEqual(len(result['rows']), 2)

    def test_explicit_gb18030(self):
        result = inspect(Path('fixtures/bookings-gb.csv').read_bytes(), 'gb18030')
        self.assertIn('上门', result['rows'][0][3])

    def test_bad_width_not_truncated(self):
        with self.assertRaises(ValueError):
            inspect(Path('fixtures/width.csv').read_bytes())


if __name__ == '__main__':
    unittest.main()
