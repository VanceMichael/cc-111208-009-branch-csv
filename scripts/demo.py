import json
from pathlib import Path
import subprocess
import sys
import tempfile

from .materialize import main as materialize


def show(title, value):
    print('== ' + title)
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main():
    materialize()
    with tempfile.TemporaryDirectory() as directory:
        state = Path(directory) / 'state'
        source = 'fixtures/demo-bookings.csv'

        print('# preview keeps the explicit-dialect inspection path')
        subprocess.run([sys.executable, '-m', 'branchcsv', 'preview', source], check=True)

        print('# formal reception of the same file')
        received = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'receive', source, '--state', str(state),
             '--encoding', 'utf-8-sig', '--delimiter', ','],
            check=True, capture_output=True, text=True)
        run = json.loads(received.stdout)
        show('receive', run)

        evidence = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'evidence', '--state', str(state),
             '--run', str(run['run_id'])],
            check=True, capture_output=True, text=True)
        records = json.loads(evidence.stdout)
        show('isolation evidence', records['records'])

        pending = [item for item in records['records'] if item['reason'] == 'bad_date'][0]
        original = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'slice', '--state', str(state),
             '--digest', pending['digest'], '--start', str(pending['byte_start']),
             '--end', str(pending['byte_end']), '--raw'],
            check=True, capture_output=True, text=True)
        show('original bytes of the record to review', {
            'byte_range': [pending['byte_start'], pending['byte_end']],
            'line_range': [pending['line_start'], pending['line_end']],
            'slice': json.loads(original.stdout)})

        reviewed = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'review', '--state', str(state),
             '--run', str(run['run_id']), '--ordinal', str(pending['ordinal']),
             '--field', 'date', '--value', '2026-11-13', '--reviewer', 'demo-reviewer',
             '--reason', 'confirmed against the operations sheet'],
            check=True, capture_output=True, text=True)
        show('review decision (original bytes untouched)', json.loads(reviewed.stdout))

        replayed = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'receive', source, '--state', str(state),
             '--encoding', 'utf-8-sig', '--delimiter', ','],
            check=True, capture_output=True, text=True)
        rerun = json.loads(replayed.stdout)
        show('same file received again: reviewed result is reused', rerun)

        followed = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'slice', '--state', str(state),
             '--digest', pending['digest'], '--start', str(pending['byte_start']),
             '--end', str(pending['byte_end'])],
            check=True, capture_output=True, text=True)
        show('original slice still unchanged after review', json.loads(followed.stdout))

        print('# resume after an interruption at a complete record boundary')
        state2 = Path(directory) / 'state-resume'
        partial = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'receive', source, '--state', str(state2),
             '--encoding', 'utf-8-sig', '--delimiter', ',', '--stop-after', '1'],
            check=True, capture_output=True, text=True)
        show('interrupted', json.loads(partial.stdout))
        resumed = subprocess.run(
            [sys.executable, '-m', 'branchcsv', 'receive', source, '--state', str(state2),
             '--encoding', 'utf-8-sig', '--delimiter', ',', '--resume'],
            check=True, capture_output=True, text=True)
        show('resumed', json.loads(resumed.stdout))


if __name__ == '__main__':
    main()
