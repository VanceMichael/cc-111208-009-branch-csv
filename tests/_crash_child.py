"""Child process used by the crash-recovery test.

Runs a real reception against the stored blob of the source and SIGKILLs itself
when the scanner has read past a chosen byte offset, i.e. in the middle of a
quoted multi-line record rather than at a record boundary.  The parent then
verifies that the checkpoint is still the last complete record and that the
resumed run accepts every remaining record exactly once.

Not a test module by name and never imported by the suite.
"""

import builtins
import os
import signal
import sys

from branchcsv.receive import Receiver


STATE = {'target': None, 'kill_at': None, 'chunk': None}


class KillingStream:
    """File proxy that kills the process once the *scanner* read reaches
    ``kill_at``.  Reads made while resolving the dialect use other sizes and
    are never killed, so the crash always lands inside record scanning."""

    def __init__(self, stream, chunk):
        self._stream = stream
        self._chunk = chunk
        self._offset = stream.tell()

    def read(self, size=-1):
        if (self._chunk is not None and size == self._chunk
                and STATE['kill_at'] is not None and self._offset >= STATE['kill_at']):
            os.kill(os.getpid(), signal.SIGKILL)
        data = self._stream.read(size)
        self._offset += len(data)
        return data

    def seek(self, *args):
        position = self._stream.seek(*args)
        self._offset = position
        return position

    def tell(self):
        return self._stream.tell()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._stream.close()


def main():
    state, source_path, chunk_size, kill_at = sys.argv[1:5]
    chunk_size = int(chunk_size)
    kill_at = int(kill_at)
    real_open = builtins.open

    def patched(path, *args, **kwargs):
        stream = real_open(path, *args, **kwargs)
        if STATE['target'] is not None and os.path.abspath(str(path)) == STATE['target']:
            return KillingStream(stream, STATE['chunk'])
        return stream

    builtins.open = patched
    receiver = Receiver(state, chunk_size=chunk_size)
    source, _ = receiver.register(source_path, ['utf-8-sig'], [','])
    STATE['target'] = os.path.abspath(str(source.path))
    STATE['kill_at'] = kill_at
    STATE['chunk'] = chunk_size
    receiver.run(source.digest)
    raise SystemExit('child was supposed to be killed')


if __name__ == '__main__':
    main()
