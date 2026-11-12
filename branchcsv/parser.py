import csv
import io


def inspect(payload, encoding='utf-8-sig', delimiter=','):
    stream = io.StringIO(payload.decode(encoding), newline='')
    reader = csv.reader(stream, delimiter=delimiter, strict=True)
    header = next(reader, None)
    if header is None:
        raise ValueError('Empty CSV document')
    rows = []
    for record in reader:
        if len(record) != len(header):
            raise ValueError('Record width differs from header')
        rows.append(record)
    return {'header': header, 'rows': rows}
