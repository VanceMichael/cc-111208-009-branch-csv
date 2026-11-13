import argparse
import json
from pathlib import Path
import sys

from .parser import inspect
from .journal import Journal
from .transport import capture
from .receive import Receiver


def _encoding_list(values):
    return values or None


def _receiver(args):
    return Receiver(args.state)


def _cmd_capture(args):
    headers = dict(value.split(':', 1) for value in args.header)
    headers = {key.strip(): value.strip() for key, value in headers.items()}
    status, response_headers, payload = capture(
        args.url, args.method, Path(args.body).read_bytes() if args.body else None, headers)
    journal = Journal(args.state)
    try:
        result = journal.save(payload, args.url)
    finally:
        journal.close()
    result.update({'http_status': status, 'headers': response_headers})
    return result


def _cmd_receive(args):
    with _receiver(args) as receiver:
        source, selection = receiver.register(
            args.source, _encoding_list(args.encoding), args.delimiter or None)
        run = receiver.run(source.digest, resume=args.resume, stop_after=args.stop_after,
                           mapping_version=selection.mapping_version)
        run.update({
            'source': str(args.source),
            'size': source.size,
            'encoding': selection.encoding,
            'delimiter': selection.delimiter,
            'header': list(selection.header_fields),
            'header_byte_range': [selection.header_start, selection.header_end],
        })
        return run


def _cmd_evidence(args):
    with _receiver(args) as receiver:
        run = receiver.database.execute(
            'SELECT digest, mapping_version, status FROM runs WHERE run_id = ?',
            (args.run,)).fetchone()
        if run is None:
            raise ValueError('unknown run: %s' % args.run)
        return {'run_id': args.run, 'digest': run[0], 'mapping_version': run[1],
                'status': run[2], 'records': receiver.evidence(args.run)}


def _cmd_slice(args):
    with _receiver(args) as receiver:
        if args.raw:
            raw = receiver.slice(args.digest, args.start, args.end, display=False)
            return {'digest': args.digest, 'byte_start': args.start, 'byte_end': args.end,
                    'hex': raw.hex(), 'size': len(raw)}
        return receiver.slice(args.digest, args.start, args.end, display=True)


def _cmd_review(args):
    with _receiver(args) as receiver:
        run = receiver.database.execute(
            'SELECT digest, mapping_version FROM runs WHERE run_id = ?', (args.run,)).fetchone()
        if run is None:
            raise ValueError('unknown run: %s' % args.run)
        record = receiver.database.execute(
            'SELECT digest, mapping_version, start, end FROM isolated '
            'WHERE run_id = ? AND ordinal = ?', (args.run, args.ordinal)).fetchone()
        if record is None:
            raise ValueError('run %s has no isolated record %s' % (args.run, args.ordinal))
        return receiver.submit_decision(
            record[0], record[1], record[2], record[3], args.field, args.value,
            args.reviewer, args.reason)


def build_parser():
    parser = argparse.ArgumentParser(description='地区预约 CSV 的显式编码与方言工具。')
    sub = parser.add_subparsers(dest='command', required=True)

    view = sub.add_parser('preview', help='查看明确编码/方言的文件，不写业务状态')
    view.add_argument('source')
    view.add_argument('--encoding', default='utf-8-sig')
    view.add_argument('--delimiter', default=',')

    raw = sub.add_parser('capture', help='捕获单份 loopback HTTP 原始响应')
    raw.add_argument('url')
    raw.add_argument('--state', default='var')
    raw.add_argument('--method', default='GET')
    raw.add_argument('--body')
    raw.add_argument('--header', action='append', default=[])

    receive = sub.add_parser('receive', help='正式接收一份预约导出；同一来源可重复接收')
    receive.add_argument('source')
    receive.add_argument('--state', required=True)
    receive.add_argument('--encoding', action='append',
                         help='候选编码，可重复；省略则依次尝试 utf-8-sig 与 gb18030')
    receive.add_argument('--delimiter', action='append',
                         help='候选分隔符，可重复；省略则尝试 , ; TAB')
    receive.add_argument('--resume', action='store_true',
                         help='从同一来源/映射的完整记录检查点继续')
    receive.add_argument('--stop-after', type=int, default=None,
                         help='处理 N 条逻辑记录后停止，便于验证恢复')

    evidence = sub.add_parser('evidence', help='查看一次接收中被隔离的逻辑记录')
    evidence.add_argument('--state', required=True)
    evidence.add_argument('--run', type=int, required=True)

    part = sub.add_parser('slice', help='按原始半开字节区间取回源文件片段')
    part.add_argument('--state', required=True)
    part.add_argument('--digest', required=True)
    part.add_argument('--start', type=int, required=True)
    part.add_argument('--end', type=int, required=True)
    part.add_argument('--raw', action='store_true', help='以十六进制输出精确原始字节')

    review = sub.add_parser('review', help='对隔离记录提交不可变字段修正')
    review.add_argument('--state', required=True)
    review.add_argument('--run', type=int, required=True)
    review.add_argument('--ordinal', type=int, required=True)
    review.add_argument('--field', required=True)
    review.add_argument('--value', required=True)
    review.add_argument('--reviewer', required=True)
    review.add_argument('--reason', required=True)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == 'capture':
            result = _cmd_capture(args)
        elif args.command == 'receive':
            result = _cmd_receive(args)
        elif args.command == 'evidence':
            result = _cmd_evidence(args)
        elif args.command == 'slice':
            result = _cmd_slice(args)
        elif args.command == 'review':
            result = _cmd_review(args)
        else:
            result = inspect(Path(args.source).read_bytes(), args.encoding, args.delimiter)
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
