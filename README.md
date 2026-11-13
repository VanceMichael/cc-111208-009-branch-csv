# branchcsv

地区预约 CSV 的显式编码/方言工具，以及带隔离与复核的正式接收流程。

## 本地运行

使用 Python 3.11 标准库，无 pip 依赖。所有来源与业务数据都是本地合成数据，不连接真实用户系统。

```sh
python -m scripts.materialize
python -m branchcsv preview fixtures/bookings.csv
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

- `preview` 只按明确编码/方言查看结构，不写业务状态。
- `capture` 保存单份 loopback HTTP 原始响应，按 SHA256 登记到 captures 表。
- `receive` 是正式入口：复制并绑定文件摘要，按候选解析编码/方言，逐逻辑记录接收、隔离或去重，并在同一事务中写入业务行、隔离证据和恢复检查点。
- `evidence` 列出一次接收中被隔离的记录（字节区间、行范围、原因、原文切片、复核状态）。
- `slice` 按 `[start, end)` 取回原件字节，可核对隔离证据。
- `review` 对隔离记录提交不可变字段修正。

详细规则见 `docs/acceptance.md`（业务与复核）与 `docs/source-contract.md`（编码/方言/字节定位）。

## 典型闭环

```sh
python -m scripts.materialize
python -m branchcsv receive fixtures/demo-bookings.csv --state /tmp/var \
  --encoding utf-8-sig --delimiter ,
# 用输出里的 run_id 查看隔离证据
python -m branchcsv evidence --state /tmp/var --run 1
# 按证据中的 byte_start/byte_end 取回原件
python -m branchcsv slice --state /tmp/var --digest <sha256> \
  --start 74 --end 107 --raw
# 复核坏日期
python -m branchcsv review --state /tmp/var --run 1 --ordinal 3 \
  --field date --value 2026-11-13 --reviewer ops --reason verified
# 同一文件重跑：已核准决定复用，原始字节不变
python -m branchcsv receive fixtures/demo-bookings.csv --state /tmp/var \
  --encoding utf-8-sig --delimiter ,
```

`scripts.demo` 自动完成上述闭环，并在另一个临时状态目录演示中断后续跑，整段通常 1 秒内完成。

## 可复现容器

```sh
docker build -t branchcsv:local .
docker run --rm --network none branchcsv:local python -m unittest discover -s tests -v
docker run --rm --network none branchcsv:local
```

镜像固定基础摘要。构建在正式运行前完成；运行和测试不安装包、不下载数据。

## 协议与来源

- docs/source-contract.md 给出支持子集、字段及失败语义。
- docs/acceptance.md 给出业务验收边界与隔离原因。
- fixtures/source.json 是本地可重放 HTTP 路由；`python -m scripts.source_server --port 8765` 启动，`/__requests` 可检查实际请求。
- scripts/materialize.py 将版本化 base64 二进制夹具还原为原始字节，重复执行安全；二进制文件不是用户附件。
- journal.py 只记录原始响应，不代替接收流程自己的事务和恢复设计。
