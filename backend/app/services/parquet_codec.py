"""Minimal pure-Python Parquet writer/reader (flat, nullable, uncompressed).

The project deliberately keeps the backend dependency-free, but the distribution
pipeline has to be able to *actually* build files whose declared format is
``parquet`` and then read them back with a real Parquet reader.  This module
implements the subset of the format that the pipeline produces:

* one row group, one data page per column;
* PLAIN encoding for BOOLEAN / INT64 / DOUBLE / BYTE_ARRAY(UTF8);
* OPTIONAL columns (definition levels, RLE/bit-packed hybrid, bit width 1);
* UNCOMPRESSED codec;
* Thrift Compact Protocol for page headers and file metadata.

Files written here are readable by Apache Arrow/pyarrow and by :func:`read_parquet`.
Anything outside that subset raises ``ParquetError`` instead of silently
producing a file that its declared format cannot read.
"""
from __future__ import annotations

import struct
from typing import Any

PARQUET_MAGIC = b"PAR1"

# Physical types.
BOOLEAN = 0
INT32 = 1
INT64 = 2
FLOAT = 4
DOUBLE = 5
BYTE_ARRAY = 6

# Repetition.
REQUIRED = 0
OPTIONAL = 1
REPEATED = 2

# Encodings.
PLAIN = 0
RLE = 3

# Compression.
UNCOMPRESSED = 0

# Page types.
DATA_PAGE = 0

# Converted types.
UTF8 = 0

# Thrift compact protocol types.
CT_STOP = 0
CT_TRUE = 1
CT_FALSE = 2
CT_BYTE = 3
CT_I16 = 4
CT_I32 = 5
CT_I64 = 6
CT_DOUBLE = 7
CT_BINARY = 8
CT_LIST = 9
CT_SET = 10
CT_MAP = 11
CT_STRUCT = 12


class ParquetError(ValueError):
    """Raised when a Parquet file cannot be encoded or decoded by this codec."""


# ---------------------------------------------------------------------------
# Thrift compact protocol
# ---------------------------------------------------------------------------
def _zigzag(value: int, bits: int = 64) -> int:
    return (value << 1) ^ (value >> (bits - 1))


def _unzigzag(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


class _CompactWriter:
    def __init__(self) -> None:
        self.buf = bytearray()
        self._last: list[int] = []

    # -- primitives ---------------------------------------------------------
    def varint(self, value: int) -> None:
        if value < 0:
            raise ParquetError("varint 不接受负数")
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                self.buf.append(byte | 0x80)
            else:
                self.buf.append(byte)
                return

    def _begin_field(self, field_id: int, ctype: int) -> None:
        last = self._last[-1]
        delta = field_id - last
        if 0 < delta <= 15:
            self.buf.append((delta << 4) | ctype)
        else:
            self.buf.append(ctype)
            self.varint(_zigzag(field_id, 16))
        self._last[-1] = field_id

    def begin_struct(self) -> None:
        self._last.append(0)

    def end_struct(self) -> None:
        self.buf.append(CT_STOP)
        self._last.pop()

    def field_i32(self, field_id: int, value: int) -> None:
        self._begin_field(field_id, CT_I32)
        self.varint(_zigzag(value, 32))

    def field_i64(self, field_id: int, value: int) -> None:
        self._begin_field(field_id, CT_I64)
        self.varint(_zigzag(value, 64))

    def field_string(self, field_id: int, value: str) -> None:
        self._begin_field(field_id, CT_BINARY)
        data = value.encode("utf-8")
        self.varint(len(data))
        self.buf += data

    def field_struct(self, field_id: int) -> None:
        self._begin_field(field_id, CT_STRUCT)
        self.begin_struct()

    def end_field_struct(self) -> None:
        self.end_struct()

    def _list_header(self, count: int, ctype: int) -> None:
        if count < 15:
            self.buf.append((count << 4) | ctype)
        else:
            self.buf.append(0xF0 | ctype)
            self.varint(count)

    def field_list_i32(self, field_id: int, values: list[int]) -> None:
        self._begin_field(field_id, CT_LIST)
        self._list_header(len(values), CT_I32)
        for value in values:
            self.varint(_zigzag(value, 32))

    def field_list_string(self, field_id: int, values: list[str]) -> None:
        self._begin_field(field_id, CT_LIST)
        self._list_header(len(values), CT_BINARY)
        for value in values:
            data = value.encode("utf-8")
            self.varint(len(data))
            self.buf += data

    def field_list_struct(self, field_id: int, count: int) -> None:
        self._begin_field(field_id, CT_LIST)
        self._list_header(count, CT_STRUCT)

    def list_struct_item(self) -> None:
        self.begin_struct()

    def end_list_struct_item(self) -> None:
        self.end_struct()


class _CompactReader:
    def __init__(self, data: bytes, offset: int = 0) -> None:
        self.data = data
        self.pos = offset
        self._last: list[int] = []

    def _varint(self) -> int:
        shift = 0
        result = 0
        while True:
            if self.pos >= len(self.data):
                raise ParquetError("Thrift 数据提前结束")
            byte = self.data[self.pos]
            self.pos += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    def _byte(self) -> int:
        if self.pos >= len(self.data):
            raise ParquetError("Thrift 数据提前结束")
        value = self.data[self.pos]
        self.pos += 1
        return value

    def begin_struct(self) -> None:
        self._last.append(0)

    def end_struct(self) -> None:
        self._last.pop()

    def _field_header(self) -> tuple[int, int]:
        byte = self._byte()
        if byte == CT_STOP:
            return 0, CT_STOP
        ctype = byte & 0x0F
        delta = (byte >> 4) & 0x0F
        if delta:
            field_id = self._last[-1] + delta
        else:
            field_id = _unzigzag(self._varint())
        self._last[-1] = field_id
        return field_id, ctype

    def read_struct(self) -> dict[int, Any]:
        """Read one struct into ``{field_id: value}`` (unknown fields preserved)."""
        self.begin_struct()
        fields: dict[int, Any] = {}
        while True:
            field_id, ctype = self._field_header()
            if ctype == CT_STOP:
                break
            fields[field_id] = self._read_value(ctype)
        self.end_struct()
        return fields

    def _read_value(self, ctype: int) -> Any:
        if ctype == CT_TRUE:
            return True
        if ctype == CT_FALSE:
            return False
        if ctype == CT_BYTE:
            value = self._byte()
            return value - 256 if value > 127 else value
        if ctype in (CT_I16, CT_I32, CT_I64):
            return _unzigzag(self._varint())
        if ctype == CT_DOUBLE:
            if self.pos + 8 > len(self.data):
                raise ParquetError("Thrift double 数据提前结束")
            value = struct.unpack_from("<d", self.data, self.pos)[0]
            self.pos += 8
            return value
        if ctype == CT_BINARY:
            length = self._varint()
            end = self.pos + length
            if end > len(self.data):
                raise ParquetError("Thrift binary 数据提前结束")
            value = self.data[self.pos:end]
            self.pos = end
            return value
        if ctype in (CT_LIST, CT_SET):
            header = self._byte()
            size = (header >> 4) & 0x0F
            elem_type = header & 0x0F
            if size == 15:
                size = self._varint()
            return [self._read_value(elem_type) for _ in range(size)]
        if ctype == CT_MAP:
            size = self._varint()
            if not size:
                return {}
            header = self._byte()
            key_type = (header >> 4) & 0x0F
            value_type = header & 0x0F
            return {self._read_value(key_type): self._read_value(value_type)
                    for _ in range(size)}
        if ctype == CT_STRUCT:
            return self.read_struct()
        raise ParquetError(f"未知的 Thrift 字段类型 {ctype}")


# ---------------------------------------------------------------------------
# Column typing
# ---------------------------------------------------------------------------
def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    import json

    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def infer_column_type(values: list[Any]) -> str:
    """Infer one of ``boolean``/``integer``/``number``/``string`` from values."""
    present = [value for value in values if value is not None]
    if not present:
        return "string"
    if all(isinstance(value, bool) for value in present):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in present):
        return "integer"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool)
           for value in present):
        return "number"
    return "string"


def _physical_type(type_name: str) -> int:
    return {
        "boolean": BOOLEAN,
        "integer": INT64,
        "number": DOUBLE,
        "string": BYTE_ARRAY,
    }[type_name]


# ---------------------------------------------------------------------------
# Page encoding
# ---------------------------------------------------------------------------
def _bitpacked_levels(levels: list[int]) -> bytes:
    """RLE/bit-packed hybrid for a single-bit definition level sequence."""
    groups = (len(levels) + 7) // 8
    payload = bytearray()
    # varint header: bit-packed run, number of groups.
    header = (groups << 1) | 1
    while True:
        byte = header & 0x7F
        header >>= 7
        if header:
            payload.append(byte | 0x80)
        else:
            payload.append(byte)
            break
    for group in range(groups):
        bits = 0
        for index in range(8):
            position = group * 8 + index
            if position < len(levels) and levels[position]:
                bits |= 1 << index
        payload.append(bits)
    return struct.pack("<I", len(payload)) + bytes(payload)


def _plain_values(values: list[Any], type_name: str) -> bytes:
    out = bytearray()
    if type_name == "boolean":
        return _plain_booleans(values)
    if type_name == "integer":
        for value in values:
            out += struct.pack("<q", int(value))
        return bytes(out)
    if type_name == "number":
        for value in values:
            out += struct.pack("<d", float(value))
        return bytes(out)
    if type_name == "string":
        for value in values:
            data = _stringify(value).encode("utf-8")
            out += struct.pack("<I", len(data))
            out += data
        return bytes(out)
    raise ParquetError(f"不支持的列类型 {type_name}")


def _plain_booleans(values: list[Any]) -> bytes:
    out = bytearray((len(values) + 7) // 8)
    for index, value in enumerate(values):
        if value:
            out[index // 8] |= 1 << (index % 8)
    return bytes(out)


def _page_payload(values: list[Any], type_name: str) -> bytes:
    levels = [1 if value is not None else 0 for value in values]
    present = [value for value in values if value is not None]
    return _bitpacked_levels(levels) + _plain_values(present, type_name)


def _data_page_header(num_values: int, payload_size: int) -> bytes:
    writer = _CompactWriter()
    writer.begin_struct()
    writer.field_i32(1, DATA_PAGE)
    writer.field_i32(2, payload_size)
    writer.field_i32(3, payload_size)
    writer.field_struct(5)
    writer.field_i32(1, num_values)
    writer.field_i32(2, PLAIN)          # encoding
    writer.field_i32(3, RLE)            # definition_level_encoding
    writer.field_i32(4, RLE)            # repetition_level_encoding
    writer.end_field_struct()
    writer.end_struct()
    return bytes(writer.buf)


# ---------------------------------------------------------------------------
# File metadata
# ---------------------------------------------------------------------------
def _schema_element(writer: _CompactWriter, *, name: str, physical_type: int | None,
                    num_children: int | None = None,
                    converted_type: int | None = None,
                    repetition: int | None = OPTIONAL) -> None:
    writer.list_struct_item()
    if physical_type is not None:
        writer.field_i32(1, physical_type)
    if repetition is not None:
        writer.field_i32(3, repetition)
    writer.field_string(4, name)
    if num_children is not None:
        writer.field_i32(5, num_children)
    if converted_type is not None:
        writer.field_i32(6, converted_type)
    writer.end_list_struct_item()


def _write_file_metadata(*, columns: list[tuple[str, str, int]],
                         chunks: list[tuple[int, int]],
                         num_rows: int) -> bytes:
    """``chunks`` is a list of (data_page_offset, total_size) per column."""
    writer = _CompactWriter()
    writer.begin_struct()
    writer.field_i32(1, 1)  # version
    writer.field_list_struct(2, len(columns) + 1)
    _schema_element(writer, name="schema", physical_type=None,
                    num_children=len(columns), repetition=REQUIRED)
    for name, type_name, _ in columns:
        converted = UTF8 if type_name == "string" else None
        _schema_element(writer, name=name, physical_type=_physical_type(type_name),
                        converted_type=converted, repetition=OPTIONAL)
    writer.field_i64(3, num_rows)
    writer.field_list_struct(4, 1)  # one row group
    writer.list_struct_item()       # RowGroup
    writer.field_list_struct(1, len(columns))
    for index, (name, type_name, _) in enumerate(columns):
        offset, total_size = chunks[index]
        writer.list_struct_item()   # ColumnChunk
        writer.field_i64(2, offset)  # file_offset
        writer.field_struct(3)       # meta_data
        writer.field_i32(1, _physical_type(type_name))
        writer.field_list_i32(2, [PLAIN, RLE])
        writer.field_list_string(3, [name])
        writer.field_i32(4, UNCOMPRESSED)
        writer.field_i64(5, num_rows)
        writer.field_i64(6, total_size)
        writer.field_i64(7, total_size)
        writer.field_i64(9, offset)
        writer.end_field_struct()
        writer.end_list_struct_item()
    writer.field_i64(2, sum(size for _, size in chunks))  # total_byte_size
    writer.field_i64(3, num_rows)                          # num_rows
    writer.end_list_struct_item()
    writer.field_string(6, "semantic-extraction-parquet/1.0")
    writer.end_struct()
    return bytes(writer.buf)


# ---------------------------------------------------------------------------
# Public writer
# ---------------------------------------------------------------------------
def write_parquet(records: list[dict[str, Any]], columns: list[str] | None = None,
                  types: dict[str, str] | None = None) -> bytes:
    """Encode flat records as a Parquet file.

    ``types`` may pin column types (``boolean``/``integer``/``number``/``string``);
    otherwise types are inferred.  ``None`` values become Parquet nulls.
    """
    if columns is None:
        columns = []
        for record in records:
            for key in record:
                if key not in columns:
                    columns.append(key)
    if not records and not columns:
        columns = ["id"]

    column_types = dict(types or {})
    for name in columns:
        if name not in column_types:
            column_types[name] = infer_column_type([record.get(name) for record in records])

    # Normalise values to the declared physical type so that read-back is exact.
    normalised: dict[str, list[Any]] = {}
    for name in columns:
        type_name = column_types[name]
        values: list[Any] = []
        for record in records:
            value = record.get(name)
            if value is None:
                values.append(None)
            elif type_name == "integer":
                try:
                    values.append(int(value))
                except (TypeError, ValueError) as exc:
                    raise ParquetError(f"列 {name} 的值 {value!r} 不能按整数写入") from exc
            elif type_name == "number":
                try:
                    values.append(float(value))
                except (TypeError, ValueError) as exc:
                    raise ParquetError(f"列 {name} 的值 {value!r} 不能按数值写入") from exc
            elif type_name == "boolean":
                values.append(bool(value))
            else:
                values.append(_stringify(value))
        normalised[name] = values

    body = bytearray()
    body += PARQUET_MAGIC
    chunks: list[tuple[int, int]] = []
    for name in columns:
        payload = _page_payload(normalised[name], column_types[name])
        header = _data_page_header(len(records), len(payload))
        offset = len(body)
        body += header
        body += payload
        chunks.append((offset, len(header) + len(payload)))

    metadata = _write_file_metadata(
        columns=[(name, column_types[name], 0) for name in columns],
        chunks=chunks,
        num_rows=len(records),
    )
    body += metadata
    body += struct.pack("<I", len(metadata))
    body += PARQUET_MAGIC
    return bytes(body)


# ---------------------------------------------------------------------------
# Public reader
# ---------------------------------------------------------------------------
def _schema_from_metadata(fields: dict[int, Any]) -> tuple[list[dict[str, Any]], int]:
    schema: list[dict[str, Any]] = []
    raw = fields.get(2) or []
    for item in raw:
        if not isinstance(item, dict):
            continue
        schema.append({
            "name": (item.get(4) or b"").decode("utf-8") if isinstance(item.get(4), bytes)
            else str(item.get(4) or ""),
            "type": item.get(1),
            "repetition": item.get(3),
            "num_children": item.get(5),
            "converted_type": item.get(6),
        })
    num_rows = int(fields.get(3) or 0)
    return schema, num_rows


def _read_levels(data: bytes, pos: int, num_values: int, bit_width: int) -> tuple[list[int], int]:
    if pos + 4 > len(data):
        raise ParquetError("定义层级长度缺失")
    length = struct.unpack_from("<I", data, pos)[0]
    pos += 4
    end = pos + length
    if end > len(data):
        raise ParquetError("定义层级数据越界")
    payload = data[pos:end]
    levels: list[int] = []
    index = 0
    while index < len(payload) and len(levels) < num_values:
        header = 0
        shift = 0
        while True:
            byte = payload[index]
            index += 1
            header |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        if header & 1:  # bit-packed run
            groups = header >> 1
            count = groups * 8
            for group in range(groups):
                if index >= len(payload):
                    raise ParquetError("bit-packed 定义层级数据不足")
                bits = payload[index]
                index += 1
                for bit in range(8):
                    if len(levels) < num_values:
                        levels.append((bits >> bit) & 1)
            if count == 0:
                break
        else:  # RLE run
            run_length = header >> 1
            byte_count = max(1, (bit_width + 7) // 8)
            value = int.from_bytes(payload[index:index + byte_count], "little")
            index += byte_count
            levels.extend([value] * run_length)
    if len(levels) < num_values:
        levels.extend([0] * (num_values - len(levels)))
    return levels[:num_values], end


def _read_plain(data: bytes, pos: int, count: int, physical: int) -> tuple[list[Any], int]:
    values: list[Any] = []
    if physical == BOOLEAN:
        for index in range(count):
            byte = data[pos + index // 8]
            values.append(bool((byte >> (index % 8)) & 1))
        return values, pos + (count + 7) // 8
    if physical == INT32:
        for _ in range(count):
            values.append(struct.unpack_from("<i", data, pos)[0])
            pos += 4
        return values, pos
    if physical == INT64:
        for _ in range(count):
            values.append(struct.unpack_from("<q", data, pos)[0])
            pos += 8
        return values, pos
    if physical == FLOAT:
        for _ in range(count):
            values.append(struct.unpack_from("<f", data, pos)[0])
            pos += 4
        return values, pos
    if physical == DOUBLE:
        for _ in range(count):
            values.append(struct.unpack_from("<d", data, pos)[0])
            pos += 8
        return values, pos
    if physical == BYTE_ARRAY:
        for _ in range(count):
            length = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            values.append(data[pos:pos + length])
            pos += length
        return values, pos
    raise ParquetError(f"不支持的物理类型 {physical}")


def _decode_column_chunk(data: bytes, meta: dict[int, Any],
                         physical: int, converted: int | None) -> list[Any]:
    codec = int(meta.get(4) or 0)
    if codec != UNCOMPRESSED:
        raise ParquetError(f"读取器只支持未压缩 Parquet（codec={codec}）")
    num_values = int(meta.get(5) or 0)
    data_page_offset = int(meta.get(9) or 0)
    total_size = int(meta.get(7) or 0)
    pos = data_page_offset
    end = data_page_offset + total_size
    values: list[Any] = []
    while pos < end and len(values) < num_values:
        reader = _CompactReader(data, pos)
        page_header = reader.read_struct()
        pos = reader.pos
        page_type = int(page_header.get(1) or 0)
        compressed_size = int(page_header.get(3) or 0)
        if page_type != DATA_PAGE:
            pos += compressed_size
            continue
        data_header = page_header.get(5) or {}
        count = int(data_header.get(1) or 0)
        payload = data[pos:pos + compressed_size]
        pos += compressed_size
        if not payload:
            raise ParquetError("空数据页")
        # Definition levels (column is OPTIONAL, max level 1).
        levels, payload_pos = _read_levels(payload, 0, count, 1)
        present = sum(1 for level in levels if level)
        plain, _ = _read_plain(payload, payload_pos, present, physical)
        iterator = iter(plain)
        for level in levels:
            value = next(iterator) if level else None
            if value is not None and physical == BYTE_ARRAY and converted == UTF8:
                value = value.decode("utf-8")
            values.append(value)
    if len(values) < num_values:
        raise ParquetError("列数据少于声明的行数")
    return values[:num_values]


def read_parquet(data: bytes) -> tuple[list[str], list[dict[str, Any]]]:
    """Read a Parquet file produced by :func:`write_parquet`.

    Returns ``(columns, records)`` with ``None`` preserved for nulls and integer
    columns returned as Python ints.
    """
    if len(data) < 12 or data[:4] != PARQUET_MAGIC or data[-4:] != PARQUET_MAGIC:
        raise ParquetError("文件缺少 PAR1 魔数，不是合法的 Parquet 文件")
    footer_length = struct.unpack_from("<I", data, len(data) - 8)[0]
    footer_start = len(data) - 8 - footer_length
    if footer_start < 4:
        raise ParquetError("Parquet 页脚长度异常")
    reader = _CompactReader(data, footer_start)
    metadata = reader.read_struct()
    schema, num_rows = _schema_from_metadata(metadata)
    leaves = [item for item in schema[1:] if item.get("num_children") is None]
    row_groups = metadata.get(4) or []
    if not row_groups:
        return [item["name"] for item in leaves], []
    row_group = row_groups[0]
    chunks = row_group.get(1) or []
    if len(chunks) != len(leaves):
        raise ParquetError("列块数量与 Schema 不一致")
    column_values: dict[str, list[Any]] = {}
    for leaf, chunk in zip(leaves, chunks):
        meta = chunk.get(3) or {}
        values = _decode_column_chunk(data, meta, int(leaf.get("type") or 0),
                                      leaf.get("converted_type"))
        column_values[leaf["name"]] = values
    columns = [leaf["name"] for leaf in leaves]
    records = [
        {name: column_values[name][row] for name in columns}
        for row in range(num_rows)
    ]
    return columns, records


def is_parquet(data: bytes) -> bool:
    return len(data) >= 8 and data[:4] == PARQUET_MAGIC and data[-4:] == PARQUET_MAGIC
