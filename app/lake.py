"""数据湖核心：结构版本、字段身份、批次登记与不可变 Parquet。

设计要点
--------
* 测量列的物理标识是永久不变的 ``field_id``；Parquet 中保存的就是 field_id。
  列名只是 ``fields`` 表上的一个可修改属性。
* 删除字段仅把状态置为 deleted。之后用同名新增会得到新的 field_id，
  旧批次里没有新 id 的列，读取时补 NULL，因此不会串值。
* 结构变更与批次登记都在 SQLite ``BEGIN IMMEDIATE`` 事务中完成，并校验
  调用方看到的 schema_version；过期则整次拒绝。
* 新 Parquet 先写临时文件、fsync、rename，再登记数据库。数据库提交失败会
  删除刚写的文件。历史 Parquet 永不以写方式打开。
* 一次页面/API 查询只在一个 SQLite 读事务里取得“结构 + 批次登记”快照。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import threading
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("metlake")

MAX_FIELDS = 8
MAX_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_STATION_LENGTH = 256
MAX_FIELD_NAME_LENGTH = 64

STATION_COLUMN = "station_id"
TIME_COLUMN = "observed_at"
RESERVED_NAMES = {STATION_COLUMN.casefold(), TIME_COLUMN.casefold()}
FIELD_ID_RE = re.compile(r"^f_[0-9a-f]{32}$")
INT_RE = re.compile(r"^[+-]?\d+$")
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

SCHEMA_VERSION_KEY = "schema_version"
UPLOAD_METADATA_PREFIX = b"metlake."
SOURCE_SUFFIX = ".csv"
PARQUET_SUFFIX = ".parquet"


class LakeError(Exception):
    """可安全展示给用户的领域错误。"""

    status_code = 400
    code = "bad_request"


class ValidationError(LakeError):
    status_code = 400
    code = "validation"


class StaleSchema(LakeError):
    status_code = 409
    code = "stale_schema"

    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"结构版本已过期：本次操作基于 v{expected}，当前已提交结构为 v{actual}。"
            "为避免旧结构写入，整次操作已被拒绝，请刷新页面后重试。"
        )


class NotFound(LakeError):
    status_code = 404
    code = "not_found"


class StorageError(LakeError):
    status_code = 500
    code = "storage_error"


class BatchIntegrityError(LakeError):
    status_code = 500
    code = "integrity_error"


class SourceNotFound(LakeError):
    """批次没有登记原件（旧批次），或批次不存在。"""

    status_code = 404
    code = "source_missing"


class SourceIntegrityError(LakeError):
    """批次登记了原件，但文件缺失 / 摘要不符，拒绝下载。"""

    status_code = 500
    code = "source_integrity_error"


# ---------------------------------------------------------------------------
# 配置 / 数据库基础
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    instance_name: str = "local"

    @classmethod
    def from_env(
        cls,
        data_dir: str | os.PathLike[str] | None = None,
        instance_name: str | None = None,
    ) -> "Settings":
        resolved = Path(data_dir or os.environ.get("DATA_DIR", "./data")).expanduser()
        name = instance_name or os.environ.get("INSTANCE_NAME", "local")
        return cls(data_dir=resolved.resolve(), instance_name=name)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "lake.sqlite3"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    @property
    def source_dir(self) -> Path:
        """上传原始 CSV 的留存目录（与转换产物分开，便于独立核对）。"""

        return self.data_dir / "source"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.source_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Field:
    id: str
    name: str
    ordinal: int
    status: str
    created_version: int
    deleted_version: int | None


@dataclass(frozen=True)
class Schema:
    version: int
    fields: tuple[Field, ...]

    @property
    def active(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "active")

    @property
    def deleted(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "deleted")


@dataclass(frozen=True)
class Batch:
    id: str
    created_at: str
    schema_version: int
    row_count: int
    file_name: str
    file_sha256: str
    fields_at_upload: tuple[dict, ...]
    source_file_name: str | None = None
    source_sha256: str | None = None
    source_size: int | None = None
    source_original_name: str | None = None

    @property
    def short_id(self) -> str:
        return self.id[:8]

    @property
    def has_source(self) -> bool:
        return self.source_file_name is not None and self.source_sha256 is not None


@dataclass(frozen=True)
class LakeState:
    schema: Schema
    batches: tuple[Batch, ...]


@dataclass(frozen=True)
class BatchView:
    schema: Schema
    batch: Batch
    table: pa.Table


@dataclass(frozen=True)
class ParsedCsv:
    stations: tuple[str, ...]
    times: tuple[datetime, ...]
    values: dict[str, tuple[int | None, ...]]
    header_fields: tuple[Field, ...]


@dataclass(frozen=True)
class IntegrityReport:
    ok: bool
    batches: int
    missing_files: tuple[str, ...]
    hash_mismatches: tuple[str, ...]
    orphan_files: tuple[str, ...]
    temp_files: tuple[str, ...]
    missing_source_files: tuple[str, ...] = ()
    source_hash_mismatches: tuple[str, ...] = ()
    orphan_source_files: tuple[str, ...] = ()
    source_temp_files: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceView:
    """原始上传 CSV；``data`` 必须与上传时逐字节一致。"""

    batch: Batch
    data: bytes


# 仅测试使用：Parquet 已写完、数据库提交前的钩子。
_before_commit_hook: Callable[[], None] | None = None


def set_before_commit_hook(hook: Callable[[], None] | None) -> None:
    """测试钩子：在转换完成、批次登记提交前执行。生产路径不设置。"""

    global _before_commit_hook
    _before_commit_hook = hook


def _connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(
        settings.db_path,
        timeout=30.0,
        isolation_level=None,  # 手动控制事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _migrate_source_columns(conn: sqlite3.Connection) -> None:
    """为旧库补上原件留存列；旧批次保持 NULL（明确视为“无原件”）。

    两实例并发初始化时由 ``BEGIN IMMEDIATE`` 串行化，重复 ALTER 不会发生。
    """

    existing = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(batches)").fetchall()
    }
    additions = {
        "source_file_name": "TEXT",
        "source_sha256": "TEXT",
        "source_size": "INTEGER",
        "source_original_name": "TEXT",
    }
    for column, sql_type in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE batches ADD COLUMN {column} {sql_type}")


def initialize(settings: Settings) -> None:
    """幂等初始化；两个实例同时启动也安全。"""

    settings.ensure_dirs()
    last_error: Exception | None = None
    for attempt in range(60):
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                settings.db_path,
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 15000")
            mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                log.warning("SQLite journal_mode=%s (期望 wal)", mode)
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fields (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
                    created_version INTEGER NOT NULL,
                    deleted_version INTEGER,
                    created_at TEXT NOT NULL,
                    deleted_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fields_active_name
                ON fields(name COLLATE NOCASE)
                WHERE status = 'active'
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_fields_status_ordinal "
                "ON fields(status, ordinal)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    file_name TEXT NOT NULL UNIQUE,
                    file_sha256 TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    source_file_name TEXT,
                    source_sha256 TEXT,
                    source_size INTEGER,
                    source_original_name TEXT
                )
                """
            )
            _migrate_source_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_batches_created_at "
                "ON batches(created_at DESC)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES (?, '0')",
                (SCHEMA_VERSION_KEY,),
            )
            conn.execute("COMMIT")
            _cleanup_stale_temps(settings)
            _cleanup_orphan_files(settings)
            return
        except sqlite3.Error as exc:  # 并发启动时可能 busy
            last_error = exc
            if conn is not None:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            time.sleep(min(0.05 * (attempt + 1), 0.5))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
    raise StorageError(
        f"无法初始化数据库 {settings.db_path}：{last_error}"
    ) from last_error


@contextmanager
def _write_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    committed = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
        committed = True
    except BaseException:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            # 关闭失败不改变事务结果；已有提交/回滚结论。
            log.exception("关闭数据库连接失败")


@contextmanager
def _read_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    try:
        conn.execute("BEGIN")
        yield conn
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        conn.close()


# 读路径使用 ``SELECT *``：尚未执行 initialize 迁移的旧库也能读，缺失的
# 原件列由 _row_value 视为 NULL（明确当作“无原件”）。
_BATCH_COLUMNS = "*"


def _row_value(row: sqlite3.Row, key: str):
    try:
        return row[key]
    except (IndexError, KeyError):  # 兼容尚未迁移的旧库（只读路径）
        return None


def _rows_to_batch(row: sqlite3.Row) -> Batch:
    try:
        fields = tuple(json.loads(row["schema_json"]))
    except (TypeError, ValueError):
        fields = ()
    source_sha256 = _row_value(row, "source_sha256")
    source_size = _row_value(row, "source_size")
    return Batch(
        id=row["id"],
        created_at=row["created_at"],
        schema_version=int(row["schema_version"]),
        row_count=int(row["row_count"]),
        file_name=row["file_name"],
        file_sha256=row["file_sha256"],
        fields_at_upload=fields,
        source_file_name=_row_value(row, "source_file_name"),
        source_sha256=str(source_sha256) if source_sha256 is not None else None,
        source_size=int(source_size) if source_size is not None else None,
        source_original_name=_row_value(row, "source_original_name"),
    )


def _query_schema(conn: sqlite3.Connection) -> Schema:
    version_row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if version_row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    version = int(version_row["value"])
    rows = conn.execute(
        "SELECT id, name, ordinal, status, created_version, deleted_version "
        "FROM fields ORDER BY ordinal ASC, created_at ASC"
    ).fetchall()
    fields = tuple(
        Field(
            id=r["id"],
            name=r["name"],
            ordinal=int(r["ordinal"]),
            status=r["status"],
            created_version=int(r["created_version"]),
            deleted_version=(
                int(r["deleted_version"]) if r["deleted_version"] is not None else None
            ),
        )
        for r in rows
    )
    return Schema(version=version, fields=fields)


def read_state(settings: Settings) -> LakeState:
    """在一个读事务中取得一致的结构 + 批次登记快照。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            rows = conn.execute(
                f"SELECT {_BATCH_COLUMNS} FROM batches "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()
            batches = tuple(_rows_to_batch(r) for r in rows)
    except sqlite3.Error as exc:
        raise StorageError("读取数据湖状态失败。") from exc
    return LakeState(schema=schema, batches=batches)


def read_schema(settings: Settings) -> Schema:
    try:
        with _read_transaction(settings) as conn:
            return _query_schema(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取结构失败。") from exc


# ---------------------------------------------------------------------------
# 字段结构变更
# ---------------------------------------------------------------------------


def normalize_field_name(raw: str | None) -> str:
    if raw is None:
        raise ValidationError("字段名不能为空。")
    name = unicodedata.normalize("NFC", str(raw).strip())
    if not name:
        raise ValidationError("字段名不能为空。")
    if len(name) > MAX_FIELD_NAME_LENGTH:
        raise ValidationError(f"字段名最长 {MAX_FIELD_NAME_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        raise ValidationError("字段名不能包含控制字符。")
    if name.casefold() in RESERVED_NAMES:
        raise ValidationError(
            f"“{name}”是固定列名（{STATION_COLUMN} / {TIME_COLUMN}），不能用作测量列名。"
        )
    return name


def _read_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    return int(row["value"])


def _check_expected_version(conn: sqlite3.Connection, expected: int) -> int:
    actual = _read_version(conn)
    if expected != actual:
        raise StaleSchema(expected, actual)
    return actual


def _bump_version(conn: sqlite3.Connection, current: int) -> int:
    new_version = current + 1
    conn.execute(
        "UPDATE meta SET value = ? WHERE key = ?",
        (str(new_version), SCHEMA_VERSION_KEY),
    )
    return new_version


def _next_ordinal(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(ordinal), -1) AS m FROM fields").fetchone()
    return int(row["m"]) + 1


def _ensure_name_free(
    conn: sqlite3.Connection, name: str, exclude_id: str | None = None
) -> None:
    rows = conn.execute(
        "SELECT id, name FROM fields WHERE status = 'active'"
    ).fetchall()
    folded = name.casefold()
    for row in rows:
        if exclude_id is not None and row["id"] == exclude_id:
            continue
        if row["name"].casefold() == folded:
            raise ValidationError(f"测量列“{name}”已存在。")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def add_field(settings: Settings, expected_version: int, raw_name: str) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM fields WHERE status = 'active'"
            ).fetchone()["n"]
            if int(active) >= MAX_FIELDS:
                raise ValidationError(
                    f"测量列最多 {MAX_FIELDS} 个，请先删除不再使用的列。"
                )
            _ensure_name_free(conn, name)
            new_version = _bump_version(conn, version)
            conn.execute(
                "INSERT INTO fields(id, name, ordinal, status, created_version, "
                "deleted_version, created_at, deleted_at) "
                "VALUES (?, ?, ?, 'active', ?, NULL, ?, NULL)",
                (
                    f"f_{uuid.uuid4().hex}",
                    name,
                    _next_ordinal(conn),
                    new_version,
                    _utc_now_iso(),
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("新增测量列失败，结构未改变。") from exc
    return read_schema(settings)


def rename_field(
    settings: Settings, expected_version: int, field_id: str, raw_name: str
) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id, name FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要改名的测量列不存在或已删除。")
            if row["name"] == name:
                raise ValidationError("新名称与当前名称相同。")
            _ensure_name_free(conn, name, exclude_id=field_id)
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET name = ? WHERE id = ?", (name, field_id)
            )
            log.info("renamed %s -> %s at v%s", field_id, name, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("测量列改名失败，结构未改变。") from exc
    return read_schema(settings)


def delete_field(settings: Settings, expected_version: int, field_id: str) -> Schema:
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要删除的测量列不存在或已删除。")
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET status = 'deleted', deleted_version = ?, "
                "deleted_at = ? WHERE id = ?",
                (new_version, _utc_now_iso(), field_id),
            )
            log.info("deleted %s at v%s", field_id, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("删除测量列失败，结构未改变。") from exc
    return read_schema(settings)


# ---------------------------------------------------------------------------
# CSV -> Parquet
# ---------------------------------------------------------------------------


def parse_csv(data: bytes, schema: Schema) -> ParsedCsv:
    if not data:
        raise ValidationError("上传文件为空。")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValidationError(
            f"CSV 文件不能超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MiB。"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须是 UTF-8 编码（可带 BOM）。") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise ValidationError("CSV 缺少表头。") from None

    header = [unicodedata.normalize("NFC", h.strip()) for h in raw_header]
    if not header or all(not h for h in header):
        raise ValidationError("CSV 表头为空。")
    if any(not h for h in header):
        raise ValidationError("CSV 表头中存在空列名。")
    if len(set(header)) != len(header):
        raise ValidationError("CSV 表头中存在重复列名。")

    fixed: dict[str, int] = {}
    for required in (STATION_COLUMN, TIME_COLUMN):
        try:
            fixed[required] = header.index(required)
        except ValueError:
            raise ValidationError(
                f"CSV 必须包含固定列“{required}”。"
            ) from None
        if header.count(required) != 1:
            raise ValidationError(f"固定列“{required}”只能出现一次。")

    active = schema.active
    by_name = {f.name: f for f in active}
    selected: dict[str, Field] = {}
    unknown: list[str] = []
    for idx, name in enumerate(header):
        if idx in fixed.values():
            continue
        field = by_name.get(name)
        if field is None:
            unknown.append(name)
        else:
            selected[name] = field
    if unknown:
        available = "、".join(f.name for f in active) or "（无）"
        raise ValidationError(
            "CSV 含当前结构之外的测量列："
            + "、".join(unknown)
            + f"。当前可接收的测量列为：{available}。"
            "请刷新页面使用最新结构导出/上传。"
        )

    stations: list[str] = []
    times: list[datetime] = []
    values: dict[str, list[int | None]] = {
        field.id: [] for field in selected.values()
    }

    station_idx = header.index(STATION_COLUMN)
    time_idx = header.index(TIME_COLUMN)
    selected_by_index = {
        idx: field for idx, name in enumerate(header) if (field := selected.get(name))
    }

    row_count = 0
    for row in reader:
        if not row:
            continue
        row_count += 1
        if row_count > MAX_ROWS:
            raise ValidationError(f"每批最多 {MAX_ROWS} 行数据。")
        line_no = reader.line_num
        if len(row) != len(header):
            raise ValidationError(
                f"CSV 第 {line_no} 行列数为 {len(row)}，表头为 {len(header)} 列。"
            )

        station = row[station_idx].strip()
        if not station:
            raise ValidationError(f"CSV 第 {line_no} 行站号为空。")
        if len(station) > MAX_STATION_LENGTH:
            raise ValidationError(f"CSV 第 {line_no} 行站号过长。")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
            raise ValidationError(f"CSV 第 {line_no} 行站号含控制字符。")
        stations.append(station)

        raw_time = row[time_idx].strip()
        if not raw_time:
            raise ValidationError(f"CSV 第 {line_no} 行 UTC 时刻为空。")
        try:
            parsed_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(
                f"CSV 第 {line_no} 行时刻“{raw_time}”不是 ISO 8601 格式。"
            ) from None
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        else:
            parsed_time = parsed_time.astimezone(timezone.utc)
        times.append(parsed_time)

        for idx, field in selected_by_index.items():
            raw_value = row[idx].strip()
            if raw_value == "":
                parsed_value: int | None = None
            else:
                if not INT_RE.fullmatch(raw_value):
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值“{raw_value}”"
                        "不是整数。空值请留空。"
                    )
                parsed_value = int(raw_value)
                if parsed_value < INT64_MIN or parsed_value > INT64_MAX:
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值超出 64 位整数范围。"
                    )
            values[field.id].append(parsed_value)

    if row_count == 0:
        raise ValidationError("CSV 至少需要 1 行数据。")

    filled: dict[str, tuple[int | None, ...]] = {}
    for field in active:
        if field.id in values:
            filled[field.id] = tuple(values[field.id])
        else:
            filled[field.id] = tuple([None] * row_count)

    return ParsedCsv(
        stations=tuple(stations),
        times=tuple(times),
        values=filled,
        header_fields=tuple(selected.values()),
    )


def _build_upload_table(parsed: ParsedCsv, schema: Schema) -> pa.Table:
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        pa.array(parsed.stations, type=pa.string()),
        pa.array(parsed.times, type=pa.timestamp("us", tz="UTC")),
    ]
    for field in schema.active:
        names.append(field.id)
        arrays.append(pa.array(parsed.values[field.id], type=pa.int64()))
    return pa.Table.from_arrays(arrays, names=names)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    try:
        _fsync_path(path)
    except OSError as exc:  # 某些文件系统不支持目录 fsync
        log.warning("目录 fsync 失败 %s: %s", path, exc)


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        log.exception("清理文件失败 %s", path)


# 本进程正在上传、尚未登记的文件；清理逻辑绝不触碰它们。
_in_flight_paths: set[Path] = set()
_in_flight_lock = threading.Lock()


@contextmanager
def _in_flight(*paths: Path) -> Iterator[None]:
    with _in_flight_lock:
        _in_flight_paths.update(paths)
    try:
        yield
    finally:
        with _in_flight_lock:
            _in_flight_paths.difference_update(paths)


def _is_in_flight(path: Path, cutoff: float) -> bool:
    """判断文件是否正被上传写入。

    同进程内用内存集合判断；跨实例则看目标文件旁的租约标记：标记仍在且
    未过期 => 另一实例正在写，清理逻辑不得触碰。崩溃遗留的过期标记会在
    清理时一并移除。
    """

    if _is_local_in_flight(path):
        return True
    marker = _lease_marker(path)
    try:
        return marker.is_file() and marker.stat().st_mtime >= cutoff
    except OSError:
        return False


def _is_local_in_flight(path: Path) -> bool:
    with _in_flight_lock:
        return path in _in_flight_paths


def _lease_marker(path: Path) -> Path:
    return path.with_name(f".{path.name}.lock")


def _refresh_lease(path: Path) -> None:
    try:
        os.utime(_lease_marker(path), None)
    except OSError:
        pass  # 标记可能被清理；本进程内存集合仍然保护


@contextmanager
def _upload_lease(*paths: Path) -> Iterator[None]:
    """为即将落盘的最终文件建立租约，跨进程标记“在途上传”。"""

    markers = [_lease_marker(p) for p in paths]
    created: list[Path] = []
    try:
        for marker in markers:
            try:
                marker.write_text(
                    f"{os.getpid()} {time.time():.3f}\n", encoding="ascii"
                )
                created.append(marker)
            except OSError:
                log.warning("无法创建上传租约标记 %s", marker)
        with _in_flight(*paths):
            yield
    finally:
        for marker in created:
            _safe_unlink(marker)


def _file_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _old_enough(path: Path, cutoff: float) -> bool:
    mtime = _file_mtime(path)
    return mtime is not None and mtime < cutoff


def _cleanup_stale_temps(settings: Settings) -> None:
    """清理超过 1 小时的临时文件。

    新写入的临时文件命名以点号开头（``.xxx.tmp``），这里的 glob 覆盖两种形态；
    用“足够旧”阈值避免误删另一实例正在写入的临时文件。
    """

    cutoff = time.time() - 3600
    for directory in (settings.parquet_dir, settings.source_dir):
        try:
            entries = list(directory.iterdir())
        except OSError:
            log.warning("扫描临时文件失败：%s", directory)
            continue
        candidates = [
            p
            for p in entries
            if p.suffix == ".tmp" and p.is_file() and _old_enough(p, cutoff)
        ]
        for path in candidates:
            if _is_in_flight(path, cutoff):
                continue
            try:
                path.unlink(missing_ok=True)
                log.warning("清理陈旧临时文件：%s", path.name)
            except OSError:
                log.warning("无法清理临时文件 %s", path)
        # 崩溃实例遗留的过期租约标记；仍然新鲜、属于本进程或本进程
        # 正在上传的目标文件，都要保留。
        for marker in entries:
            if not marker.name.endswith(".lock") or not marker.is_file():
                continue
            target = directory / marker.name[1 : -len(".lock")]
            try:
                if not _old_enough(marker, cutoff) or _is_local_in_flight(target):
                    continue
                marker.unlink(missing_ok=True)
            except OSError:
                log.warning("无法清理过期上传租约 %s", marker)


def _cleanup_orphan_files(settings: Settings) -> None:
    """清理崩溃遗留的、未被任何批次登记的 Parquet 与原件 CSV。

    只处理超过 1 小时、且没有在途租约的文件。删除动作放在一个
    ``BEGIN IMMEDIATE`` 写事务里：登记批次同样要拿写锁，因此“查登记表 ->
    删除”之间不可能插入新的批次登记，不会误删刚提交或仍在途的上传。
    """

    cutoff = time.time() - 3600
    scan_pairs = (
        (settings.parquet_dir, PARQUET_SUFFIX, "file_name", "Parquet"),
        (settings.source_dir, SOURCE_SUFFIX, "source_file_name", "原件 CSV"),
    )
    candidates: list[tuple[Path, str, str]] = []
    for directory, suffix, attr, label in scan_pairs:
        try:
            found = list(directory.glob(f"*{suffix}"))
        except OSError:
            log.warning("扫描未登记 %s 失败", label)
            continue
        for path in found:
            if not path.is_file() or not _old_enough(path, cutoff):
                continue
            if _is_in_flight(path, cutoff):
                continue
            candidates.append((path, attr, label))
    if not candidates:
        return
    try:
        with _write_transaction(settings) as conn:
            rows = conn.execute(
                "SELECT file_name, source_file_name FROM batches"
            ).fetchall()
            registered = {
                "file_name": {r["file_name"] for r in rows},
                "source_file_name": {
                    r["source_file_name"]
                    for r in rows
                    if r["source_file_name"] is not None
                },
            }
            for path, attr, label in candidates:
                if path.name in registered[attr]:
                    continue
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    log.warning("无法清理孤立 %s：%s", label, path.name)
                    continue
                log.warning("清理未登记的孤立 %s：%s", label, path.name)
    except (LakeError, sqlite3.Error):
        log.warning("清理孤立文件时无法获取登记表，本轮跳过")


def _write_bytes_atomic(data: bytes, final_path: Path) -> str:
    """原件 CSV：临时文件 -> fsync -> 校验摘要 -> rename -> fsync 目录。

    返回落盘文件的 sha256；调用方保证 ``data`` 就是上传时的原始字节。
    """

    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp_path.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        digest = _sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return digest
    except BaseException:
        _safe_unlink(tmp_path)
        raise


def _write_parquet_atomic(
    table: pa.Table, final_path: Path, metadata: dict[bytes, bytes]
) -> str:
    """临时文件 -> fsync -> rename -> fsync 目录；返回内容 sha256。"""

    table = table.replace_schema_metadata(metadata)
    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path, compression="snappy")
        _fsync_path(tmp_path)
        digest = _sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return digest
    except BaseException:
        _safe_unlink(tmp_path)
        raise


# ---------------------------------------------------------------------------
# 批次上传与读取
# ---------------------------------------------------------------------------


def _safe_original_name(raw: str | None, fallback: str) -> str:
    """保留上传文件名用于展示；只取 basename，去掉控制字符，限制长度。"""

    if not raw:
        return fallback
    name = str(raw).replace("\\", "/").split("/")[-1]
    name = "".join(ch for ch in name if ord(ch) >= 32 and ord(ch) != 127).strip()
    if not name:
        return fallback
    return name[:200]


def upload_csv(
    settings: Settings,
    expected_version: int,
    data: bytes,
    original_name: str | None = None,
) -> Batch:
    """解析并转换后，在最终提交前再次校验结构版本。

    原件 CSV 与转换后的 Parquet 都先完整落盘（写入、fsync、rename），
    只有两者都成功且结构版本仍是上传者所见版本时才登记批次。转换/写文件
    期间不持有数据库写锁，因此结构仍可被另一进程修改；提交时若版本已变，
    批次绝不登记，刚写的两份文件都会删除。原件的 SHA-256 由上传字节直接
    计算，登记后不再随结构变化而改变。
    """

    schema = read_schema(settings)
    if schema.version != expected_version:
        raise StaleSchema(expected_version, schema.version)

    parsed = parse_csv(data, schema)
    table = _build_upload_table(parsed, schema)

    batch_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    file_name = f"{batch_id}{PARQUET_SUFFIX}"
    source_file_name = f"{batch_id}{SOURCE_SUFFIX}"
    final_path = settings.parquet_dir / file_name
    source_path = settings.source_dir / source_file_name
    source_sha256 = hashlib.sha256(data).hexdigest()
    fields_at_upload = [
        {"id": f.id, "name": f.name, "ordinal": f.ordinal} for f in schema.active
    ]
    schema_json = json.dumps(fields_at_upload, ensure_ascii=False, separators=(",", ":"))
    metadata = {
        UPLOAD_METADATA_PREFIX + b"format": b"2",
        UPLOAD_METADATA_PREFIX + b"batch_id": batch_id.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"schema_version": str(schema.version).encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"created_at": created_at.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"fields": schema_json.encode("utf-8"),
        UPLOAD_METADATA_PREFIX + b"source_sha256": source_sha256.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"source_size": str(len(data)).encode("ascii"),
    }

    with _upload_lease(final_path, source_path):
        try:
            parquet_sha256 = _write_parquet_atomic(table, final_path, metadata)
            _refresh_lease(final_path)
            written_source_sha256 = _write_bytes_atomic(data, source_path)
            _refresh_lease(source_path)
        except BaseException as exc:
            _safe_unlink(final_path)
            _safe_unlink(source_path)
            if isinstance(exc, (OSError, pa.ArrowException)):
                raise StorageError("写入批次文件失败，批次未登记。") from exc
            raise
        if written_source_sha256 != source_sha256:
            _safe_unlink(final_path)
            _safe_unlink(source_path)
            raise StorageError("原件摘要校验失败，批次未登记。")

        try:
            if _before_commit_hook is not None:
                _before_commit_hook()
            # 提交前再次确认两份文件都在位，绝不让新批次只登记到一份文件。
            if not final_path.is_file() or not source_path.is_file():
                raise StorageError("批次文件在登记前丢失，批次未登记。")
            batch = _commit_batch(
                settings=settings,
                expected_version=expected_version,
                batch_id=batch_id,
                created_at=created_at,
                row_count=len(parsed.stations),
                file_name=file_name,
                file_sha256=parquet_sha256,
                schema_json=schema_json,
                fields_at_upload=fields_at_upload,
                source_file_name=source_file_name,
                source_sha256=source_sha256,
                source_size=len(data),
                source_original_name=_safe_original_name(
                    original_name, f"batch-{batch_id[:8]}.csv"
                ),
            )
        except BaseException:
            _safe_unlink(final_path)
            _safe_unlink(source_path)
            raise
    log.info(
        "batch %s committed rows=%s schema=v%s source=%s",
        batch_id,
        len(parsed.stations),
        schema.version,
        source_file_name,
    )
    return batch


def _commit_batch(
    *,
    settings: Settings,
    expected_version: int,
    batch_id: str,
    created_at: str,
    row_count: int,
    file_name: str,
    file_sha256: str,
    schema_json: str,
    fields_at_upload: Sequence[dict],
    source_file_name: str | None = None,
    source_sha256: str | None = None,
    source_size: int | None = None,
    source_original_name: str | None = None,
) -> Batch:
    try:
        with _write_transaction(settings) as conn:
            _check_expected_version(conn, expected_version)
            conn.execute(
                "INSERT INTO batches(id, created_at, schema_version, row_count, "
                "file_name, file_sha256, schema_json, source_file_name, "
                "source_sha256, source_size, source_original_name) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    created_at,
                    expected_version,
                    row_count,
                    file_name,
                    file_sha256,
                    schema_json,
                    source_file_name,
                    source_sha256,
                    source_size,
                    source_original_name,
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("批次登记失败，未写入新批次。") from exc
    return Batch(
        id=batch_id,
        created_at=created_at,
        schema_version=expected_version,
        row_count=row_count,
        file_name=file_name,
        file_sha256=file_sha256,
        fields_at_upload=tuple(fields_at_upload),
        source_file_name=source_file_name,
        source_sha256=source_sha256,
        source_size=source_size,
        source_original_name=source_original_name,
    )


def get_batch(settings: Settings, batch_id: str) -> BatchView:
    """读取批次；结构快照与批次登记来自同一个已提交读事务。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            row = conn.execute(
                f"SELECT {_BATCH_COLUMNS} FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc

    if row is None:
        raise NotFound("批次不存在。")
    batch = _rows_to_batch(row)
    physical = _read_physical_table(settings, batch)
    current = _materialize_current_table(physical, schema)
    return BatchView(schema=schema, batch=batch, table=current)


def get_source(settings: Settings, batch_id: str) -> SourceView:
    """按登记摘要返回上传时的原始字节。

    原件校验独立于 Parquet 视图：即使转换产物损坏，只要原件完整仍可核对；
    反之原件缺失或摘要不符则拒绝下载并说明原因，绝不回退到重新生成的 CSV。
    """

    try:
        with _read_transaction(settings) as conn:
            row = conn.execute(
                f"SELECT {_BATCH_COLUMNS} FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc
    if row is None:
        raise NotFound("批次不存在。")

    batch = _rows_to_batch(row)
    if not batch.has_source:
        raise SourceNotFound(
            f"批次 {batch.short_id} 是启用原件留存之前登记的旧批次，"
            "没有保存原始 CSV，无法下载原件；Parquet 数据仍可正常查看和下载。"
        )

    path = settings.source_dir / str(batch.source_file_name)
    if not path.is_file():
        raise SourceIntegrityError(
            f"批次 {batch.short_id} 登记的原件 {batch.source_file_name} 已缺失，"
            "拒绝提供原件下载。可继续使用 Parquet 数据，或通过 /api/integrity 核对。"
        )
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise SourceIntegrityError(
            f"批次 {batch.short_id} 的原件无法读取，拒绝提供原件下载。"
        ) from exc
    digest = hashlib.sha256(data).hexdigest()
    size_mismatch = batch.source_size is not None and len(data) != batch.source_size
    if size_mismatch or digest != batch.source_sha256:
        raise SourceIntegrityError(
            f"批次 {batch.short_id} 的原件摘要不符：登记 "
            f"{batch.source_size} 字节 / SHA-256 {batch.source_sha256}，实际 "
            f"{len(data)} 字节 / SHA-256 {digest}。"
            "文件可能被篡改或损坏，拒绝提供原件下载。"
        )
    return SourceView(batch=batch, data=data)


def source_status(settings: Settings, batch: Batch) -> str:
    """网页展示用的轻量原件状态：``ok`` / ``none`` / ``broken``。

    只分块计算摘要，不把整个文件读进内存；不抛异常，保证 Parquet 页面可用。
    """

    if not batch.has_source:
        return "none"
    path = settings.source_dir / str(batch.source_file_name)
    try:
        if not path.is_file():
            return "broken"
        if batch.source_size is not None and path.stat().st_size != batch.source_size:
            return "broken"
        if batch.source_sha256 is not None and _sha256_file(path) != batch.source_sha256:
            return "broken"
    except OSError:
        return "broken"
    return "ok"


def _read_physical_table(settings: Settings, batch: Batch) -> pa.Table:
    path = settings.parquet_dir / batch.file_name
    if not path.is_file():
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的 Parquet 文件缺失：{batch.file_name}"
        )
    try:
        table = pq.read_table(path)
    except (OSError, pa.ArrowException) as exc:
        raise BatchIntegrityError(f"批次 {batch.id[:8]} 的 Parquet 无法读取。") from exc
    for fixed in (STATION_COLUMN, TIME_COLUMN):
        if fixed not in table.column_names:
            raise BatchIntegrityError(f"批次 {batch.id[:8]} 缺少固定列 {fixed}。")
    return table


def _materialize_current_table(physical: pa.Table, schema: Schema) -> pa.Table:
    """按当前已提交结构生成只读视图；不重写历史文件。

    当前字段 id 在历史文件中不存在（新字段，或删除后同名重建）时补 NULL。
    """

    row_count = physical.num_rows
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        physical.column(STATION_COLUMN).combine_chunks().cast(pa.string()),
        physical.column(TIME_COLUMN).combine_chunks().cast(
            pa.timestamp("us", tz="UTC")
        ),
    ]
    for field in schema.active:
        names.append(field.name)
        if field.id in physical.column_names:
            try:
                column = physical.column(field.id).combine_chunks().cast(pa.int64())
            except (pa.ArrowException, KeyError) as exc:
                raise BatchIntegrityError(
                    f"历史 Parquet 的列 {field.id} 类型异常。"
                ) from exc
        else:
            column = pa.nulls(row_count, type=pa.int64())
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def csv_bytes(table: pa.Table) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(table.column_names)
    for record in table.to_pylist():
        row: list[str] = []
        for name in table.column_names:
            value = record[name]
            if value is None:
                row.append("")
            elif isinstance(value, datetime):
                row.append(format_utc(value))
            else:
                row.append(str(value))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def display_cell(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return format_utc(value)
    return str(value)


def rows_as_display(table: pa.Table) -> tuple[list[str], list[list[str | None]]]:
    columns = list(table.column_names)
    rows = [
        [display_cell(record[name]) for name in columns]
        for record in table.to_pylist()
    ]
    return columns, rows


def sample_csv(schema: Schema, rows: int = 5) -> bytes:
    """按当前结构生成可直接上传的示例 CSV，用于网页演示。"""

    stations = ["54511", "54512", "58362", "50953", "45007"]
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    active = schema.active
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow([STATION_COLUMN, TIME_COLUMN, *[f.name for f in active]])
    for i in range(rows):
        row = [
            stations[i % len(stations)],
            format_utc(base.replace(hour=i % 24, minute=(i * 7) % 60)),
        ]
        for j, _field in enumerate(active):
            # 留一个空值，演示可空整数列
            row.append("" if (i + j) % 4 == 3 else str(10 + (i * 3 + j * 5) % 40))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def check_integrity(settings: Settings) -> IntegrityReport:
    state = read_state(settings)
    registered = {b.file_name for b in state.batches}
    registered_sources = {
        b.source_file_name for b in state.batches if b.source_file_name is not None
    }
    missing: list[str] = []
    mismatched: list[str] = []
    missing_sources: list[str] = []
    mismatched_sources: list[str] = []
    try:
        disk_files = {
            p.name for p in settings.parquet_dir.glob(f"*{PARQUET_SUFFIX}") if p.is_file()
        }
        temp_files = sorted(
            p.name
            for p in settings.parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file()
        )
        disk_sources = {
            p.name for p in settings.source_dir.glob(f"*{SOURCE_SUFFIX}") if p.is_file()
        }
        source_temp_files = sorted(
            p.name
            for p in settings.source_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file()
        )
    except OSError as exc:
        raise StorageError("扫描数据文件目录失败。") from exc

    for batch in state.batches:
        path = settings.parquet_dir / batch.file_name
        if not path.is_file():
            missing.append(batch.file_name)
            continue
        try:
            if _sha256_file(path) != batch.file_sha256:
                mismatched.append(batch.file_name)
        except OSError:
            missing.append(batch.file_name)

    for batch in state.batches:
        if batch.source_file_name is None:
            continue  # 旧批次没有登记原件，属于已知状态，不算不一致
        path = settings.source_dir / batch.source_file_name
        if not path.is_file():
            missing_sources.append(batch.source_file_name)
            continue
        try:
            if batch.source_sha256 is not None and (
                _sha256_file(path) != batch.source_sha256
            ):
                mismatched_sources.append(batch.source_file_name)
            elif batch.source_size is not None and (
                path.stat().st_size != batch.source_size
            ):
                mismatched_sources.append(batch.source_file_name)
        except OSError:
            missing_sources.append(batch.source_file_name)

    # 正在转换、尚未登记的新文件是正常瞬时状态；只把 60 秒以上的未登记文件
    # 视为孤立文件，避免并发上传时误报。
    orphan_cutoff = time.time() - 60
    orphans: list[str] = []
    for name in sorted(disk_files - registered):
        path = settings.parquet_dir / name
        try:
            if path.stat().st_mtime < orphan_cutoff and not _is_in_flight(
                path, orphan_cutoff
            ):
                orphans.append(name)
        except OSError:
            continue
    orphan_sources: list[str] = []
    for name in sorted(disk_sources - registered_sources):
        path = settings.source_dir / name
        try:
            if path.stat().st_mtime < orphan_cutoff and not _is_in_flight(
                path, orphan_cutoff
            ):
                orphan_sources.append(name)
        except OSError:
            continue
    return IntegrityReport(
        ok=not missing
        and not mismatched
        and not missing_sources
        and not mismatched_sources,
        batches=len(state.batches),
        missing_files=tuple(missing),
        hash_mismatches=tuple(mismatched),
        orphan_files=tuple(orphans),
        temp_files=tuple(temp_files),
        missing_source_files=tuple(missing_sources),
        source_hash_mismatches=tuple(mismatched_sources),
        orphan_source_files=tuple(orphan_sources),
        source_temp_files=tuple(source_temp_files),
    )
