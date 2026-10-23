"""Hash operations (hset/hget/hincrby/...) over JSON object values.

Every test runs twice: once on the JSON1 single-statement path, and once
with the read-modify-write fallback forced, so both are proven equivalent.
"""

from __future__ import annotations

import dataclasses
import json
import multiprocessing
import sqlite3
import time

import pytest
from _workers import hincrby_worker, hset_field_worker

from lytecache import CacheFullError, LyteCache, SerializationError
from lytecache._schema import TYPE_BYTES, TYPE_FLOAT, TYPE_INT, TYPE_JAVA, TYPE_PICKLE, TYPE_STR

WEIRD_FIELDS = [
    "a.b", "a[0]", "a]b", 'q"uote', "back\\slash", '\\"', "sp ace", "ünï 字", "", "$", "*",
]


@pytest.fixture(params=["json1", "fallback"])
def mode(request, monkeypatch):
    if request.param == "fallback":
        monkeypatch.setenv("LYTECACHE_FORCE_JSON_FALLBACK", "1")
    else:
        monkeypatch.delenv("LYTECACHE_FORCE_JSON_FALLBACK", raising=False)
    return request.param


@pytest.fixture
def hc(db_path, mode):
    c = LyteCache(db_path, sweep_interval=None)
    assert c.stats()["hash_mode"] == mode
    yield c
    c.close()


def raw(db_path, key):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT value, value_type, expires_at, size_bytes, length(value), last_accessed, "
            "access_count FROM cache WHERE namespace = 'default' AND key = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()


def insert_raw(db_path, key, blob, vtype):
    now = int(time.time() * 1000)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, "
        "last_accessed, access_count, size_bytes) VALUES ('default', ?, ?, ?, ?, NULL, ?, 0, ?)",
        (key, blob, vtype, now, now, len(blob)),
    )
    conn.commit()
    conn.close()


# -- happy paths ------------------------------------------------------------


def test_hset_hget_roundtrip_natural_types(hc):
    values = {
        "s": "text", "i": 42, "f": 1.5, "t": True, "n": None, "o": {"x": [1, 2]}, "l": [1, "a"],
    }
    for f, v in values.items():
        assert hc.hset("h", f, v) is True
    for f, v in values.items():
        got = hc.hget("h", f)
        assert got == v and type(got) is type(v)
    assert hc.hgetall("h") == values


def test_hset_reports_created_vs_replaced(hc):
    assert hc.hset("h", "a", 1) is True
    assert hc.hset("h", "a", 2) is False
    assert hc.hset("h", "b", 3) is True
    assert hc.hgetall("h") == {"a": 2, "b": 3}


def test_hash_is_a_view_over_ordinary_json_values(hc):
    hc.set("user:1", {"name": "Ada", "age": 36})
    assert hc.hget("user:1", "name") == "Ada"
    hc.hset("user:1", "status", "active")
    assert hc.get("user:1") == {"name": "Ada", "age": 36, "status": "active"}


def test_hset_many(hc):
    hc.hset("h", "keep", 0)
    assert hc.hset_many("h", {"a": 1, "b": "two"}) is None
    assert hc.hgetall("h") == {"keep": 0, "a": 1, "b": "two"}
    hc.hset_many("h", {})  # no-op


def test_hset_many_large_mapping_beyond_sqlite_argument_limits(hc):
    mapping = {f"f{i}": i for i in range(1234)}
    hc.hset_many("big", mapping)
    assert hc.hgetall("big") == mapping
    assert hc.hlen("big") == 1234


def test_hsetnx(hc):
    assert hc.hsetnx("h", "a", 1) is True
    assert hc.hsetnx("h", "a", 2) is False
    assert hc.hget("h", "a") == 1
    assert hc.hsetnx("h", "b", 3) is True


def test_hmget_in_order_with_none_for_missing(hc):
    hc.hset_many("h", {"a": 1, "b": 2})
    assert hc.hmget("h", ["b", "nope", "a"]) == [2, None, 1]
    assert hc.hmget("missing", ["a", "b"]) == [None, None]
    assert hc.hmget("h", []) == []


def test_hdel_counts_only_present_fields(hc):
    hc.hset_many("h", {"a": 1, "b": 2, "c": 3})
    assert hc.hdel("h", "a", "nope", "a") == 1
    assert hc.hdel("h", "b", "c") == 2
    assert hc.hdel("h", "b") == 0
    assert hc.hdel("missing", "x") == 0
    assert hc.hdel("h") == 0


def test_hdel_last_field_leaves_empty_object(hc):
    hc.hset("h", "a", 1)
    hc.hdel("h", "a")
    assert hc.exists("h")
    assert hc.get("h") == {}


def test_hexists_hkeys_hvals_hlen(hc):
    hc.hset_many("h", {"a": 1, "b": {"n": [True]}, "c": None})
    assert hc.hexists("h", "a") and hc.hexists("h", "c")
    assert not hc.hexists("h", "z") and not hc.hexists("missing", "a")
    assert hc.hkeys("h") == ["a", "b", "c"]
    assert hc.hvals("h") == [1, {"n": [True]}, None]
    assert hc.hlen("h") == 3
    hc.set("empty", {})
    assert hc.hkeys("empty") == [] and hc.hvals("empty") == [] and hc.hlen("empty") == 0


def test_hget_with_cls_reconstructs_like_get(hc):
    @dataclasses.dataclass
    class Addr:
        city: str

    hc.hset("u", "addr", {"city": "Lagos"})
    assert hc.hget("u", "addr", cls=Addr) == Addr("Lagos")
    assert hc.hget("u", "nope", cls=Addr) is None


def test_hget_default(hc):
    assert hc.hget("missing", "f", "dflt") == "dflt"
    hc.hset("h", "a", 1)
    assert hc.hget("h", "b", "dflt") == "dflt"
    hc.hset("h", "n", None)
    assert hc.hget("h", "n", "dflt") is None  # a stored null is a value, not a miss


def test_big_integers_round_trip_exactly(hc):
    big = 123456789012345678901234567890
    hc.hset("h", "big", big)
    assert hc.hget("h", "big") == big
    assert hc.hvals("h") == [big]


# -- misses never create, never resurrect -----------------------------------


def test_reads_on_missing_key_return_empty_and_create_nothing(hc, db_path):
    assert hc.hget("missing", "f") is None
    assert hc.hmget("missing", ["f"]) == [None]
    assert hc.hgetall("missing") == {}
    assert hc.hkeys("missing") == [] and hc.hvals("missing") == []
    assert hc.hlen("missing") == 0
    assert hc.hexists("missing", "f") is False
    assert hc.hdel("missing", "f") == 0
    assert raw(db_path, "missing") is None


def test_missing_key_is_created_with_no_expiry(hc):
    hc.hset("new", "f", 1)
    assert hc.ttl("new") == -1.0
    hc.hincrby("ctr", "n", 5)
    assert hc.ttl("ctr") == -1.0 and hc.hget("ctr", "n") == 5


def test_missing_key_created_with_optional_ttl(hc):
    hc.hset("new", "f", 1, ttl=100)
    t = hc.ttl("new")
    assert t is not None and 0 < t <= 100
    hc.hset("new", "g", 2, ttl=5)  # ttl is ignored for an existing key
    t2 = hc.ttl("new")
    assert t2 is not None and t2 > 5


def test_expired_key_is_not_resurrected(hc, db_path):
    hc.set("h", {"old": 1, "stale": 2}, ttl=0.01)
    time.sleep(0.05)
    assert hc.hget("h", "old") is None
    assert hc.hgetall("h") == {}
    assert hc.hexists("h", "old") is False
    assert hc.hlen("h") == 0
    assert hc.hdel("h", "old") == 0
    assert hc.hsetnx("h", "old", 9) is True  # treated as missing
    # A write to an expired key starts a fresh object: old fields are gone,
    # and the stale expiry is not carried over.
    assert hc.hgetall("h") == {"old": 9}
    assert hc.ttl("h") == -1.0


def test_hset_on_expired_key_starts_fresh(hc):
    hc.set("h", {"old": 1}, ttl=0.01)
    time.sleep(0.05)
    assert hc.hset("h", "new", 1) is True
    assert hc.get("h") == {"new": 1}


def test_hincrby_on_expired_key_starts_at_zero(hc):
    hc.set("h", {"n": 100}, ttl=0.01)
    time.sleep(0.05)
    assert hc.hincrby("h", "n", 1) == 1


# -- type guarding ------------------------------------------------------------


@pytest.mark.parametrize(
    "blob,vtype",
    [
        (b"\x00\x01raw", TYPE_BYTES),
        (b"hello", TYPE_STR),
        (b"42", TYPE_INT),
        (b"3.14", TYPE_FLOAT),
        (b"\x80\x04pickle", TYPE_PICKLE),
        (b"\xac\xedjava", TYPE_JAVA),
    ],
)
def test_hash_ops_reject_non_json_type_codes(hc, db_path, blob, vtype):
    insert_raw(db_path, "k", blob, vtype)
    for op in (
        lambda: hc.hset("k", "f", 1),
        lambda: hc.hset_many("k", {"f": 1}),
        lambda: hc.hsetnx("k", "f", 1),
        lambda: hc.hincrby("k", "f", 1),
        lambda: hc.hincrbyfloat("k", "f", 1.0),
        lambda: hc.hdel("k", "f"),
        lambda: hc.hget("k", "f"),
        lambda: hc.hmget("k", ["f"]),
        lambda: hc.hgetall("k"),
        lambda: hc.hexists("k", "f"),
        lambda: hc.hkeys("k"),
        lambda: hc.hvals("k"),
        lambda: hc.hlen("k"),
    ):
        with pytest.raises(SerializationError, match=f"value_type={vtype}"):
            op()
    row = raw(db_path, "k")
    assert row[0] == blob and row[1] == vtype  # untouched


@pytest.mark.parametrize("value,root", [([1, 2], "array"), (True, ""), ("not-an-object", "")])
def test_hash_ops_reject_non_object_json_roots(hc, db_path, value, root):
    if isinstance(value, str):
        insert_raw(db_path, "k", json.dumps(value).encode(), 4)  # a JSON string root
    else:
        hc.set("k", value)
    before = raw(db_path, "k")[0]
    with pytest.raises(SerializationError, match="not a JSON object"):
        hc.hset("k", "f", 1)
    with pytest.raises(SerializationError, match="not a JSON object"):
        hc.hget("k", "f")
    with pytest.raises(SerializationError, match="not a JSON object"):
        hc.hincrby("k", "f")
    assert raw(db_path, "k")[0] == before


def test_malformed_json_row_is_rejected_not_rewritten(hc, db_path):
    insert_raw(db_path, "k", b"{not json", 4)
    with pytest.raises(SerializationError, match="malformed JSON"):
        hc.hset("k", "f", 1)
    with pytest.raises(SerializationError, match="malformed JSON"):
        hc.hget("k", "f")
    assert raw(db_path, "k")[0] == b"{not json"


def test_unserializable_field_value_raises(hc):
    with pytest.raises(SerializationError):
        hc.hset("h", "f", b"bytes are not JSON")
    with pytest.raises(SerializationError):
        hc.hset("h", "f", float("nan"))
    with pytest.raises(TypeError):
        hc.hset("h", 1, "field names must be str")  # type: ignore[arg-type]


# -- field-name quoting -----------------------------------------------------


@pytest.mark.parametrize("field", WEIRD_FIELDS)
def test_awkward_field_names_address_exactly_one_top_level_key(hc, field):
    hc.set("h", {"a": {"b": "nested-decoy"}, "other": 0})
    assert hc.hset("h", field, "v") is True
    assert hc.hget("h", field) == "v"
    assert hc.hexists("h", field)
    assert hc.hmget("h", [field]) == ["v"]
    doc = hc.get("h")
    assert doc[field] == "v" and doc["a"] == {"b": "nested-decoy"} and doc["other"] == 0
    assert field in hc.hkeys("h")
    assert hc.hincrby("h", field + "#n", 2) == 2
    assert hc.hdel("h", field) == 1
    assert hc.hget("h", field) is None
    assert set(hc.get("h")) == {"a", "other", field + "#n"}


def test_all_awkward_field_names_together(hc):
    mapping = {f: i for i, f in enumerate(WEIRD_FIELDS)}
    hc.hset_many("h", mapping)
    assert hc.hgetall("h") == mapping
    assert hc.hkeys("h") == WEIRD_FIELDS
    assert hc.hvals("h") == list(range(len(WEIRD_FIELDS)))
    assert hc.hdel("h", *WEIRD_FIELDS) == len(WEIRD_FIELDS)
    assert hc.get("h") == {}


# -- TTL, size_bytes, LRU metadata -----------------------------------------


def test_field_writes_preserve_ttl(hc, db_path):
    hc.set("h", {"a": 1}, ttl=60)
    expires_before = raw(db_path, "h")[2]
    hc.hset("h", "b", 2)
    hc.hset_many("h", {"c": 3})
    hc.hsetnx("h", "d", 4)
    hc.hincrby("h", "n", 1)
    hc.hincrbyfloat("h", "x", 0.5)
    hc.hdel("h", "a")
    assert raw(db_path, "h")[2] == expires_before
    t = hc.ttl("h")
    assert t is not None and 0 < t <= 60
    time.sleep(0.05)
    t2 = hc.ttl("h")
    assert t2 is not None and t2 < t  # still counting down


def test_size_bytes_tracks_value_length(hc, db_path):
    ops = [
        lambda: hc.hset("h", "a", "x" * 100),
        lambda: hc.hset("h", "ü", "字" * 50),  # multi-byte: bytes, not characters
        lambda: hc.hset_many("h", {f"k{i}": i for i in range(30)}),
        lambda: hc.hincrby("h", "n", 7),
        lambda: hc.hincrbyfloat("h", "x", 1.25),
        lambda: hc.hdel("h", "a", "k3"),
        lambda: hc.hset("h", "a", "short"),
    ]
    for op in ops:
        op()
        value, _, _, size_bytes, length, _, _ = raw(db_path, "h")
        assert size_bytes == length == len(value)


def test_hash_writes_count_as_access(hc, db_path):
    hc.set("h", {"a": 1})
    _, _, _, _, _, la0, ac0 = raw(db_path, "h")
    time.sleep(0.01)
    hc.hset("h", "b", 2)
    _, _, _, _, _, la1, ac1 = raw(db_path, "h")
    assert la1 > la0 and ac1 == ac0 + 1
    hc.hset_many("h", {"c": 3, "d": 4, "e": 5})
    hc.hdel("h", "nope", "c", "d")  # several statements, still one access
    assert raw(db_path, "h")[6] == ac1 + 2


def test_hash_reads_do_not_write_synchronously(hc, db_path):
    hc.set("h", {"a": 1})
    before = raw(db_path, "h")
    hc.hget("h", "a")
    hc.hmget("h", ["a"])
    hc.hgetall("h")
    hc.hlen("h")
    assert raw(db_path, "h") == before  # metadata is buffered, not written
    hc._flush_lru_buffer()
    assert raw(db_path, "h")[6] > before[6]


def test_hits_and_misses_are_counted(hc):
    hc.hset("h", "a", 1)
    hc.hget("h", "a")
    hc.hget("missing", "a")
    stats = hc.stats()
    assert stats["hits"] == 1 and stats["misses"] == 1


def test_noeviction_capacity_applies_to_hash_creation(db_path, mode):
    c = LyteCache(db_path, sweep_interval=None, max_keys=1, eviction="noeviction")
    try:
        c.hset("a", "f", 1)
        c.hset("a", "g", 2)  # updating an existing key never trips the limit
        with pytest.raises(CacheFullError):
            c.hset("b", "f", 1)
        with pytest.raises(CacheFullError):
            c.hincrby("b", "n")
        assert not c.exists("b")
    finally:
        c.close()


# -- hincrby ------------------------------------------------------------------


def test_hincrby_basic_and_integerness_preserved(hc, db_path):
    assert hc.hincrby("h", "n") == 1
    assert hc.hincrby("h", "n", 4) == 5
    assert hc.hincrby("h", "n", -10) == -5
    assert isinstance(hc.hget("h", "n"), int)
    assert json.loads(raw(db_path, "h")[0]) == {"n": -5}
    assert b"-5.0" not in raw(db_path, "h")[0]


def test_hincrby_into_existing_document(hc):
    hc.set("acct", {"owner": "ada", "balance": 10})
    assert hc.hincrby("acct", "balance", 5) == 15
    assert hc.get("acct") == {"owner": "ada", "balance": 15}


@pytest.mark.parametrize("bad", ["5", True, False, None, {"a": 1}, [1], 1.5])
def test_hincrby_rejects_non_integer_fields(hc, bad):
    hc.hset("h", "f", bad)
    with pytest.raises(TypeError):
        hc.hincrby("h", "f", 1)
    assert hc.hget("h", "f") == bad


def test_hincrby_int64_boundary(hc, db_path):
    hc.hset("h", "n", 2**63 - 2)
    assert hc.hincrby("h", "n") == 2**63 - 1
    before = raw(db_path, "h")[0]
    with pytest.raises(OverflowError):
        hc.hincrby("h", "n")
    assert raw(db_path, "h")[0] == before  # nothing written
    hc.hset("m", "n", -(2**63) + 1)
    assert hc.hincrby("m", "n", -1) == -(2**63)
    with pytest.raises(OverflowError):
        hc.hincrby("m", "n", -1)
    with pytest.raises(OverflowError):
        hc.hincrby("h", "x", 2**63)
    with pytest.raises(TypeError):
        hc.hincrby("h", "x", 1.5)  # type: ignore[arg-type]


def test_hincrbyfloat(hc):
    assert hc.hincrbyfloat("h", "x", 1.5) == 1.5
    assert hc.hincrbyfloat("h", "x", 0.1) == pytest.approx(1.6)
    hc.hset("h", "i", 2)
    assert hc.hincrbyfloat("h", "i", 0.5) == 2.5
    assert hc.hincrbyfloat("h", "y", 2) == 2.0 and isinstance(hc.hget("h", "y"), float)
    hc.hset("h", "s", "str")
    with pytest.raises(TypeError):
        hc.hincrbyfloat("h", "s", 1.0)
    with pytest.raises(ValueError):
        hc.hincrbyfloat("h", "x", float("inf"))
    hc.hset("h", "huge", 1.7e308)
    with pytest.raises(OverflowError):
        hc.hincrbyfloat("h", "huge", 1.7e308)


def test_hincrbyfloat_full_precision(hc):
    hc.hincrbyfloat("h", "x", 0.1)
    assert hc.hincrbyfloat("h", "x", 0.2) == 0.1 + 0.2


# -- cross-language file contract --------------------------------------------


def test_hash_writes_store_canonical_compact_json(hc, db_path):
    hc.hset("h", "name", "Adé")
    hc.hset("h", "n", 1)
    value, vtype, *_ = raw(db_path, "h")
    assert vtype == 4
    assert value == '{"name":"Adé","n":1}'.encode()


def test_reads_json_written_by_other_encoders(hc, db_path):
    # Another implementation (or a human) may format JSON differently.
    insert_raw(db_path, "h", b'{ "a" : 1,\n  "b.c": [true, null], "u": "\\u00e9" }', 4)
    assert hc.hget("h", "a") == 1
    assert hc.hget("h", "b.c") == [True, None]
    assert hc.hget("h", "u") == "é"
    assert hc.hincrby("h", "a", 1) == 2
    assert hc.get("h") == {"a": 2, "b.c": [True, None], "u": "é"}


# -- the reason hashes exist: no lost updates across processes ----------------


def test_concurrent_hset_on_different_fields_loses_nothing(db_path, mode):
    c = LyteCache(db_path, sweep_interval=None)
    c.set("shared", {})
    c.close()

    n_procs, per_proc = 6, 150
    procs = [
        multiprocessing.Process(
            target=hset_field_worker, args=(str(db_path), "shared", f"field{i}", per_proc)
        )
        for i in range(n_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
        assert p.exitcode == 0

    c = LyteCache(db_path, sweep_interval=None)
    try:
        assert c.hgetall("shared") == {f"field{i}": per_proc - 1 for i in range(n_procs)}
    finally:
        c.close()


def test_concurrent_hincrby_on_one_field_is_exact(db_path, mode):
    c = LyteCache(db_path, sweep_interval=None)
    c.set("shared", {"other": "untouched"})
    c.close()

    n_procs, per_proc = 6, 200
    procs = [
        multiprocessing.Process(target=hincrby_worker, args=(str(db_path), "shared", "n", per_proc))
        for _ in range(n_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
        assert p.exitcode == 0

    c = LyteCache(db_path, sweep_interval=None)
    try:
        assert c.hgetall("shared") == {"other": "untouched", "n": n_procs * per_proc}
    finally:
        c.close()


def test_read_modify_write_in_app_code_does_lose_updates(db_path):
    """The control: the naive get()/set() pattern hashes replace really does race."""
    c = LyteCache(db_path, sweep_interval=None)
    c.set("doc", {"a": 0, "b": 0})
    doc_a = c.get("doc")  # worker A reads
    doc_b = c.get("doc")  # worker B reads
    doc_a["a"] = 1
    c.set("doc", doc_a)  # A writes
    doc_b["b"] = 1
    c.set("doc", doc_b)  # B writes -- clobbers A
    assert c.get("doc") == {"a": 0, "b": 1}
    c.hset("doc", "a", 1)
    c.hset("doc", "b", 2)
    assert c.get("doc") == {"a": 1, "b": 2}
    c.close()


def test_stats_reports_hash_mode(hc, mode):
    assert hc.stats()["hash_mode"] == mode
