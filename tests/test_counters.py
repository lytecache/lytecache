from __future__ import annotations

import time

import pytest


def test_incr_starts_at_zero(cache):
    assert cache.incr("counter") == 1
    assert cache.incr("counter") == 2


def test_incr_with_amount(cache):
    assert cache.incr("counter", 5) == 5
    assert cache.incr("counter", 3) == 8


def test_decr(cache):
    cache.set("counter", 10)
    assert cache.decr("counter") == 9
    assert cache.decr("counter", 4) == 5


def test_decr_starts_at_zero(cache):
    assert cache.decr("counter") == -1


def test_incr_float(cache):
    assert cache.incr_float("f", 1.5) == 1.5
    assert cache.incr_float("f", 2.25) == 3.75


def test_incr_float_on_int_value(cache):
    cache.set("k", 5)
    assert cache.incr_float("k", 0.5) == 5.5


def test_incr_on_non_numeric_raises_type_error(cache):
    cache.set("k", "not a number")
    with pytest.raises(TypeError):
        cache.incr("k")


def test_incr_on_json_value_raises_type_error(cache):
    cache.set("k", {"a": 1})
    with pytest.raises(TypeError):
        cache.incr("k")


def test_incr_on_float_raises_type_error(cache):
    cache.set("k", 3.14)
    with pytest.raises(TypeError):
        cache.incr("k")


def test_incr_float_on_non_numeric_raises_type_error(cache):
    cache.set("k", "nope")
    with pytest.raises(TypeError):
        cache.incr_float("k", 1.0)


def test_incr_on_expired_key_resets(cache):
    cache.set("k", 100, ttl=0.01)
    time.sleep(0.05)
    assert cache.incr("k") == 1


def test_incr_float_on_expired_key_resets(cache):
    cache.set("k", 100.0, ttl=0.01)
    time.sleep(0.05)
    assert cache.incr_float("k", 2.5) == 2.5


def test_incr_does_not_disturb_existing_ttl(cache):
    cache.set("k", 5, ttl=100)
    cache.incr("k")
    ttl = cache.ttl("k")
    assert ttl is not None and 0 < ttl <= 100
