from __future__ import annotations

from streamplat import wire


def test_frame_round_trip():
    raw = wire.frame(42, b"body")
    assert raw[:5] == b"\x00\x00\x00\x00\x2a"
    assert wire.parse_header(raw) == 42


def test_not_framed():
    assert wire.parse_header(None) is None
    assert wire.parse_header(b"\x00\x01") is None
    assert wire.parse_header(b"\x01\x00\x00\x00\x01rest") is None
