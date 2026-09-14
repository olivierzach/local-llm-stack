import base64
from pathlib import Path
import runpy
import struct
import zlib

import pytest

MOD = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'scripts/probe-glm53-features.py'))


class Response:
    def __init__(self, lines):
        self.lines = lines

    def iter_lines(self):
        return iter(self.lines)


def test_stream_keeps_reasoning_separate_and_requires_done():
    lines = [b'data: {"choices":[{"delta":{"reasoning_content":"work"}}]}',
             b'data: {"choices":[{"delta":{"content":"49"},"finish_reason":"stop"}]}',
             b'data: {"choices":[],"usage":{"completion_tokens":7}}']
    result = MOD['collect'](Response(lines + [b'data: [DONE]']), True)
    assert result == dict(content='49', reasoning='work', finish_reason='stop', usage={'completion_tokens': 7})
    with pytest.raises(RuntimeError, match='without DONE'):
        MOD['collect'](Response(lines), True)


def test_image_fixture_reverses_pixels_without_text_hints():
    for reverse, left, right in [(False, b'\xff\x00\x00', b'\x00\x00\xff'),
                                 (True, b'\x00\x00\xff', b'\xff\x00\x00')]:
        data = base64.b64decode(MOD['image_url'](reverse).split(',', 1)[1])
        assert data[:8] == b'\x89PNG\r\n\x1a\n'
        chunks = {}
        cursor = 8
        while cursor < len(data):
            size = struct.unpack('>I', data[cursor:cursor + 4])[0]
            kind = data[cursor + 4:cursor + 8]
            payload = data[cursor + 8:cursor + 8 + size]
            assert zlib.crc32(kind + payload) == struct.unpack('>I', data[cursor + 8 + size:cursor + 12 + size])[0]
            chunks[kind] = payload
            cursor += size + 12
        assert set(chunks) == {b'IHDR', b'IDAT', b'IEND'}
        assert zlib.decompress(chunks[b'IDAT']) == (b'\x00' + left * 128 + right * 128) * 128
