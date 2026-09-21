import json

from antiphon.rawlog import RawLog


def test_log_appends_one_json_line_per_call(tmp_path):
    log = RawLog(tmp_path / "raw.jsonl")
    log.log("out", "codex", '{"jsonrpc":"2.0","id":1,"method":"initialize"}')
    log.log("in", "codex", {"id": 1, "result": {"userAgent": "x"}})

    lines = (tmp_path / "raw.jsonl").read_text().splitlines()
    first, second = (json.loads(line) for line in lines)
    assert first["dir"] == "out"
    assert first["boundary"] == "codex"
    assert first["data"] == '{"jsonrpc":"2.0","id":1,"method":"initialize"}'
    assert isinstance(first["t"], float)
    assert second == {"t": second["t"], "dir": "in", "boundary": "codex", "data": {"id": 1, "result": {"userAgent": "x"}}}


def test_unserializable_data_is_logged_by_its_repr(tmp_path):
    log = RawLog(tmp_path / "raw.jsonl")
    log.log("in", "codex", b"\x81\x02{}")
    assert json.loads((tmp_path / "raw.jsonl").read_text())["data"] == "b'\\x81\\x02{}'"


def test_rotates_past_the_size_limit_and_keeps_three_old_files(tmp_path):
    path = tmp_path / "raw.jsonl"
    log = RawLog(path, max_bytes=200, keep=3)
    for i in range(40):
        log.log("in", "codex", "x" * 50 + str(i))

    assert path.exists()
    assert (tmp_path / "raw.jsonl.1").exists()
    assert (tmp_path / "raw.jsonl.3").exists()
    assert not (tmp_path / "raw.jsonl.4").exists()
    # every surviving file is intact JSON lines, and the newest line is in the live file
    for candidate in tmp_path.iterdir():
        for line in candidate.read_text().splitlines():
            json.loads(line)
    assert json.loads(path.read_text().splitlines()[-1])["data"].endswith("39")
