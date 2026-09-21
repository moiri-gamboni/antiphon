from fixtures.scrub import Scrubber, scrub


def test_home_directories_become_tilde():
    line = '{"codexHome":"/home/alice/.codex","path":"/Users/bob/.codex/sessions/x.jsonl"}'
    assert scrub(line) == '{"codexHome":"~/.codex","path":"~/.codex/sessions/x.jsonl"}'


def test_machine_id_in_pid_domain_becomes_zeros():
    line = '"pidDomain": "linux:3f9c2a7b1d4e5f60718293a4b5c6d7e8:pid:[4026531836]"'
    assert scrub(line) == '"pidDomain": "linux:00000000000000000000000000000000:pid:[4026531836]"'


def test_pids_become_stable_small_integers():
    lines = [
        '{"pid": 252795, "messagingSocketPath": "/run/user/1000/cc-socks/252795.sock"}',
        '{"peer_pid": 141123, "from": "uds:/run/user/1000/cc-socks/141123.sock"}',
        '{"pid": 252795}',
        '{"type":"commandExecution","processId":"75767","status":"completed"}',
    ]
    scrubber = Scrubber()
    assert [scrubber.line(l) for l in lines] == [
        '{"pid": 1001, "messagingSocketPath": "/run/user/1000/cc-socks/1001.sock"}',
        '{"peer_pid": 1002, "from": "uds:/run/user/1000/cc-socks/1002.sock"}',
        '{"pid": 1001}',
        '{"type":"commandExecution","processId":"1003","status":"completed"}',
    ]


def test_session_titles_become_claude_main():
    line = 'from=\\"uds:/run/user/1000/cc-socks/7.sock\\" from-name=\\"Notes on the bridge design\\" from-mode=\\"prompting\\"'
    assert scrub(line) == 'from=\\"uds:/run/user/1000/cc-socks/1001.sock\\" from-name=\\"claude-main\\" from-mode=\\"prompting\\"'


def test_peer_names_without_spaces_are_kept():
    line = 'from-name=\\"codex-stub\\"'
    assert scrub(line) == line


def test_daemon_identity_fields_become_placeholders():
    line = '{"status":"disabled","serverName":"mybox","installationId":"ba66fe9c-2b12-463e-b243-7d7605c32435","environmentId":null}'
    assert scrub(line) == (
        '{"status":"disabled","serverName":"codex-host",'
        '"installationId":"00000000-0000-0000-0000-000000000000","environmentId":null}'
    )


def test_thread_and_turn_ids_are_untouched():
    line = '{"threadId":"01a0c399-780c-73f3-9209-e09112a796a0","turnId":"01a0c399-785f-7953-8ddf-540dd55ad0f7"}'
    assert scrub(line) == line
