from streaming.demos import demo_crash, demo_poison, demo_replay, demo_schema


def test_replay_is_idempotent() -> None:
    demo_replay()


def test_schema_change_lands_in_extra_then_promotes() -> None:
    demo_schema()


def test_crash_before_offset_commit_does_not_duplicate() -> None:
    demo_crash()


def test_poison_message_is_isolated() -> None:
    demo_poison()
