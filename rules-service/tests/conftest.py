import json
from pathlib import Path

import pytest

from paystream_rules.publisher import MemoryPublisher
from paystream_rules.store import RuleStore

LIBRARY = Path(__file__).resolve().parents[2] / "rules" / "library"


@pytest.fixture
def published():
    return MemoryPublisher()


@pytest.fixture
def store(tmp_path, published):
    return RuleStore(f"sqlite:///{tmp_path}/rules.db", published)


@pytest.fixture
def pin_rule():
    return json.loads((LIBRARY / "failed-pin-burst.json").read_text())
