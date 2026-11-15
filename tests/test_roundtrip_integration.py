import pytest

pytestmark = pytest.mark.integration


def test_roundtrip_against_broker():
    # Imported here so collection never needs confluent_kafka / a broker.
    from scripts.roundtrip import main

    assert main() == 0
