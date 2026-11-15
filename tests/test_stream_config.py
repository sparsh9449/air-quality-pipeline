import streaming.config as cfg


def test_topic_specs_defaults():
    specs = {s.name: s for s in cfg.TOPIC_SPECS}
    assert specs["sensor.readings"].partitions == 3
    assert specs["sensor.readings"].config["retention.ms"] == "86400000"
    assert specs["sensor.readings"].config["retention.bytes"] == "536870912"
    assert specs["sensor.readings.dlq"].partitions == 1
    assert specs["sensor.readings.dlq"].config["retention.ms"] == "604800000"


def test_broker_default_and_env_override(monkeypatch):
    monkeypatch.delenv("REDPANDA_BROKER", raising=False)
    assert cfg.broker() == "localhost:9092"
    monkeypatch.setenv("REDPANDA_BROKER", "example:1234")
    assert cfg.broker() == "example:1234"


def test_group_names():
    assert cfg.ARCHIVER_GROUP == "archiver"
    assert cfg.STATS_GROUP == "stats"
