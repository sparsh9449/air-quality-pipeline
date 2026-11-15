from datetime import UTC, datetime

from ingestion.poller import prune_partitions

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


def test_prune_deletes_only_old_partitions(tmp_path):
    parquet_dir = tmp_path / "parquet"
    old = parquet_dir / "dt=2026-11-01"      # older than 7 days -> deleted
    recent = parquet_dir / "dt=2026-11-14"   # within 7 days -> kept
    other_dir = parquet_dir / "logs"         # not a dt= dir -> kept
    old.mkdir(parents=True)
    recent.mkdir(parents=True)
    other_dir.mkdir(parents=True)
    (old / "measurements.parquet").write_text("x")
    (parquet_dir / "dt=2026-11-01.txt").write_text("not a dir")  # stray file -> kept
    (parquet_dir / "README.md").write_text("keep me")

    removed = prune_partitions(str(parquet_dir), retention_days=7, now=NOW)

    assert removed == ["dt=2026-11-01"]
    assert not old.exists()
    assert recent.exists()
    assert other_dir.exists()
    assert (parquet_dir / "dt=2026-11-01.txt").exists()
    assert (parquet_dir / "README.md").exists()


def test_prune_missing_dir_is_noop(tmp_path):
    assert prune_partitions(str(tmp_path / "nope"), retention_days=7, now=NOW) == []
