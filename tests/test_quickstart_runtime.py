from concurrent.futures import ThreadPoolExecutor
import asyncio
import json
import threading

import pytest

from scripts.quickstart_runtime import SerialQdrant, run


def test_local_qdrant_is_created_and_used_on_one_thread(tmp_path):
    client = SerialQdrant(tmp_path / "vectors")
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: client.get_collections(), range(8)))
        assert len(results) == 8
        assert client.thread_id != threading.get_ident()
    finally:
        client.close()


def test_quickstart_rejects_paths_outside_tryout(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"storage": {"sqlite_path": "/tmp/other.db"}}))
    monkeypatch.setenv("AMR_TRYOUT_DIR", str(tmp_path))
    monkeypatch.setenv("AMR_TRYOUT_RUNTIME", str(tmp_path / "run"))
    monkeypatch.setenv("AMR_TRYOUT_MODEL", "unused")
    with pytest.raises(RuntimeError, match="路径已改变"):
        asyncio.run(run())
