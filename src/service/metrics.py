"""进程内轻量指标：计数器 + 耗时观测 + 仪表，经 admin UDS status.metrics 暴露。

刻意不引入 Prometheus：单进程守护进程，snapshot() 足够 amr doctor 与运维排查使用
（docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md D5）。
"""
import threading
import time
from collections import defaultdict, deque
from typing import Dict

_OBS_WINDOW = 1024


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._observations: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_OBS_WINDOW)
        )
        self._gauges: Dict[str, float] = {}

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] += n

    def observe(self, name: str, value_ms: float) -> None:
        with self._lock:
            self._observations[name].append(float(value_ms))

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = float(value)

    def snapshot(self) -> Dict:
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            summary: Dict[str, Dict] = {}
            for name, window in self._observations.items():
                values = sorted(window)
                if not values:
                    continue
                n = len(values)

                def pct(p: float) -> float:
                    idx = min(n - 1, int(round(p * (n - 1))))
                    return values[idx]

                summary[name] = {
                    "count": n,
                    "avg_ms": round(sum(values) / n, 2),
                    "p50_ms": round(pct(0.5), 2),
                    "p95_ms": round(pct(0.95), 2),
                    "max_ms": round(values[-1], 2),
                }
            return {
                "counters": counters,
                "gauges": gauges,
                "summary": summary,
                "ts": int(time.time()),
            }


METRICS = Metrics()
