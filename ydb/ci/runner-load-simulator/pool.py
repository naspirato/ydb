"""Runner pool capacity model from .github/config/runner_capacity.yml."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

RESOURCES = ("vcpu", "ram_gb", "nrd_ssd_gb")


@dataclass
class PoolConfig:
    quotas: dict[str, float]
    reserved: dict[str, float]
    headroom_fraction: float
    footprints: dict[str, dict[str, int]]
    default_footprint: dict[str, int]
    saturated_min_shards: int = 1

    @classmethod
    def load(cls, path: Path) -> PoolConfig:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return cls(
            quotas={k: float(v) for k, v in raw["quotas"].items()},
            reserved={k: float(v) for k, v in raw.get("reserved", {}).items()},
            headroom_fraction=float(raw.get("headroom_fraction", 1.0)),
            footprints=raw["footprints"],
            default_footprint=raw["default_footprint"],
            saturated_min_shards=int(raw.get("saturated_min_shards", 1)),
        )

    def footprint(self, preset_label: str) -> dict[str, int]:
        return self.footprints.get(preset_label) or self.default_footprint

    def max_instances_budget(self) -> float:
        return (
            self.quotas["instances"] - self.reserved.get("instances", 0)
        ) * self.headroom_fraction

    def max_new_runners(self, active_by_label: Counter, preset_label: str) -> int:
        used = {res: 0.0 for res in RESOURCES}
        used_instances = 0
        for label, count in active_by_label.items():
            fp = self.footprint(label)
            for res in RESOURCES:
                used[res] += fp[res] * count
            used_instances += count

        free_instances = self.max_instances_budget() - used_instances
        fp = self.footprint(preset_label)
        fits = [free_instances]
        for res in RESOURCES:
            budget = (
                self.quotas[res] - self.reserved.get(res, 0)
            ) * self.headroom_fraction
            fits.append((budget - used[res]) / fp[res])
        return max(int(math.floor(min(fits))), 0)


@dataclass
class ActiveJob:
    end_time: float
    preset: str
    job_key: str


@dataclass
class PoolSimulator:
    config: PoolConfig
    active: list[ActiveJob] = field(default_factory=list)
    queue_wait_sec: float = 0.0
    queued_events: int = 0
    peak_instances: int = 0
    instance_seconds: float = 0.0
    saturated_seconds: float = 0.0
    last_time: float = 0.0

    def _active_by_label(self) -> Counter:
        counts: Counter = Counter()
        for job in self.active:
            counts[job.preset] += 1
        return counts

    def _expire(self, now: float) -> None:
        if now <= self.last_time:
            return
        alive = [job for job in self.active if job.end_time > self.last_time]
        dt = now - self.last_time
        self.instance_seconds += len(alive) * dt
        budget = self.config.max_instances_budget()
        if len(alive) >= budget * 0.9:
            self.saturated_seconds += dt
        self.active = [job for job in self.active if job.end_time > now]
        self.last_time = now

    def can_allocate(self, preset: str, count: int = 1) -> bool:
        return self.config.max_new_runners(self._active_by_label(), preset) >= count

    def capacity_cap(self, preset: str) -> int:
        max_new = self.config.max_new_runners(self._active_by_label(), preset)
        floor = self.config.saturated_min_shards
        return max(max_new, floor)

    def allocate(self, now: float, duration_sec: float, preset: str, job_key: str) -> float:
        """Allocate one runner; return wait time before the job actually starts."""
        self._expire(now)
        wait = 0.0
        while not self.can_allocate(preset):
            if not self.active:
                break
            next_free = min(job.end_time for job in self.active)
            wait += next_free - now
            self._expire(next_free)
            now = next_free
            self.queued_events += 1
        end = now + duration_sec
        self.active.append(ActiveJob(end_time=end, preset=preset, job_key=job_key))
        self.queue_wait_sec += wait
        self.peak_instances = max(self.peak_instances, len(self.active))
        self.last_time = now
        return wait

    def allocate_parallel(
        self, now: float, duration_sec: float, preset: str, count: int, job_key: str
    ) -> float:
        """Allocate `count` runners at once (e.g. shard matrix)."""
        self._expire(now)
        wait = 0.0
        while self.config.max_new_runners(self._active_by_label(), preset) < count:
            if not self.active:
                break
            next_free = min(job.end_time for job in self.active)
            wait += next_free - now
            self._expire(next_free)
            now = next_free
            self.queued_events += 1
        for idx in range(count):
            end = now + duration_sec
            self.active.append(
                ActiveJob(end_time=end, preset=preset, job_key=f"{job_key}:{idx}")
            )
        self.queue_wait_sec += wait
        self.peak_instances = max(self.peak_instances, len(self.active))
        self.last_time = now
        return wait

    def finalize(self, end_time: float) -> None:
        self._expire(end_time)
