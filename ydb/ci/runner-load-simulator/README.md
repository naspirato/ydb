# Self-hosted runner pool simulator

Discrete-event replay of the shared auto-provisioned runner pool over the last
14 days of GitHub Actions history. Compares **baseline** (`pr_check.yml`
monolith) vs **`pr_check_parallel.yml`** from [PR #43351](https://github.com/ydb-platform/ydb/pull/43351).

## What it models

- **Historical load**: all `auto-provisioned` + `build-preset-*` jobs from 18
  workflows (PR-check, postcommits, nightly, regressions, analytics, …).
- **Pool limits**: `vendor/runner_capacity.yml` (5400 vCPU / 23 TB RAM /
  110 VMs, minus static reserve, 90% headroom → **~79 concurrent runners**).
- **PR-check parallel path**:
  - change-volume classifier (heavy paths / ≥500 files → sharded);
  - `choose_shard_count.py` tiers + peak-hour cap (09–16 UTC → max 4);
  - `estimate_runner_capacity.py` cap from live quota demand;
  - prepare + N parallel shards (N=1 falls back to monolith timing).

## Usage

```bash
# 1. Collect 14 days of job intervals (needs `gh auth login`)
python3 ydb/ci/runner-load-simulator/collect_jobs.py --days 14

# 2. Run simulation
./ydb/ci/runner-load-simulator/run_simulation.sh
# or
python3 ydb/ci/runner-load-simulator/simulate.py
```

Output: `data/simulation_report.json`

## Latest run (2026-06-13, 14 days, 9336 jobs)

| Metric | Baseline | PR-check parallel | Δ |
|--------|----------|-------------------|---|
| PR-check rwdi median wall time | 12.7 min | 11.5 min | **−1.2 min** |
| PR-check rwdi p90 wall time | 197.9 min | 130.9 min | **−67 min** |
| Heavy PRs (monolith ≥60 min), median | 170.1 min | 98.7 min | **−71 min (−42%)** |
| Heavy PRs p90 | 210.4 min | 165.0 min | **−45 min** |
| Total pool runner-hours (2 weeks) | 11 846 h | 12 571 h | +6.1% |
| Peak concurrent runners | 65 | 66 | +1 |
| Queue wait (all jobs) | 276.9 h | 269.3 h | −7.6 h |
| Mean pool utilization | 44.6% | 47.3% | +2.7 pp |

Classifier split: **139** light (single) + **278** heavy path; of the latter
**156** adaptive N=1 (no shard overhead) → **122** runs with real N>1 sharding.

### Interpretation

**Выигрыш есть**, но он **сильно неровный**:

1. **Тяжёлые PR-check** (≥60 min на монолите) — главный выигрыш: медиана
   **~71 мин быстрее** (−42%). Это как раз целевой сегмент шардинга.
2. **Хвост (p90)** всех PR-check: **−67 min** — заметное ускорение без
   раздувания пика (+1 раннер).
3. **Лёгкие PR** — без деградации (медиана −1.2 min): classifier + N=1
   fallback не добавляют prepare-overhead.
4. **Цена для пула**: +6% runner-hours за 2 недели и +2.7 pp средней
   утилизации — умеренно; пик 65→66, очередь чуть **лучше** (−7.6 h wait).
5. Пул **не упирается в 79 VM** (пик 66, saturation 0% по instance cap) —
   узкое место скорее **vCPU/RAM quota** на отдельные пресеты; симулятор это
   учитывает через footprints из `runner_capacity.yml`.

### Caveats

- Длительности shard/prepare — модель (prepare ≈18% монолита, min 15 min;
  shard phase = test_time / N × 1.08). Реальные цифры нужно сверить с
  `run_and_debug_tests` на тяжёлом PR.
- Классификатор ходит в GitHub API за списком файлов PR (кэшируется в прогоне).
- Postcommit / nightly / regression нагрузка **одинакова** в обоих сценариях;
  меняется только relwithdebinfo PR-check.
