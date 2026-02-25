# Mute Optimizer v3 ML — Требования и аудит

## 1. Цель системы

Автоматически подобрать **оптимальные пороги** для мьюта/размьюта тестов, минимизируя:
- **Время реакции на мьют** — сколько дней сломанный тест остаётся незамьюченным (мешает разработчикам)
- **Время реакции на размьют** — сколько дней починенный тест остаётся замьюченным (скрывает регрессии)
- **Волатильность** — частота переключений mute↔unmute на тест (шум, нестабильность)

Целевая функция: `score = w_vol * volatility + w_mute * avg_time_to_mute + w_unmute * avg_time_to_unmute` (чем меньше — тем лучше).

## 2. Данные

### 2.1 Источник: `test_runs_column` (таблица `test_results` в YDB)

| Поле | Описание |
|------|----------|
| `full_name` | `suite_folder/test_name` |
| `branch` | Ветка (main, stable-25-4-1, ...) |
| `date_window` | Дата запуска (day precision) |
| `job_type` | `'wf'` (Nightly/Regression/Postcommit) или `'pr'` (PR-check) |
| `pass_count`, `fail_count`, `mute_count`, `skip_count` | Агрегированные счётчики за (test, branch, date, job_name) |

**Фильтры:**
- `build_type = 'relwithdebinfo'`
- `branch = '{target_branch}'`
- `run_timestamp >= now() - {days}d`
- `job_name IN (WF_JOB_NAMES)` OR `job_name = 'PR-check'`
- `test_name NOT IN ('unittest', 'py3test', 'gtest')` — исключаем мета-тесты

### 2.2 Источник: PR merged data (таблица `test_results` + `pull_requests`)

Полные результаты (pass+fail+mute+skip) из **последнего PR-check запуска** для каждого **merged PR**.
Фильтр по `pr_target_branch`: только PR, у которых `base_ref_name = '{branch}'` (как в pr_with_test_failures.sql).

**Логика:**
1. Взять все merged PR из `pull_requests` (dedupe по `pr_number`)
2. Из `test_results` взять PR-check записи с парсингом `pull` → `pr_num`
3. Для каждого PR взять последний `job_id` (по `run_timestamp`)
4. JOIN с merged PR → получить тесты только из merged PR
5. GROUP BY (full_name, branch, date) → pass/fail/mute/skip counts

### 2.3 Локальный кэш

JSON файлы в `mute_optimizer_cache/`. Параметры кэша: `(branch, build_type, days)`. Используются при повторных запусках для экономии времени.

## 3. Пространство поиска (Optuna)

### 3.1 PR-данные

| Параметр | Тип | Диапазон | Описание |
|----------|-----|----------|----------|
| `use_pr` | categorical | `[True, False]` | Использовать ли PR-check данные |
| `pr_source` | categorical | `['merged_only', 'full']` | Источник PR: только merged или все |
| `pr_coefficient` | float | `[0.1, 1.5]` | Вес PR запусков. 1.0 = наравне с WF |

### 3.2 Тип окна

| Параметр | Тип | Диапазон | Описание |
|----------|-----|----------|----------|
| `window_type` | categorical | `['days', 'runs', 'hybrid']` | Как определять окно наблюдения |

### 3.3 Пороги

| Параметр | Тип | Диапазон | Контекст | Описание |
|----------|-----|----------|----------|----------|
| `mute_fail_threshold` | int | `[1, 4]` | все | Мин. кол-во failures для мьюта |
| `mute_days` | int | `[3, 8]` | days | Окно наблюдения для мьюта |
| `unmute_days` | int | `[5, 18]` | days | Окно наблюдения для размьюта |
| `unmute_min_runs` | int | `[3, 22]` | days | Мин. запусков для размьюта |
| `mute_last_runs` | int | `[5, 50]` | runs/hybrid | Последние N запусков для мьюта |
| `unmute_last_runs` | int | `[5, 50]` | runs/hybrid | Последние N запусков для размьюта |
| `mute_days` | int | `[3, 14]` | hybrid | Max дней для hybrid окна (mute) |
| `unmute_days` | int | `[5, 21]` | hybrid | Max дней для hybrid окна (unmute) |

## 4. Симуляция

### 4.1 Логика mute решения

**Для window_type='days':**
- Агрегировать test results за последние `mute_days` дней
- Если `fail_count >= mute_fail_threshold` → мьютить
- (Доп. порог: если `total_runs <= low_runs_bound`, использовать `mute_fail_threshold_low_runs`)

**Для window_type='runs':**
- Агрегировать последние `mute_last_runs` запусков (по total_runs per day, идя назад по датам)
- Если `fail_count >= mute_fail_threshold` → мьютить

**Для window_type='hybrid':**
- Агрегировать последние `mute_last_runs` запусков, но только в пределах `mute_days` дней
- Если `fail_count >= mute_fail_threshold` → мьютить

### 4.2 Логика unmute решения

**Для window_type='days':**
- Агрегировать за `unmute_days` дней
- Если `total_runs >= unmute_min_runs AND total_fails == 0` → размьютить

**Для window_type='runs' и 'hybrid':**
- Агрегировать последние `unmute_last_runs` запусков (или в пределах `unmute_days` для hybrid)
- Если `total_runs > 0 AND total_fails == 0` → размьютить

### 4.3 Update interval

Решения принимаются каждый `update_interval_days` день (фиксирован = 1 день).

### 4.4 Порядок проверок

На каждый день: сначала проверить unmute, потом mute. Это важно: если тест мьючен, сначала проверяем можно ли размьютить.

## 5. Метрики

| Метрика | Формула | Что значит |
|---------|---------|------------|
| `volatility` | `total_transitions / n_tests` | Среднее число переключений mute↔unmute на тест |
| `avg_time_to_mute` | Среднее (дни от первого fail до mute решения) | Скорость реакции на поломку |
| `avg_time_to_unmute` | Среднее (дни от прекращения fails до unmute решения) | Скорость реакции на починку |
| `n_mute_transitions` | Кол-во переходов unmuted→muted | |
| `n_unmute_transitions` | Кол-во переходов muted→unmuted | |

### 5.1 Ограничение

Hard constraint: если `volatility > 0.10`, добавляется штраф `50 * (volatility - 0.10)` к score. Это предотвращает слишком агрессивные конфигурации.

## 6. Сравнение с production

Production config (из `create_new_muted_ya.py`):
- `MUTE_DAYS=4`, `fail >= 3` (если runs > 10), `fail >= 2` (если runs <= 10)
- `UNMUTE_DAYS=7`, `total_runs >= 4` и `total_fails == 0`
- `window_type='days'`, только WF данные

## 7. Статистическая значимость

Bootstrap resampling (100 итераций): ресемплируем тесты с возвратом, пересчитываем score для best и production, считаем 95% CI для разницы. Если весь CI < 0 → improvement значимый.

## 8. Выходные артефакты

| Файл | Описание |
|------|----------|
| `mute_optimizer_v3_ml_report.html` | HTML отчёт с графиками, метриками, diff |
| `mute_optimizer_output/to_mute_prod_{branch}.txt` | Список тестов для мьюта по production правилам |
| `mute_optimizer_output/to_mute_best_{branch}.txt` | Список тестов для мьюта по оптимизированным правилам |

---

## АУДИТ: Найденные проблемы

### BUG-1: `PrecomputedAggregates.pr_merged` — неверная типизация в dataclass

**Файл:** `mute_optimizer_v3.py`, строка 344

```python
pr_merged: Dict[Tuple[str, str, int], int]  # ← устаревший тип
```

**Факт:** После рефакторинга `pr_merged` хранит `Dict[..., Dict]` (с ключами `pr_pass/pr_fail/pr_mute/pr_skip`), но аннотация в dataclass осталась `int`. Не влияет на runtime (Python не проверяет типы), но вводит в заблуждение.

**Исправление:** Обновить аннотацию на `Dict[Tuple[str, str, int], Dict]`.

### BUG-2: PR merged query — парсинг `pull` теряет большинство данных

**Файл:** `mute_optimizer_v3.py`, строки 167-174

Фильтр `pull IS NOT NULL AND pull != '' AND String::Contains(CAST(pull AS UTF8), 'PR_')` может отрезать записи, если поле `pull` пустое или имеет другой формат. При 3.4M PR строк в основных данных, PR merged query возвращает данные, соответствующие лишь ~37 (test, date) комбинациям. Нужно проверить формат поля `pull` в production.

### BUG-3: `date_window` может быть `None` для тестов из PR-merged, у которых нет WF-записей

**Файл:** `mute_optimizer_v3.py`, строка 417

```python
'date_window': pre.date_windows.get(k),  # может быть None
```

Если тест присутствует только в `pr_merged` но не в `wf`, `date_windows` не содержит его ключ → `date_window=None`. Это приведёт к `to_days(None) = -1`, и строка будет отсортирована первой, что сломает оконные агрегации.

**Исправление:** Добавить `date_windows` из PR-merged данных тоже.

### ISSUE-1: `failing_since` может перескакивать через разрывы

**Файл:** `mute_optimizer_v3.py`, `compute_metrics`, строки 678-688

`failing_since` сбрасывается только после 10 consecutive clean дней. Но если тест вообще не запускался 10 дней (нет записей), `consecutive_clean` всё равно инкрементится для каждого дня в `dates`, даже если данных нет. Это корректно по замыслу (нет данных = нет failure), но стоит проверить.

### ISSUE-2: `would_mute` проверяет `fail_count`, а не `total_fails`

**Файл:** `mute_optimizer_v3.py`, строка 563

```python
return agg['fail_count'] >= cfg.mute_fail_threshold
```

`total_fails = fail_count + mute_count`. Production код (`create_new_muted_ya.py`) использует `fail_count` (не включая mute). Наша реализация согласована с production — это OK.

Но `would_unmute` проверяет `total_fails == 0`, что включает `mute_count`. Это значит: тест не размьючивается, пока у него есть статусы `mute` (= тест всё ещё числится замьюченным в CI). Это **корректное** поведение.

### ISSUE-3: Sampling нестабилен между триалами

**Файл:** `mute_optimizer_v3_ml.py`, строки 91-96

```python
random.seed(42)
keys = list(daily.keys())
n = max(1000, int(len(keys) * sample_ratio))
daily = {k: daily[k] for k in random.sample(keys, min(n, len(keys)))}
```

`daily` может иметь **разные ключи** в разных триалах (зависит от `use_pr`, `pr_source`, `pr_coefficient`). `random.seed(42)` гарантирует одинаковый seed, но `list(daily.keys())` может дать разный порядок → `random.sample` выберет разные тесты. Это делает сравнение между триалами с разными PR-настройками не полностью честным.

**Рекомендация:** Семплировать на уровне WF-ключей (которые одинаковы для всех триалов), а PR-данные добавлять потом.

### ISSUE-4: `build_daily_fast` — PR-only тесты без WF-данных

Если `job_filter='wf_and_pr'` и тест есть в `pre.pr` но не в `pre.wf`, он добавляется в `all_keys` (строка 404/407). Для него:
- `wf` данные = 0 (pass/fail/mute/skip = 0)
- PR данные добавляются с коэффициентом
- `date_window = None` (см. BUG-3)

Такие тесты искажают simulate: они имеют только PR-runs, симуляция работает на потенциально некорректных датах.

### ISSUE-5: PROD_CONFIG.mute_fail_threshold_low_runs не тестируется Optuna

Production использует two-tier threshold (`fail>=3` при runs>10, `fail>=2` при runs<=10). Optuna не включает `mute_fail_threshold_low_runs` и `low_runs_bound` в пространство поиска — использует единый `mute_fail_threshold`. Это может давать нечестное сравнение: production гибче в low-runs случае.

### OK: Коэффициент PR применяется консистентно

После фикса: `pass_count, fail_count, mute_count, skip_count` все умножаются на `coef`. При `coef=1.0` PR данные наравне с WF. Это **корректно**.

### OK: `total_runs` и `total_fails` формулы

```python
total_runs = pass_count + fail_count + mute_count  # skip не считается
total_fails = fail_count + mute_count
```

Согласовано с production (`create_new_muted_ya.py`).

### OK: Window aggregation

`aggregate_for_window_by_runs` и `aggregate_for_window_hybrid` корректно итерируют назад по отсортированным (ascending) данным.
