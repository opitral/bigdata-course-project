# Big Data Course Project

Лабораторні роботи з дисципліни **Аналіз великих даних** — медальйонна архітектура (Bronze → Silver → Gold) на Cloudflare R2 + Apache Spark, плюс ML-шар з регресією/класифікацією/кластеризацією.

Домен — `weather` (Open-Meteo). Усі лабораторні запускаються **локально** (без VM).

## Структура

```
src/
├── bronze/        # ingest сирих даних у R2
├── silver/        # очищення/нормалізація з Bronze у Silver
├── gold/          # агреговані метрики з Silver у Gold
├── ml/            # моделі прогнозу швидкості/заторів + heatmap
└── common/        # спільні утиліти (Spark session, R2 paths)
reports/           # ML-артефакти (gitignored)
```

## Передумови

### Системні залежності

- **Python 3.11**
- **Java 17** (Hadoop 3.4 з Java 25 несумісний).
  На macOS, наприклад, MS OpenJDK 17:

### Python venv

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### `.env`

У корені проекту створіть `.env` з R2-ключами та прізвищем:

```env
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=data
R2_BASE_PREFIX=raw
AUTHOR_SURNAME=...
```

### Спільний префікс команд

Усі скрипти запускаються з кореня репозиторію з `PYTHONPATH=src`. Spark-етапи додатково потребують `JAVA_HOME=...openjdk-17...`. Для зручності задаємо в одному рядку:

```bash
export JAVA_HOME=/Users/slobodian/Library/Java/JavaVirtualMachines/ms-17.0.16/Contents/Home
export PYTHONPATH=src
source venv/bin/activate
```

---

## Лабораторна Bronze (Ingest)

**Що робить:** циклічно тягне сирі дані з зовнішніх API (Open-Meteo тощо), зберігає локально та вивантажує до R2 у `s3://data/raw/bronze/domain=.../source=.../ingest_date=.../hour=.../`.

Параметри керуються env-змінними (значення з прикладу — за замовчуванням):

| Змінна                    | Опис                                                            |
|---------------------------|-----------------------------------------------------------------|
| `INGEST_DOMAINS`          | Список доменів через кому (`weather`). За замовч. — всі відомі. |
| `INGEST_INTERVAL_SECONDS` | Період циклу збору                                              |
| `INGEST_MAX_ITERATIONS`   | `0` — нескінченно; інакше — N ітерацій                          |
| `LOCAL_DATA_ROOT`         | Локальна папка-буфер                                            |
| `SKIP_R2_UPLOAD`          | `1`/`true` — не завантажувати в R2                              |

**Запуск (нескінченний цикл, фон у tmux/screen):**

```bash
PYTHONPATH=src python -m bronze.ingest
```

**Разовий batch (одна ітерація):**

```bash
INGEST_MAX_ITERATIONS=1 PYTHONPATH=src python -m bronze.ingest
```

---

## Лабораторна Silver

**Що робить:** читає Bronze (JSONL з R2), фільтрує null/некоректні значення, нормалізує схему, пише Parquet у `s3://data/processed/silver/domain=.../source=.../schema_v=1/run_id=.../`.

```bash
JAVA_HOME=$JAVA_HOME PYTHONPATH=src python -m silver.pipeline \
    --domain weather \
    --source open_meteo_current
```

**Параметри:**

- `--domain` (обов'язково) — `weather`.
- `--source` (обов'язково) — `open_meteo_current`.
- `--output-format` — `parquet` (default) або `json`.

---

## Лабораторна Gold

**Що робить:** читає Silver, рахує метрики на Spark, пише в `s3://data/processed/gold/domain=weather/metric=<metric>/schema_v=1/run_id=.../`.

Доступні метрики (`src/gold/metrics/`):

- `temperature_hourly` — середня погодинна температура.
- `precipitation_daily` — опади за добу.
- `wind_daily_extremes` — макс/мін вітер за добу.
- `subzero_hours_daily` — кількість годин <0 °C на день.
- `comfort_index_hourly` — комбінований індекс комфорту.

**Усі метрики:**

```bash
JAVA_HOME=$JAVA_HOME PYTHONPATH=src python -m gold.gold_pipeline \
    --domain weather \
    --source open_meteo_current
```

**Підмножина:**

```bash
JAVA_HOME=$JAVA_HOME PYTHONPATH=src python -m gold.gold_pipeline \
    --domain weather \
    --source open_meteo_current \
    --metrics temperature_hourly precipitation_daily
```

`--output-format parquet|json` — формат запису (default `parquet`).

---

## Лабораторна ML

**Що робить:** будує локальні датасети з R2 silver (трафік `kpt_socketio` + `tomtom` від чужих студентів, погода — союз 3 джерел), тренує лінійну/логістичну регресію + K-means, малює heatmap.

### 4.1 Зібрати датасети

```bash
JAVA_HOME=$JAVA_HOME PYTHONPATH=src python -m ml.build_dataset \
    --time-from 2026-02-01 --time-to 2026-04-01
```

- Виводи: `reports/dataset_kpt.parquet`, `reports/dataset_tomtom.parquet`.
- Скрипт ідемпотентний — наявні файли пропускаються. Перебудувати — `--force`.
- KPT-частина читає ~5 ГБ через Spark (5–10 хв). TomTom читається через `boto3+pyarrow`.

### 4.2 Тренування

```bash
PYTHONPATH=src python -m ml.train_regression       # LinReg/Ridge/Lasso, baseline=mean, MAE/RMSE/R²
PYTHONPATH=src python -m ml.train_classification   # LogReg L2 (class_weight=balanced), acc/prec/rec/F1/AUC, confusion
PYTHONPATH=src python -m ml.train_kmeans           # K=2..10, вибір за silhouette
```

Метрики накопичуються в одному файлі `reports/metrics.json` (3 секції: `regression`, `classification`, `kmeans`).
Прогнози для heatmap: `reports/predictions_kpt.parquet`, `reports/predictions_tomtom.parquet`.
K-means центри: `reports/kmeans_centers.json`.

Кожен скрипт можна запустити для одного датасета: `--dataset kpt|tomtom|all` (default `all`).

### 4.3 Карта

```bash
PYTHONPATH=src python -m ml.visualize_map
```

Створить `reports/heatmap.html` (folium). Відкрити:

```bash
open reports/heatmap.html
```

Шари (керуються LayerControl у правому верхньому куті):

- **Real congestion (kpt / tomtom)** — фактичні затори з silver-даних.
- **Predicted congestion (kpt / tomtom)** — прогнози LogReg (вага точки = `congestion_proba`).
- **K-Means cluster centers** — центри секторів заторів (червоні маркери, popup з розміром та інтенсивністю).

---

## Послідовний запуск ML-етапу (one-liner)

```bash
source venv/bin/activate && \
JAVA_HOME=/Users/slobodian/Library/Java/JavaVirtualMachines/ms-17.0.16/Contents/Home \
PYTHONPATH=src \
python -m ml.build_dataset --time-from 2026-02-01 --time-to 2026-04-01 && \
PYTHONPATH=src python -m ml.train_regression && \
PYTHONPATH=src python -m ml.train_classification && \
PYTHONPATH=src python -m ml.train_kmeans && \
PYTHONPATH=src python -m ml.visualize_map && \
open reports/heatmap.html
```
