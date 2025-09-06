# Greyhound Racing Data Pipeline

A robust, end-to-end data pipeline for extracting, processing, and preparing greyhound racing data for machine learning models.

## Features

- **Race Card Extraction**: Scrapes daily race cards from Racing Post with smart caching
- **Dog Statistics**: Extracts detailed historical statistics for each dog
- **Feature Engineering**: Creates advanced features for ML modeling
- **Dual Output**: Maintains both daily overwrite and historical append datasets
- **Cache Management**: Handles SPA caching issues with intelligent cache busting
- **Comprehensive Logging**: Full execution logging and summary reports

## Project Structure

```
Dogs/
├── src/                          # Source code
│   ├── extractors/              # Data extraction modules
│   │   ├── race_card_extractor.py
│   │   └── dog_stats_extractor.py
│   ├── processors/              # Data processing modules
│   │   └── feature_engineer.py
│   └── utils/                   # Utility modules
│       ├── browser_utils.py
│       ├── file_utils.py
│       └── config.py
├── data/                        # Data storage
│   ├── raw/                     # Raw extracted data
│   └── processed/               # Processed, model-ready data
├── logs/                        # Execution logs
├── config/                      # Configuration files
├── tests/                       # Test files
├── pipeline.py                  # Main pipeline orchestrator
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd "Dogs"
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # or
   source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify installation:**
   ```bash
   python -c "import selenium, pandas, requests; print('All dependencies installed successfully')"
   ```

## Usage

### Quick Start

Run the complete pipeline:
```bash
python pipeline.py
```

This will:
1. Extract today's race cards from all tracks
2. Extract detailed statistics for all dogs
3. Engineer advanced features
4. Create both daily and historical datasets
5. Generate execution logs and summary reports

### Individual Components

You can also run individual components:

```bash
# Extract race cards only
python -m src.extractors.race_card_extractor

# Greyhound Racing Data Pipeline

A robust, end-to-end data pipeline for extracting, processing, and preparing greyhound racing data for machine learning models.

## Features

- Race Card Extraction: Scrapes today's race cards and historical results (inclusive date range) with smart caching
- Dog Statistics (RAW): Extracts raw race history rows for each dog (as shown on the site)
- Feature Engineering: Creates advanced features for ML modeling
- Dual Output: Maintains both daily overwrite and historical append datasets
- Cache Management: Handles SPA caching issues with intelligent cache busting
- Comprehensive Logging: Full execution logging and summary reports

## Project Structure

```
greyhound-racing/
├── src/                          # Source code
│   ├── extractors/              # Data extraction modules
│   │   ├── race_card_extractor.py
│   │   └── dog_stats_extractor.py
│   ├── processors/              # Data processing modules
│   │   └── feature_engineer.py
│   └── utils/                   # Utility modules
│       ├── browser_utils.py
│       ├── file_utils.py
│       └── config.py
├── data/                        # Data storage
│   ├── raw/                     # Raw extracted data
│   │   ├── results/             # Historical race results
│   │   └── stats/               # Raw dog stats
│   └── processed/               # Processed, model-ready data
├── logs/                        # Execution logs
├── config/                      # Configuration files
├── pipeline.py                  # Main pipeline orchestrator
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Installation

1) Create and activate a virtual environment

```bash
python -m venv .venv
# Linux/Mac
source .venv/bin/activate
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
```

2) Install dependencies

```bash
pip install -r requirements.txt
```

3) Verify installation

```bash
python -c "import selenium, pandas, requests; print('All dependencies installed successfully')"
```

## Usage

### Quick start

Run the complete pipeline:

```bash
python pipeline.py
```

This will:
- Extract today's race cards from all tracks
- Extract raw dog statistics for all dogs
- Engineer advanced features
- Create daily and historical datasets
- Generate execution logs and a summary report

### Historical mode

Extract historical race results for a date or inclusive date range (saved to data/raw/results):

```bash
# Single day
python pipeline.py --mode historical --start-date 2025-09-05

# Inclusive range
python pipeline.py --mode historical --start-date 2025-09-01 --end-date 2025-09-03
```

Notes:
- Today mode saves race cards to data/raw.
- Historical mode navigates the Racing Post results list per date and saves to data/raw/results.

### Individual components

```bash
# Extract race cards only
python -m src.extractors.race_card_extractor

# Extract dog statistics (requires race card data)
python -m src.extractors.dog_stats_extractor

# Feature engineering (requires race card and dog stats)
python -m src.processors.feature_engineer
```

## Output files

### Daily files (overwritten each run)
- data/processed/todays_model.csv — Today's modeling-ready dataset

### Historical files (appended)
- data/processed/modeling_ready_dataset_historical.csv — Complete historical dataset for ML training

### Raw data files
- data/raw/race_cards_YYYY-MM-DD.csv — Raw race card data (today)
- data/raw/results/results_YYYY-MM-DD.csv or results_YYYY-MM-DD_to_YYYY-MM-DD.csv — Historical race results
- data/raw/stats/dog_stats_YYYY-MM-DD.csv — Raw dog statistics

### Logs
- logs/pipeline_YYYY-MM-DD.log — Detailed execution logs
- logs/summary_YYYY-MM-DD.txt — Summary reports

## Configuration

Centralized config in `src/utils/config.py` controls:
- URLs: Racing Post and greyhoundstats
- Extraction settings: timeouts, cache busting frequency
- Browser settings: headless mode, user agent
- Feature settings: trap advantages, track difficulties
- File patterns and output paths

### Custom configuration

```python
from src.utils.config import config

custom_config = {
   "extraction_settings": {
      "max_workers": 2,
      "request_timeout": 10
   }
}

config.save_custom_config(custom_config)
```

## Feature Engineering

Creates features including:
- Performance metrics: win/place rates
- Track features: difficulty scores
- Distance features: categories and preferences
- Grade features: levels and combined scores
- Trap features: inside/outside positions
- Form features: recent form
- Time features: speed scores

## Troubleshooting

1) Chromium/ChromeDriver
```bash
# The extractor prefers system Chromium/Chrome + chromedriver.
# If needed, set CHROME_BINARY to point to your browser binary.
# If issues persist, upgrade these:
pip install --upgrade selenium webdriver-manager
```

2) Network timeouts
- Check internet connection
- Increase timeout values in config
- Run during off-peak hours

3) Missing data
- Check logs for specific error messages
- Verify website availability
- Check for website structure changes

4) Cache issues
- The extractor includes smart cache busting
- If persistent, check `src/utils/browser_utils.py` settings

## Development

- New extractors: add to `src/extractors/`
- New processors: add to `src/processors/`
- New utilities: add to `src/utils/`
- Configuration: update `src/utils/config.py`
- Pipeline integration: update `pipeline.py`

## Changelog

### v1.1.0 (Current)
- Added historical mode for race cards (results pages, inclusive date range)
- Historical outputs saved to data/raw/results
- Dog stats saved to data/raw/stats and extracted as RAW table rows
- Improved grade/distance parsing for today cards and time parsing for results pages
