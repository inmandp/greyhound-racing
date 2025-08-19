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

# Extract dog statistics (requires race card data)
python -m src.extractors.dog_stats_extractor

# Feature engineering (requires race card data)
python -m src.processors.feature_engineer
```

## Output Files

### Daily Files (Overwritten Each Run)
- `data/processed/todays_model.csv` - Today's modeling-ready dataset

### Historical Files (Appended)
- `data/processed/modeling_ready_dataset_historical.csv` - Complete historical dataset for ML training

### Raw Data Files
- `data/raw/race_cards_YYYY-MM-DD.csv` - Raw race card data
- `data/raw/dog_stats_YYYY-MM-DD.csv` - Raw dog statistics

### Logs
- `logs/pipeline_YYYY-MM-DD.log` - Detailed execution logs
- `logs/summary_YYYY-MM-DD.txt` - Summary reports

## Configuration

The pipeline uses a centralized configuration system in `src/utils/config.py`. You can customize:

- **URLs**: Racing Post and dog stats websites
- **Extraction Settings**: Timeouts, cache busting frequency
- **Browser Settings**: Headless mode, user agents
- **Feature Settings**: Trap advantages, track difficulties
- **File Patterns**: Output file naming conventions

### Custom Configuration

Create a custom config file:
```python
from src.utils.config import config

# Load custom settings
custom_config = {
    "extraction_settings": {
        "max_workers": 15,
        "request_timeout": 15
    }
}

config.save_custom_config(custom_config)
```

## Features Engineering

The pipeline creates advanced features including:

- **Performance Metrics**: Win rates, place rates, success scores
- **Track Features**: Track-specific performance, difficulty scores
- **Distance Features**: Distance categories, preferences
- **Grade Features**: Grade levels, combined scores
- **Trap Features**: Trap advantages, inside/outside positions
- **Form Features**: Recent form analysis
- **Time Features**: Speed scores, time per meter

## Logging

Comprehensive logging includes:
- **Execution Details**: Step-by-step pipeline progress
- **Error Handling**: Detailed error messages with stack traces
- **Performance Metrics**: Extraction counts, success rates
- **Summary Reports**: High-level execution summaries

## Error Handling

The pipeline includes robust error handling:
- **Graceful Degradation**: Continues with available data if some steps fail
- **Cache Issue Detection**: Automatically detects and handles SPA caching problems
- **Retry Logic**: Intelligent retry with cache busting for failed extractions
- **Duplicate Prevention**: Prevents duplicate data in historical datasets

## Best Practices

- **Daily Execution**: Run once per day to get fresh race data
- **Data Validation**: Always check logs for extraction success rates
- **Historical Data**: Use the historical dataset for model training
- **Daily Data**: Use the daily dataset for predictions
- **Backup Strategy**: Consider backing up historical datasets regularly

## Troubleshooting

### Common Issues

1. **Chrome Driver Issues**:
   ```bash
   # The pipeline automatically manages Chrome driver
   # If issues persist, try updating selenium:
   pip install --upgrade selenium webdriver-manager
   ```

2. **Network Timeouts**:
   - Check internet connection
   - Increase timeout values in config
   - Run during off-peak hours

3. **Missing Data**:
   - Check logs for specific error messages
   - Verify website availability
   - Check for website structure changes

4. **Cache Issues**:
   - Pipeline includes smart cache busting
   - If persistent, check browser_utils.py settings

### Getting Help

1. Check the logs directory for detailed error messages
2. Review the summary report for overall pipeline health
3. Verify all dependencies are properly installed
4. Ensure sufficient disk space for data files

## Development

### Adding New Features

1. **New Extractors**: Add to `src/extractors/`
2. **New Processors**: Add to `src/processors/`
3. **New Utilities**: Add to `src/utils/`
4. **Configuration**: Update `src/utils/config.py`
5. **Pipeline Integration**: Update `pipeline.py`

### Testing

Create tests in the `tests/` directory:
```python
# Example test structure
tests/
├── test_extractors.py
├── test_processors.py
└── test_utils.py
```

## License

This project is intended for educational and research purposes. Please respect the terms of service of the data sources.

## Contributing

1. Follow the existing code structure
2. Add comprehensive logging
3. Include error handling
4. Update configuration as needed
5. Document new features

## Changelog

### v1.0.0 (Current)
- Initial release with complete pipeline
- Race card extraction with smart caching
- Dog statistics extraction
- Advanced feature engineering
- Dual output system (daily/historical)
- Comprehensive logging and error handling
