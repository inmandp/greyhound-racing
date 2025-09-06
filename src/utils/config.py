"""
Configuration settings for the greyhound racing data pipeline.
"""

import os
from pathlib import Path
from typing import Dict, Any


class Config:
    """Configuration class for pipeline settings."""
    
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    LOGS_DIR = PROJECT_ROOT / "logs"
    CONFIG_DIR = PROJECT_ROOT / "config"
    # Subfolder specifically for historical results
    RAW_RESULTS_DIR = RAW_DATA_DIR / "results"
    
    # URLs
    RACING_POST_URL = "https://greyhoundbet.racingpost.com/"
    GREYHOUND_STATS_URL = "https://greyhoundstats.co.uk"
    
    # Extraction settings
    EXTRACTION_SETTINGS = {
        "max_workers": 2,
        "request_timeout": 10,
        "cache_bust_frequency": 8,  # Every N races on same track
        "aggressive_cache_bust_delay": 8,  # Seconds
        "light_cache_bust_delay": 2,  # Seconds
        "page_load_timeout": 3,  # Seconds
        "content_wait_timeout": 2,  # Seconds
    }
    
    # Browser settings
    BROWSER_SETTINGS = {
        "headless": True,
        "window_size": "1366,768",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "disable_images": True,
        "disable_plugins": True,
        "disable_extensions": True,
    }
    
    # Feature engineering settings
    FEATURE_SETTINGS = {
        "trap_advantages": {1: 0.9, 2: 0.8, 3: 0.7, 4: 0.6, 5: 0.65, 6: 0.7},
        "track_difficulties": {
            "Belle Vue": 0.8,
            "Crayford": 0.7,
            "Hove": 0.9,
            "Romford": 0.6,
            "Default": 0.7
        },
        "grade_mapping": {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6},
        "distance_categories": {
            "sprint_max": 300,
            "middle_max": 500
        }
    }
    
    # File naming patterns
    FILE_PATTERNS = {
        "race_cards": "race_cards_{date}.csv",
    "race_results": "results_{date}.csv",
        "dog_stats": "dog_stats_{date}.csv",
        "daily_model": "todays_model.csv",
        "historical_model": "modeling_ready_dataset_historical.csv",
        "logs": "pipeline_{date}.log"
    }
    
    # Logging settings
    LOGGING_SETTINGS = {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "max_file_size": 10 * 1024 * 1024,  # 10MB
        "backup_count": 5
    }
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist."""
        directories = [
            cls.DATA_DIR,
            cls.RAW_DATA_DIR,
            cls.RAW_RESULTS_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.LOGS_DIR,
            cls.CONFIG_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_file_path(cls, file_type: str, date_str: str = None) -> Path:
        """
        Get file path for a specific file type.
        
        Args:
            file_type: Type of file (race_cards, dog_stats, etc.)
            date_str: Date string for date-based files
            
        Returns:
            Path object for the file
        """
        if date_str is None:
            from datetime import datetime
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        pattern = cls.FILE_PATTERNS.get(file_type, f"{file_type}_{date_str}.csv")
        filename = pattern.format(date=date_str)
        
        if file_type == "race_results":
            return cls.RAW_RESULTS_DIR / filename
        if file_type in ["race_cards", "dog_stats"]:
            return cls.RAW_DATA_DIR / filename
        elif file_type in ["daily_model", "historical_model"]:
            return cls.PROCESSED_DATA_DIR / filename
        elif file_type == "logs":
            return cls.LOGS_DIR / filename
        else:
            return cls.DATA_DIR / filename
    
    @classmethod
    def load_custom_config(cls, config_file: str = "custom_config.json") -> Dict[str, Any]:
        """
        Load custom configuration from JSON file.
        
        Args:
            config_file: Name of the config file
            
        Returns:
            Dictionary with custom configuration
        """
        import json
        
        config_path = cls.CONFIG_DIR / config_file
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading custom config: {e}")
        
        return {}
    
    @classmethod
    def save_custom_config(cls, config_data: Dict[str, Any], config_file: str = "custom_config.json"):
        """
        Save custom configuration to JSON file.
        
        Args:
            config_data: Configuration data to save
            config_file: Name of the config file
        """
        import json
        
        cls.ensure_directories()
        config_path = cls.CONFIG_DIR / config_file
        
        try:
            with open(config_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            print(f"Custom config saved to {config_path}")
        except Exception as e:
            print(f"Error saving custom config: {e}")


# Create default configuration instance
config = Config()

# Ensure directories exist on import
config.ensure_directories()
