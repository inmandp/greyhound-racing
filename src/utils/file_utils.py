"""
File utilities for data handling and I/O operations.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Optional


def save_to_csv(dataframe: pd.DataFrame, filename: Optional[str] = None, directory: str = "data/raw") -> str:
    """
    Save DataFrame to CSV file.
    
    Args:
        dataframe: DataFrame to save
        filename: Optional filename, defaults to date-based name
        directory: Directory to save file in
        
    Returns:
        Path to saved file
    """
    if dataframe is None or dataframe.empty:
        raise ValueError("Cannot save empty or None DataFrame")
    
    if filename is None:
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"race_cards_{today_str}.csv"
    
    # Ensure directory exists
    os.makedirs(directory, exist_ok=True)
    
    filepath = os.path.join(directory, filename)
    dataframe.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")
    
    return filepath


def load_csv(filepath: str) -> pd.DataFrame:
    """
    Load CSV file into DataFrame.
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Loaded DataFrame
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    return pd.read_csv(filepath)


def ensure_directory(directory: str) -> str:
    """
    Ensure directory exists, create if it doesn't.
    
    Args:
        directory: Directory path
        
    Returns:
        Directory path
    """
    os.makedirs(directory, exist_ok=True)
    return directory


def get_latest_file(directory: str, pattern: str = "*.csv") -> Optional[str]:
    """
    Get the latest file matching pattern in directory.
    
    Args:
        directory: Directory to search
        pattern: File pattern to match
        
    Returns:
        Path to latest file or None if no files found
    """
    import glob
    
    if not os.path.exists(directory):
        return None
    
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    
    # Return most recently modified file
    return max(files, key=os.path.getmtime)
