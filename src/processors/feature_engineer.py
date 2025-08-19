"""
Feature engineering processor for greyhound racing data.
Combines race card and dog statistics to create modeling-ready features.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from ..utils.file_utils import save_to_csv, load_csv


class FeatureEngineer:
    """Feature engineering for greyhound racing prediction model."""
    
    def __init__(self):
        self.feature_columns = []
    
    def process_data(self, race_data: pd.DataFrame, dog_stats: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw data into modeling-ready features.
        
        Args:
            race_data: DataFrame with race card information
            dog_stats: DataFrame with detailed dog statistics
            
        Returns:
            DataFrame with engineered features
        """
        print("Starting feature engineering...")
        
        # Merge race data with dog stats
        merged_data = self._merge_data(race_data, dog_stats)
        
        if merged_data.empty:
            print("No data to process after merge")
            return pd.DataFrame()
        
        # Engineer features
        featured_data = self._engineer_features(merged_data)
        
        # Final cleanup and validation
        final_data = self._finalize_features(featured_data)
        
        print(f"Feature engineering completed. Shape: {final_data.shape}")
        return final_data
    
    def _merge_data(self, race_data: pd.DataFrame, dog_stats: pd.DataFrame) -> pd.DataFrame:
        """Merge race card data with dog statistics."""
        print("Merging race data with dog statistics...")
        
        if dog_stats.empty:
            print("No dog statistics available, using race data only")
            return race_data.copy()
        
        # Merge on dog name
        merged = race_data.merge(
            dog_stats, 
            on='Dog_Name', 
            how='left',
            suffixes=('', '_stats')
        )
        
        print(f"Merged data shape: {merged.shape}")
        return merged
    
    def _engineer_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create engineered features from merged data."""
        print("Engineering features...")
        
        df = data.copy()
        
        # Basic race features
        df = self._create_race_features(df)
        
        # Performance features
        df = self._create_performance_features(df)
        
        # Track-specific features
        df = self._create_track_features(df)
        
        # Distance-specific features
        df = self._create_distance_features(df)
        
        # Grade-specific features
        df = self._create_grade_features(df)
        
        # Trap-specific features
        df = self._create_trap_features(df)
        
        # Form features
        df = self._create_form_features(df)
        
        # Time-based features
        df = self._create_time_features(df)
        
        return df
    
    def _create_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create basic race-level features."""
        # Race size (number of runners)
        race_sizes = df.groupby(['Track', 'Race_Number']).size()
        df['Race_Size'] = df.apply(lambda x: race_sizes[(x['Track'], x['Race_Number'])], axis=1)
        
        # Distance numeric
        df['Distance_Meters'] = df['Distance'].str.extract('(\d+)').astype(float)
        
        # Grade numeric (extract number from grade like A1, B2, etc.)
        df['Grade_Number'] = df['Grade'].str.extract('(\d+)').astype(float)
        df['Grade_Letter'] = df['Grade'].str.extract('([A-Z])').fillna('U')
        
        # Trap number handling
        df['Trap_Number'] = pd.to_numeric(df['Trap'], errors='coerce').fillna(0)
        
        return df
    
    def _create_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create performance-based features."""
        # Win rate and place rate
        df['Win_Rate'] = df.get('Win_Percentage', 0) / 100
        df['Place_Rate'] = df.get('Place_Percentage', 0) / 100
        
        # Total experience
        df['Total_Experience'] = df.get('Total_Runs', 0)
        
        # Success metrics
        df['Success_Rate'] = df['Win_Rate'] + (df['Place_Rate'] * 0.5)  # Weighted success
        
        # Performance consistency (placeholder - would need race-by-race data)
        df['Performance_Consistency'] = 0.5  # Default value
        
        return df
    
    def _create_track_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create track-specific features."""
        # Track performance (would need historical data)
        df['Track_Win_Rate'] = df['Win_Rate']  # Placeholder
        df['Track_Experience'] = df['Total_Experience']  # Placeholder
        
        # Track difficulty (based on average times - placeholder)
        track_difficulty = {
            'Belle Vue': 0.8,
            'Crayford': 0.7,
            'Hove': 0.9,
            'Romford': 0.6,
            'Default': 0.7
        }
        df['Track_Difficulty'] = df['Track'].map(track_difficulty).fillna(0.7)
        
        return df
    
    def _create_distance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create distance-specific features."""
        # Distance category
        def categorize_distance(distance):
            if distance <= 300:
                return 'Sprint'
            elif distance <= 500:
                return 'Middle'
            else:
                return 'Long'
        
        df['Distance_Category'] = df['Distance_Meters'].apply(categorize_distance)
        
        # Distance preference (placeholder - would need historical data)
        df['Distance_Preference'] = 0.5  # Default neutral preference
        
        return df
    
    def _create_grade_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create grade-specific features."""
        # Grade level (A=1, B=2, etc.)
        grade_mapping = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6}
        df['Grade_Level'] = df['Grade_Letter'].map(grade_mapping).fillna(6)
        
        # Combined grade score
        df['Grade_Score'] = df['Grade_Level'] + (df['Grade_Number'] / 10)
        
        return df
    
    def _create_trap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create trap-specific features."""
        # Trap advantage (based on general statistics)
        trap_advantage = {1: 0.9, 2: 0.8, 3: 0.7, 4: 0.6, 5: 0.65, 6: 0.7}
        df['Trap_Advantage'] = df['Trap_Number'].map(trap_advantage).fillna(0.5)
        
        # Inside/outside trap
        df['Inside_Trap'] = (df['Trap_Number'] <= 2).astype(int)
        df['Outside_Trap'] = (df['Trap_Number'] >= 5).astype(int)
        
        return df
    
    def _create_form_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create form-based features."""
        # Parse form string (placeholder - would need proper form parsing)
        df['Recent_Form_Score'] = 0.5  # Default neutral form
        
        # Handle Recent_Form column safely
        if 'Recent_Form' in df.columns:
            df['Form_Length'] = df['Recent_Form'].astype(str).str.len()
        else:
            df['Form_Length'] = 0  # Default when no form data available
        
        return df
    
    def _create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create time-based features."""
        # Best time relative to distance
        df['Time_Per_Meter'] = df.get('Best_Time', 30.0) / df['Distance_Meters']
        
        # Speed score (placeholder)
        df['Speed_Score'] = 1.0 / (df['Time_Per_Meter'] + 0.001)  # Avoid division by zero
        
        return df
    
    def _finalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final feature selection and cleanup."""
        # Define final feature columns
        feature_columns = [
            # Race identifiers
            'Track', 'Race_Number', 'Race_Time', 'Dog_Name', 'Trap_Number',
            
            # Basic features
            'Grade', 'Distance', 'Race_Size', 'Distance_Meters', 'Grade_Score',
            
            # Performance features
            'Win_Rate', 'Place_Rate', 'Success_Rate', 'Total_Experience',
            
            # Track features
            'Track_Difficulty', 'Track_Win_Rate',
            
            # Distance features
            'Distance_Category', 'Distance_Preference',
            
            # Trap features
            'Trap_Advantage', 'Inside_Trap', 'Outside_Trap',
            
            # Form features
            'Recent_Form_Score', 'Form_Length',
            
            # Time features
            'Speed_Score', 'Time_Per_Meter'
        ]
        
        # Select available columns
        available_columns = [col for col in feature_columns if col in df.columns]
        df_final = df[available_columns].copy()
        
        # Fill missing values
        numeric_columns = df_final.select_dtypes(include=[np.number]).columns
        df_final[numeric_columns] = df_final[numeric_columns].fillna(0)
        
        categorical_columns = df_final.select_dtypes(include=['object']).columns
        df_final[categorical_columns] = df_final[categorical_columns].fillna('Unknown')
        
        # Add feature creation timestamp
        df_final['Feature_Creation_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return df_final
    
    def create_dual_outputs(self, featured_data: pd.DataFrame) -> Tuple[str, str]:
        """
        Create both daily and historical datasets.
        
        Args:
            featured_data: DataFrame with engineered features
            
        Returns:
            Tuple of (daily_file_path, historical_file_path)
        """
        if featured_data.empty:
            print("No data to save")
            return "", ""
        
        # Daily overwrite file
        daily_file = save_to_csv(
            featured_data, 
            filename="todays_model.csv", 
            directory="data/processed"
        )
        
        # Historical append file
        historical_file = "data/processed/modeling_ready_dataset_historical.csv"
        
        try:
            # Load existing historical data
            if pd.io.common.file_exists(historical_file):
                existing_data = load_csv(historical_file)
                
                # Remove duplicates based on key columns
                key_columns = ['Track', 'Race_Number', 'Dog_Name', 'Race_Time']
                
                # Filter out today's data from historical if it exists
                today_str = datetime.now().strftime('%Y-%m-%d')
                if 'Feature_Creation_Date' in existing_data.columns:
                    existing_data = existing_data[
                        ~existing_data['Feature_Creation_Date'].str.contains(today_str, na=False)
                    ]
                
                # Append new data
                combined_data = pd.concat([existing_data, featured_data], ignore_index=True)
                
                # Remove duplicates
                combined_data = combined_data.drop_duplicates(subset=key_columns, keep='last')
            else:
                combined_data = featured_data
            
            # Save historical file
            combined_data.to_csv(historical_file, index=False)
            print(f"Historical data saved to {historical_file}")
            
        except Exception as e:
            print(f"Error saving historical data: {e}")
            # Fallback to just saving today's data
            featured_data.to_csv(historical_file, index=False)
        
        return daily_file, historical_file


def engineer_features(race_data: pd.DataFrame, dog_stats: pd.DataFrame = None) -> pd.DataFrame:
    """
    Main function for feature engineering.
    
    Args:
        race_data: DataFrame with race card data
        dog_stats: Optional DataFrame with dog statistics
        
    Returns:
        DataFrame with engineered features
    """
    if dog_stats is None:
        dog_stats = pd.DataFrame()
    
    engineer = FeatureEngineer()
    return engineer.process_data(race_data, dog_stats)


if __name__ == "__main__":
    from ..utils.file_utils import get_latest_file
    
    # Load latest race data and dog stats
    race_file = get_latest_file("../../data/raw", "race_cards_*.csv")
    stats_file = get_latest_file("../../data/raw", "dog_stats_*.csv")
    
    if race_file:
        race_data = load_csv(race_file)
        dog_stats = load_csv(stats_file) if stats_file else pd.DataFrame()
        
        featured_data = engineer_features(race_data, dog_stats)
        
        if not featured_data.empty:
            engineer = FeatureEngineer()
            daily_file, historical_file = engineer.create_dual_outputs(featured_data)
            print(f"Feature engineering completed:")
            print(f"  Daily file: {daily_file}")
            print(f"  Historical file: {historical_file}")
        else:
            print("No features engineered")
    else:
        print("No race data file found")
