"""
Dog statistics extractor module.
Handles extraction of detailed historical stats for greyhounds.
"""

import time
import random
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from urllib.parse import quote

from ..utils.file_utils import save_to_csv


class DogStatsExtractor:
    """Extractor for detailed dog statistics from greyhoundstats.co.uk"""
    
    def __init__(self, base_url: str = "https://greyhoundstats.co.uk/complete_runner_stats.php"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })
        self.request_delay = 2.0  # Start with 2 second delay between requests
        self.max_retries = 3
        self.backoff_factor = 2.0
    
    def extract_dog_stats(self, race_data: pd.DataFrame, max_workers: int = 3) -> pd.DataFrame:
        """
        Extract detailed stats for all dogs in race data.
        
        Args:
            race_data: DataFrame containing race card data with dog names
            max_workers: Number of concurrent threads for extraction (reduced for rate limiting)
            
        Returns:
            DataFrame with detailed dog statistics
        """
        unique_dogs = race_data['Dog_Name'].unique()
        print(f"Extracting stats for {len(unique_dogs)} unique dogs...")
        print(f"Using {max_workers} workers with {self.request_delay}s delay between requests")
        
        all_stats = []
        failed_dogs = []
        
        # Use sequential processing for better rate limiting control
        if max_workers == 1:
            print("Using sequential processing for better rate limiting...")
            for i, dog_name in enumerate(unique_dogs, 1):
                print(f"Progress: {i}/{len(unique_dogs)} - Processing {dog_name}")
                
                stats = self._get_dog_stats_with_retry(dog_name)
                if stats:
                    all_stats.append(stats)
                    print(f"✓ Stats extracted for {dog_name}")
                else:
                    failed_dogs.append(dog_name)
                    print(f"✗ Failed to extract stats for {dog_name}")
                
                # Add delay between requests
                if i < len(unique_dogs):  # Don't sleep after the last request
                    delay = self.request_delay + random.uniform(0.5, 1.5)  # Add random jitter
                    time.sleep(delay)
        else:
            # Use ThreadPoolExecutor with rate limiting
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_dog = {
                    executor.submit(self._get_dog_stats_with_retry, dog_name): dog_name 
                    for dog_name in unique_dogs
                }
                
                # Collect results
                for future in as_completed(future_to_dog):
                    dog_name = future_to_dog[future]
                    try:
                        stats = future.result()
                        if stats:
                            all_stats.append(stats)
                            print(f"✓ Stats extracted for {dog_name}")
                        else:
                            failed_dogs.append(dog_name)
                            print(f"✗ No stats found for {dog_name}")
                    except Exception as e:
                        failed_dogs.append(dog_name)
                        print(f"✗ Error extracting stats for {dog_name}: {e}")
        
        # Report results
        if failed_dogs:
            print(f"\n⚠️  Failed to extract stats for {len(failed_dogs)} dogs:")
            for dog in failed_dogs[:10]:  # Show first 10 failed dogs
                print(f"   - {dog}")
            if len(failed_dogs) > 10:
                print(f"   ... and {len(failed_dogs) - 10} more")
        
        if all_stats:
            stats_df = pd.DataFrame(all_stats)
            print(f"\n✅ Successfully extracted stats for {len(stats_df)} out of {len(unique_dogs)} dogs")
            return stats_df
        else:
            print("\n❌ No stats extracted")
            return pd.DataFrame()
    
    def _get_dog_stats_with_retry(self, dog_name: str, track_name: str = None) -> Optional[Dict]:
        """
        Get detailed statistics for a single dog with retry logic and error handling.
        
        Args:
            dog_name: Name of the dog
            track_name: Optional track name to filter by
            
        Returns:
            Dictionary with dog statistics or None if not found
        """
        for attempt in range(self.max_retries):
            try:
                return self._get_dog_stats(dog_name, track_name)
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    print(f"   ⚠️  403 Forbidden for {dog_name} (attempt {attempt + 1}/{self.max_retries})")
                    if attempt < self.max_retries - 1:
                        # Increase delay and try different headers
                        self._adjust_for_blocking()
                        delay = self.request_delay * (attempt + 2)
                        print(f"   ⏳ Waiting {delay:.1f}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"   ❌ Giving up on {dog_name} after {self.max_retries} 403 errors")
                        return None
                        
                elif e.response.status_code == 429:
                    print(f"   ⚠️  429 Rate Limited for {dog_name} (attempt {attempt + 1}/{self.max_retries})")
                    if attempt < self.max_retries - 1:
                        # Exponential backoff for rate limiting
                        delay = self.request_delay * (self.backoff_factor ** (attempt + 1))
                        delay += random.uniform(1, 3)  # Add jitter
                        print(f"   ⏳ Rate limited - waiting {delay:.1f}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"   ❌ Giving up on {dog_name} after {self.max_retries} rate limit errors")
                        return None
                        
                elif e.response.status_code == 404:
                    print(f"   ℹ️  Dog not found: {dog_name}")
                    return None
                    
                else:
                    print(f"   ❌ HTTP {e.response.status_code} error for {dog_name}: {e}")
                    if attempt < self.max_retries - 1:
                        delay = self.request_delay * (attempt + 1)
                        time.sleep(delay)
                        continue
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️  Network error for {dog_name} (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    delay = self.request_delay * (attempt + 1)
                    time.sleep(delay)
                    continue
                return None
                
            except Exception as e:
                print(f"   ❌ Unexpected error for {dog_name}: {e}")
                return None
        
        return None
    
    def _adjust_for_blocking(self):
        """Adjust headers and settings when getting blocked."""
        # Rotate user agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]
        
        self.session.headers['User-Agent'] = random.choice(user_agents)
        
        # Increase delay
        self.request_delay = min(self.request_delay * 1.5, 10.0)  # Cap at 10 seconds
    
    def _get_dog_stats(self, dog_name: str, track_name: str = None) -> Optional[Dict]:
        """
        Get detailed statistics for a single dog.
        
        Args:
            dog_name: Name of the dog
            track_name: Optional track name to filter by
            
        Returns:
            Dictionary with dog statistics or None if not found
        """
        try:
            # URL encode the dog name for the query
            encoded_dog_name = quote(dog_name)
            encoded_track_name = quote(track_name) if track_name else ""
            
            # Build the URL using the working pattern from the old file
            params = f"?dog={encoded_dog_name}&track={encoded_track_name}&pos=&trap=&grade=&distance="
            url = self.base_url + params
            
            print(f"Fetching stats for {dog_name}")
            
            # Make the request
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # Parse the response
            return self._parse_dog_stats(response.text, dog_name)
            
        except Exception as e:
            print(f"Error getting stats for {dog_name}: {e}")
            return None
    
    def _parse_search_results(self, html: str, dog_name: str) -> Optional[str]:
        """
        Parse search results to find the correct dog URL.
        
        Args:
            html: HTML content from search results
            dog_name: Name of the dog we're looking for
            
        Returns:
            URL to dog's detail page or None if not found
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for dog links in search results
            dog_links = soup.find_all('a', href=lambda x: x and '/dog/' in x)
            
            for link in dog_links:
                link_text = link.get_text().strip()
                if dog_name.lower() in link_text.lower():
                    return f"{self.base_url}{link.get('href')}"
            
            return None
            
        except Exception as e:
            print(f"Error parsing search results for {dog_name}: {e}")
            return None
    
    def _parse_dog_stats(self, html: str, dog_name: str) -> Optional[Dict]:
        """
        Parse detailed statistics from dog's page using the working logic from the old file.
        
        Args:
            html: HTML content from dog's detail page
            dog_name: Name of the dog
            
        Returns:
            Dictionary with parsed statistics
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract summary statistics
            summary_stats = self._extract_summary_stats(soup, dog_name)
            
            # Extract detailed race history
            race_history = self._extract_race_history(soup, dog_name)
            
            # Combine summary with additional calculated fields
            stats = {
                'Dog_Name': dog_name,
                'Total_Runs': summary_stats.get('total_runs', 0),
                'Wins': summary_stats.get('total_wins', 0),
                'Win_Percentage': summary_stats.get('win_percentage', 0.0),
                'Places': 0,  # Would need additional parsing
                'Place_Percentage': 0.0,  # Would need additional parsing
                'Best_Time': None,  # Would need additional parsing
                'Average_Time': None,  # Would need additional parsing
                'Recent_Form': '',  # Would need additional parsing
                'Career_Earnings': 0.0,  # Would need additional parsing
                'Last_Run_Date': None,  # Would need additional parsing
                'Age': None,  # Would need additional parsing
                'Sex': None,  # Would need additional parsing
                'Color': None,  # Would need additional parsing
                'Sire': None,  # Would need additional parsing
                'Dam': None,  # Would need additional parsing
                'Trainer': None,  # Would need additional parsing
                'Owner': None,  # Would need additional parsing
                'Race_Count': len(race_history) if race_history else 0,
                'Track': summary_stats.get('track', 'All Tracks')
            }
            
            return stats
            
        except Exception as e:
            print(f"Error parsing stats for {dog_name}: {e}")
            return None
    
    def _extract_summary_stats(self, soup, dog_name, track_name=None):
        """
        Extract the summary statistics table (Runs, Wins, Win %).
        """
        summary = {
            'dog_name': dog_name,
            'track': track_name or 'All Tracks',
            'total_runs': 0,
            'total_wins': 0,
            'win_percentage': 0.0
        }
        
        try:
            # Find the summary table - it's the first table with Runs, Wins, Win %
            tables = soup.find_all('table')
            
            for table in tables:
                # Look for table with headers "Runs", "Wins", "Win %"
                headers = table.find_all('th') if table.find('th') else table.find_all('td')
                if len(headers) >= 3:
                    header_text = [h.get_text().strip() for h in headers]
                    if 'Runs' in header_text and 'Wins' in header_text and 'Win %' in header_text:
                        # Found the summary table
                        rows = table.find_all('tr')
                        if len(rows) > 1:  # Skip header row
                            data_row = rows[1]
                            cells = data_row.find_all('td')
                            if len(cells) >= 3:
                                summary['total_runs'] = int(cells[0].get_text().strip())
                                summary['total_wins'] = int(cells[1].get_text().strip())
                                summary['win_percentage'] = float(cells[2].get_text().strip())
                                break
                                
        except Exception as e:
            print(f"Error extracting summary stats: {e}")
        
        return summary

    def _extract_race_history(self, soup, dog_name, track_name=None):
        """
        Extract the detailed race history table.
        """
        races = []
        
        try:
            import re
            # Find the main race results table
            tables = soup.find_all('table')
            
            for table in tables:
                # Look for the race history table (has Date, Track, Trap, etc. columns)
                headers = table.find_all('th') if table.find('th') else []
                if not headers:
                    # Sometimes headers are in the first tr as td elements
                    first_row = table.find('tr')
                    if first_row:
                        headers = first_row.find_all('td')
                
                if headers:
                    header_text = [h.get_text().strip() for h in headers]
                    # Check if this looks like the race history table
                    if 'Date' in header_text and 'Track' in header_text and 'Grade' in header_text:
                        # Found the race history table
                        rows = table.find_all('tr')[1:]  # Skip header row
                        
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) >= 10:  # Ensure we have enough columns
                                try:
                                    race_data = self._extract_race_data_from_row(cells, dog_name, track_name)
                                    if race_data:
                                        races.append(race_data)
                                except Exception as e:
                                    continue  # Skip problematic rows silently
                        break
                        
        except Exception as e:
            print(f"Error extracting race history: {e}")
        
        return races

    def _extract_race_data_from_row(self, cells, dog_name, track_name=None):
        """
        Extract race data from a table row.
        """
        try:
            import re
            # Skip AVERAGE row
            if cells[0].get_text().strip() == 'AVERAGE':
                return None
            
            # Extract trap number from image source
            trap_number = 'N/A'
            if len(cells) > 2:
                trap_cell = cells[2]
                img_tag = trap_cell.find('img')
                if img_tag and img_tag.get('src'):
                    src = img_tag.get('src')
                    # Extract number from './images/trap_1.jpg' format
                    trap_match = re.search(r'trap_(\d+)\.', src)
                    if trap_match:
                        trap_number = trap_match.group(1)
            
            race_data = {
                'dog_name': dog_name,
                'track': track_name or 'All Tracks',
                'date': cells[0].get_text().strip() if len(cells) > 0 else '',
                'track_name': cells[1].get_text().strip() if len(cells) > 1 else '',
                'trap': trap_number,
                'grade': cells[3].get_text().strip() if len(cells) > 3 else '',
                'distance': cells[4].get_text().strip() if len(cells) > 4 else '',
                'going': cells[5].get_text().strip() if len(cells) > 5 else '',
                'runners': cells[6].get_text().strip() if len(cells) > 6 else '',
                'position': cells[7].get_text().strip() if len(cells) > 7 else '',
                'btn': cells[8].get_text().strip() if len(cells) > 8 else '',
                'time': cells[9].get_text().strip() if len(cells) > 9 else '',
                'sp': cells[10].get_text().strip() if len(cells) > 10 else '',
                'comment': cells[11].get_text().strip() if len(cells) > 11 else ''
            }
            
            return race_data
            
        except Exception as e:
            print(f"Error extracting race data from row: {e}")
            return None


def extract_dog_statistics(race_data: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to extract dog statistics.
    
    Args:
        race_data: DataFrame containing race card data
        
    Returns:
        DataFrame with dog statistics
    """
    extractor = DogStatsExtractor()
    return extractor.extract_dog_stats(race_data)


if __name__ == "__main__":
    # Test with sample data
    from ..utils.file_utils import load_csv, get_latest_file
    
    # Load latest race data
    latest_race_file = get_latest_file("../../data/raw", "race_cards_*.csv")
    if latest_race_file:
        race_data = load_csv(latest_race_file)
        dog_stats = extract_dog_statistics(race_data)
        
        if not dog_stats.empty:
            # Save dog stats
            save_to_csv(dog_stats, directory="../../data/raw")
            print("Dog statistics extraction completed")
        else:
            print("No dog statistics extracted")
    else:
        print("No race data file found")
