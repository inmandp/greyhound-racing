"""
Race card extractor module for greyhound racing data.
Handles extraction of race cards from Racing Post website with smart caching.
"""

import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from ..utils.browser_utils import setup_chrome_options, smart_cache_bust
from ..utils.file_utils import save_to_csv


class RaceCardExtractor:
    """Main class for extracting race card data from Racing Post."""
    
    def __init__(self, base_url: str = "https://greyhoundbet.racingpost.com/"):
        self.base_url = base_url
        self.driver = None
        
    def extract_race_data(self) -> Optional[pd.DataFrame]:
        """
        Extract race data for today's races.
        
        Returns:
            DataFrame with race data or None if extraction fails
        """
        try:
            self._setup_driver()
            
            print("Loading Racing Post website...")
            self.driver.get(self.base_url)
            time.sleep(2)
            
            # Extract race card URLs
            race_card_urls = self._extract_race_card_urls()
            
            # Extract detailed race information
            all_race_data = self._extract_from_race_cards(race_card_urls)
            
            # Create DataFrame
            df = pd.DataFrame(all_race_data)
            print(f"Extracted {len(df)} race entries")
            return df
            
        except Exception as e:
            print(f"Error during race card extraction: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                print("Browser closed")
    
    def _setup_driver(self):
        """Setup Chrome driver with optimized options."""
        chrome_options = setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Disable automation detection
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    def _extract_race_card_urls(self) -> List[Dict]:
        """Extract all race card URLs from meetings."""
        race_card_urls = []
        
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        meeting_links = soup.find_all('a', href=lambda x: x and 'meeting-races' in x)
        
        print(f"Found {len(meeting_links)} meetings")
        processed_meetings = set()
        
        for meeting_link in meeting_links:
            try:
                track_element = meeting_link.find('h4')
                if not track_element:
                    continue
                    
                track_name = track_element.get_text().strip().split('\n')[0].strip()
                
                if track_name in processed_meetings:
                    continue
                processed_meetings.add(track_name)
                
                print(f"Processing meeting: {track_name}")
                
                meeting_href = meeting_link.get('href', '')
                if meeting_href:
                    full_url = f"{self.base_url}{meeting_href}"
                    self.driver.get(full_url)
                    time.sleep(2)
                    
                    meeting_race_urls = self._extract_race_urls_from_meeting(track_name)
                    race_card_urls.extend(meeting_race_urls)
                    
            except Exception as e:
                print(f"Error processing meeting {track_name}: {e}")
                continue
        
        print(f"Found {len(race_card_urls)} total race cards")
        return race_card_urls
    
    def _extract_race_urls_from_meeting(self, track_name: str) -> List[Dict]:
        """Extract race URLs from current meeting page."""
        race_urls = []
        
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        race_links = soup.find_all('a', href=lambda x: x and '#card/' in x)
        
        for race_link in race_links:
            href = race_link.get('href', '')
            if href:
                race_info = self._extract_race_info_from_link(race_link)
                if race_info:
                    race_urls.append({
                        'url': href,
                        'track': track_name,
                        'race_time': race_info.get('time', 'TBC'),
                        'race_number': race_info.get('number', 'TBC')
                    })
        
        print(f"Found {len(race_urls)} races for {track_name}")
        return race_urls
    
    def _extract_race_info_from_link(self, race_link) -> Optional[Dict]:
        """Extract race time and number from race link."""
        try:
            time_element = race_link.find('strong')
            race_time = time_element.get_text().strip() if time_element else 'TBC'
            
            race_number_element = race_link.find('h4')
            race_number = 'TBC'
            if race_number_element:
                race_text = race_number_element.get_text().strip()
                race_match = re.search(r'Race (\d+)', race_text)
                if race_match:
                    race_number = race_match.group(1)
            
            return {
                'time': race_time,
                'number': race_number
            }
        except:
            return None
    
    def _extract_from_race_cards(self, race_card_urls: List[Dict]) -> List[Dict]:
        """Extract runners from individual race cards with smart cache handling."""
        all_race_data = []
        current_track = None
        cache_bust_counter = 0
        
        print(f"Processing {len(race_card_urls)} race cards...")
        
        for i, race_info in enumerate(race_card_urls):
            try:
                print(f"Processing race {i+1}/{len(race_card_urls)}: {race_info['track']} Race {race_info['race_number']}")
                
                # Smart cache busting
                if current_track != race_info['track']:
                    print(f"    Switching to new track: {race_info['track']}")
                    smart_cache_bust(self.driver, force_aggressive=True)
                    current_track = race_info['track']
                    cache_bust_counter += 1
                elif i % 8 == 0:
                    smart_cache_bust(self.driver, force_aggressive=False)
                    cache_bust_counter += 1
                
                # Navigate to race card
                race_url = f"{self.base_url}{race_info['url']}"
                if i % 5 == 0:
                    race_url += f"?t={int(time.time())}"
                
                self.driver.get(race_url)
                time.sleep(1.5)
                
                # Quick content verification
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.ID, "sortContainer"))
                    )
                except:
                    print(f"    Content not loaded, refreshing...")
                    self.driver.refresh()
                    time.sleep(2)
                
                # Extract runners
                runners = self._extract_runners_from_race_card(race_info)
                
                if runners:
                    # Duplicate detection
                    if self._check_for_duplicates(all_race_data, runners, race_url):
                        runners = self._retry_with_cache_bust(race_info, race_url)
                    
                    if runners:
                        all_race_data.extend(runners)
                        print(f"    Extracted {len(runners)} runners")
                    else:
                        print(f"    Skipped race due to cache/duplicate issues")
                else:
                    print(f"    No runners found")
                    
            except Exception as e:
                print(f"Error processing race {i+1}: {e}")
                continue
        
        print(f"Cache busts performed: {cache_bust_counter}")
        return all_race_data
    
    def _extract_runners_from_race_card(self, race_info: Dict) -> List[Dict]:
        """Extract all runners from a race card page."""
        runners = []
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            grade, distance = self._extract_race_grade_and_distance(soup)
            
            sort_container = soup.find('div', id='sortContainer')
            if not sort_container:
                return runners
            
            runner_blocks = sort_container.find_all('div', class_='runnerBlock')
            print(f"Found {len(runner_blocks)} runners (Grade: {grade}, Distance: {distance})")
            
            for runner_block in runner_blocks:
                try:
                    runner_data = self._extract_runner_data(runner_block, race_info, grade, distance)
                    if runner_data:
                        runners.append(runner_data)
                except Exception as e:
                    print(f"Error processing runner: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error extracting runners: {e}")
        
        return runners
    
    def _extract_runner_data(self, runner_block, race_info: Dict, grade: str, distance: str) -> Optional[Dict]:
        """Extract data for a single runner."""
        dog_name_element = runner_block.find('strong')
        if not dog_name_element:
            return None
        
        dog_name = dog_name_element.get_text().strip()
        dog_name = re.sub(r'\s*\([MW]\)\s*', '', dog_name).strip()
        
        # Extract trap number
        trap_element = runner_block.find('i', class_=re.compile(r'trap\d+'))
        trap_number = 'TBC'
        if trap_element:
            trap_classes = trap_element.get('class', [])
            for cls in trap_classes:
                if cls.startswith('trap') and len(cls) > 4:
                    try:
                        trap_number = int(cls.replace('trap', ''))
                        break
                    except:
                        pass
        
        # Extract additional info
        form, forecast, trainer = self._extract_additional_info(runner_block)
        
        return {
            'Track': race_info['track'],
            'Race_Number': race_info['race_number'],
            'Race_Time': race_info['race_time'],
            'Grade': grade,
            'Distance': distance,
            'Trap': trap_number,
            'Dog_Name': dog_name,
            'Form': form,
            'SP_Forecast': forecast,
            'Trainer': trainer
        }
    
    def _extract_additional_info(self, runner_block) -> tuple:
        """Extract form, forecast, and trainer info."""
        form = forecast = trainer = 'N/A'
        
        info_section = runner_block.find('div', class_='info')
        if info_section:
            # Extract form
            form_cell = info_section.find('em', string='Form:')
            if form_cell and form_cell.parent:
                form_text = form_cell.parent.get_text()
                form_match = re.search(r'Form:\s*([A-Z0-9T]+)', form_text)
                if form_match:
                    form = form_match.group(1)
            
            # Extract SP Forecast
            forecast_cell = info_section.find('em', string='SP Forecast:')
            if forecast_cell and forecast_cell.parent:
                forecast_text = forecast_cell.parent.get_text()
                forecast_match = re.search(r'SP Forecast:\s*([0-9/]+)', forecast_text)
                if forecast_match:
                    forecast = forecast_match.group(1)
            
            # Extract trainer
            trainer_cell = info_section.find('em', string='Tnr:')
            if trainer_cell and trainer_cell.parent:
                trainer_text = trainer_cell.parent.get_text()
                trainer_match = re.search(r'Tnr:\s*([A-Za-z\s]+)', trainer_text)
                if trainer_match:
                    trainer = trainer_match.group(1).strip()
        
        return form, forecast, trainer
    
    def _extract_race_grade_and_distance(self, soup) -> tuple:
        """Extract race grade and distance from title."""
        grade = distance = 'N/A'
        
        try:
            title_container = soup.find('span', {'id': 'title-circle-container'})
            if title_container:
                title_text = title_container.get_text()
                
                grade_match = re.search(r'\b([A-Z]\d+)\b', title_text)
                if grade_match:
                    grade = grade_match.group(1)
                
                distance_match = re.search(r'\b(\d+m)\b', title_text)
                if distance_match:
                    distance = distance_match.group(1)
        except Exception as e:
            print(f"Error extracting grade/distance: {e}")
        
        return grade, distance
    
    def _check_for_duplicates(self, all_race_data: List[Dict], runners: List[Dict], race_url: str) -> bool:
        """Check for duplicate dogs indicating cache issues."""
        if len(all_race_data) == 0:
            return False
        
        recent_races_count = min(6, len(all_race_data))
        recent_dogs = {r['Dog_Name'] for r in all_race_data[-recent_races_count:]}
        current_dogs = {r['Dog_Name'] for r in runners}
        overlap = recent_dogs.intersection(current_dogs)
        
        return overlap and len(overlap) > len(current_dogs) * 0.5
    
    def _retry_with_cache_bust(self, race_info: Dict, race_url: str) -> List[Dict]:
        """Retry extraction after cache bust."""
        print(f"    Cache issue detected, applying aggressive cache bust...")
        smart_cache_bust(self.driver, force_aggressive=True)
        
        retry_url = race_url + f"?refresh={int(time.time())}"
        self.driver.get(retry_url)
        time.sleep(5)
        
        runners = self._extract_runners_from_race_card(race_info)
        return runners if runners else []


def extract_todays_races() -> Optional[pd.DataFrame]:
    """
    Main function to extract today's race data.
    
    Returns:
        DataFrame with race data or None if extraction fails
    """
    extractor = RaceCardExtractor()
    return extractor.extract_race_data()


if __name__ == "__main__":
    race_data = extract_todays_races()
    if race_data is not None:
        print("--- Today's Greyhound Race Data ---")
        print(race_data)
        print("-----------------------------------")
        
        # Save to data/raw directory
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"../../../data/raw/race_cards_{today_str}.csv"
        race_data.to_csv(filename, index=False)
        print(f"Race data saved to {filename}")
