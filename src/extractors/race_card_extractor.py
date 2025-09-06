"""
Race card extractor module for greyhound racing data.
Handles extraction of race cards from Racing Post website with smart caching.
"""
from __future__ import annotations

import time
import os
import re
from datetime import datetime
from typing import List, Dict, Optional
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore
"""Note: heavy dependencies (selenium, bs4) are imported lazily to allow
importing this module in environments without those packages for light tests."""

# Defer heavy imports to runtime in methods
BeautifulSoup = None  # type: ignore
webdriver = None  # type: ignore
Service = None  # type: ignore
Options = None  # type: ignore
By = None  # type: ignore
WebDriverWait = None  # type: ignore
EC = None  # type: ignore
ChromeDriverManager = None  # type: ignore

# Utilities imported lazily where needed
# from ..utils.browser_utils import setup_chrome_options, smart_cache_bust
# from ..utils.file_utils import save_to_csv


class RaceCardExtractor:
    """Main class for extracting race card data from Racing Post.

    Supports two modes:
      - Today: scrape today's race cards from the homepage meetings list
      - Historical: scrape past race results/cards for a given date or date range
    """
    
    def __init__(self, base_url: str = "https://greyhoundbet.racingpost.com/"):
        self.base_url = base_url.rstrip("/") + "/"
        self.driver = None
        
    def extract_race_data(
        self,
        mode: str = "today",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[object]:
        """
        Extract race data.
        
        Args:
            mode: "today" or "historical"
            start_date: inclusive start date (YYYY-MM-DD) for historical mode; if None and mode==historical, uses end_date
            end_date: inclusive end date (YYYY-MM-DD) for historical mode; if None and mode==historical, uses start_date
        
        Returns:
            DataFrame with race data or None if extraction fails
        """
        try:
            self._setup_driver()

            if mode == "historical":
                if not start_date and not end_date:
                    raise ValueError("historical mode requires start_date and/or end_date (YYYY-MM-DD)")
                # Normalize dates
                if start_date and not end_date:
                    end_date = start_date
                if end_date and not start_date:
                    start_date = end_date
                print(f"Loading Racing Post historical results from {start_date} to {end_date}...")
                all_race_data: List[Dict] = []
                for date_str in self._iter_dates_inclusive(start_date, end_date):
                    day_data = self._extract_for_single_date(date_str)
                    all_race_data.extend(day_data)
                if pd is None:
                    raise ImportError("pandas is required to build the output DataFrame. Install requirements.txt")
                df = pd.DataFrame(all_race_data)
                print(f"Extracted {len(df)} race entries across {len(list(self._iter_dates_inclusive(start_date, end_date)))} day(s)")
                return df

            # Default: today's races
            print("Loading Racing Post website (today's races)...")
            self.driver.get(self.base_url)
            time.sleep(2)
            
            # Extract race card URLs
            race_card_urls = self._extract_race_card_urls()
            
            # Extract detailed race information
            all_race_data = self._extract_from_race_cards(race_card_urls)
            
            # Create DataFrame
            if pd is None:
                raise ImportError("pandas is required to build the output DataFrame. Install requirements.txt")
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

    # ---------------- Historical helpers ----------------
    def _iter_dates_inclusive(self, start_date: str, end_date: str):
        from datetime import datetime, timedelta
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        if end < start:
            start, end = end, start
        cur = start
        while cur <= end:
            yield cur.strftime("%Y-%m-%d")
            cur += timedelta(days=1)

    def _extract_for_single_date(self, date_str: str) -> List[Dict]:
        """Extract all races for a specific historical date."""
        try:
            results_url = f"{self.base_url}#results-list/r_date={date_str}"
            print(f"Navigating to results page: {results_url}")
            self.driver.get(results_url)
            time.sleep(2)
            # Wait up to ~12s for meeting links to render in SPA
            try:
                WebDriverWait(self.driver, 12).until(
                    lambda d: len(d.find_elements(
                        By.CSS_SELECTOR,
                        'a.results-race-name[href*="#result-meeting/"]'
                    )) > 0
                )
            except Exception:
                pass

            meeting_links = self._extract_results_meeting_links()
            print(f"Found {len(meeting_links)} results meetings for {date_str}")

            race_card_urls: List[Dict] = []
            for meeting in meeting_links:
                try:
                    print(f"Processing results meeting: {meeting['track']}")
                    self.driver.get(meeting['url'])
                    time.sleep(2)
                    race_urls = self._extract_race_urls_from_results_meeting(meeting['track'])
                    race_card_urls.extend(race_urls)
                except Exception as e:
                    print(f"Error processing results meeting {meeting.get('track','?')}: {e}")
                    continue

            print(f"Found {len(race_card_urls)} race cards for {date_str}")
            return self._extract_from_race_cards(race_card_urls)
        except Exception as e:
            print(f"Error extracting for date {date_str}: {e}")
            return []
    
    def _setup_driver(self):
        """Setup Chrome driver with optimized options."""
        self._lazy_imports()
        from ..utils.browser_utils import setup_chrome_options
        chrome_options = setup_chrome_options()
        # Prefer system-installed chromedriver (e.g., from apt) with optional CHROMEDRIVER override
        driver_path = os.environ.get("CHROMEDRIVER")
        if not driver_path:
            for p in [
                "/usr/bin/chromedriver",
                "/usr/local/bin/chromedriver",
            ]:
                if os.path.exists(p):
                    driver_path = p
                    break
        if driver_path and os.path.exists(driver_path):
            service = Service(driver_path)
        else:
            # Fallback to webdriver-manager auto install
            service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # Disable automation detection
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    def _extract_race_card_urls(self) -> List[Dict]:
        """Extract all race card URLs from meetings."""
        race_card_urls: List[Dict] = []
        self._ensure_bs4()
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

    def _extract_results_meeting_links(self) -> List[Dict]:
        """Extract meeting links from a results list page for a specific date."""
        links: List[Dict] = []
        self._ensure_bs4()
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        # Example: <a class="results-race-name" href="#result-meeting/track_id=5&r_date=YYYY-MM-DD&r_time=HH:MM">Hove</a>
        meeting_anchors = soup.find_all(
            'a',
            href=lambda x: x and ('#result-meeting/' in x)
        )
        processed = set()
        for a in meeting_anchors:
            try:
                track_el = a.find('h4') or a.find('strong') or a
                track_name = track_el.get_text().strip().split('\n')[0].strip()
                href = a.get('href', '')
                if not href or track_name in processed:
                    continue
                processed.add(track_name)
                full_url = f"{self.base_url}{href}" if not href.startswith('http') else href
                links.append({'track': track_name, 'url': full_url})
            except Exception:
                continue
        return links
    
    def _extract_race_urls_from_meeting(self, track_name: str) -> List[Dict]:
        """Extract race URLs from current meeting page."""
        race_urls: List[Dict] = []
        self._ensure_bs4()
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

    def _extract_race_urls_from_results_meeting(self, track_name: str) -> List[Dict]:
        """Extract race URLs from a results meeting page."""
        race_urls: List[Dict] = []
        # Wait for race links to load (result/card/race anchors)
        try:
            WebDriverWait(self.driver, 12).until(
                lambda d: len(d.find_elements(
                    By.CSS_SELECTOR,
                    'a[href*="#result-meeting-result/"], a[href*="#card/"]'
                )) > 0
            )
        except Exception:
            pass
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        # Example race anchor: <a href="#result-meeting-result/race_id=...&track_id=...&r_date=YYYY-MM-DD&r_time=HH:MM">11:01</a>
        race_links = soup.find_all('a', href=lambda x: x and ('#result-meeting-result/' in x or '#card/' in x))
        for race_link in race_links:
            href = race_link.get('href', '')
            if not href:
                continue
            info = self._extract_race_info_from_link(race_link)
            # Fallback for results pages: use anchor text as time if missing
            if (not info or not info.get('time')):
                anchor_text_time = race_link.get_text(strip=True)
                info = info or {}
                if anchor_text_time:
                    info['time'] = anchor_text_time
            race_urls.append({
                'url': href,
                'track': track_name,
                'race_time': info.get('time', 'TBC') if info else 'TBC',
                'race_number': info.get('number', 'TBC') if info else 'TBC',
            })
        print(f"Found {len(race_urls)} races for {track_name} (results)")
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
        from ..utils.browser_utils import smart_cache_bust
        self._lazy_imports()
        all_race_data: List[Dict] = []
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
                
                # Quick content verification - support both card and results pages
                try:
                    WebDriverWait(self.driver, 6).until(
                        lambda d: d.find_elements(By.ID, "sortContainer") or 
                                  d.find_elements(By.CSS_SELECTOR, "div.container a.details")
                    )
                except:
                    print(f"    Content not loaded, refreshing...")
                    self.driver.refresh()
                    time.sleep(2)
                
                # Decide extractor based on page content
                self._ensure_bs4()
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                if self._is_results_page(soup) or '#result-' in race_url:
                    runners = self._extract_runners_from_result_page(race_info)
                else:
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

    def _is_results_page(self, soup) -> bool:
        """Detect if current page is a results page (not a card)."""
        if soup.find('div', class_='result-dog-name-details'):
            return True
        if soup.select('div.container a.details'):
            return True
        # Fallback: anchors starting with results-dog
        if soup.find('a', href=lambda x: x and 'results-dog' in x):
            return True
        return False

    def _extract_runners_from_result_page(self, race_info: Dict) -> List[Dict]:
        """Extract runners from a historical results page.
        Expects DOM like:
          <div class="container">
            <a class="details" href="#results-dog/...">
              <div class="result"> ... <div class="bigTrap trap1"></div> ...
                <div class="result-dog-name-details"><div class="name">First Chosen</div> ...
                <div class="cols dog-cols"><div class="col col2">SP</div><div class="col col3">... Trainer: Name</div>
        """
        runners: List[Dict] = []
        try:
            self._ensure_bs4()
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            # Ensure Race_Time is populated from results page controls
            try:
                extracted_time = self._extract_time_from_results_page(soup)
                if extracted_time and (not race_info.get('race_time') or race_info.get('race_time') == 'TBC'):
                    race_info['race_time'] = extracted_time
            except Exception:
                pass
            grade, distance = self._extract_race_grade_and_distance(soup)
            # Try to enrich race number from the title e.g. "Race 9 €240 (A4) 480m"
            race_no = race_info.get('race_number', 'TBC')
            title_el = soup.select_one('span#circle-race-title')
            if title_el and (race_no == 'TBC' or not race_no):
                m = re.search(r'Race\s+(\d+)', title_el.get_text(" ", strip=True))
                if m:
                    race_info['race_number'] = m.group(1)

            entries = soup.select('div.container a.details')
            print(f"Found {len(entries)} result entries (Grade: {grade}, Distance: {distance})")
            for entry in entries:
                try:
                    # Trap number from class 'bigTrap trapX'
                    trap_div = entry.select_one('div.bigTrap')
                    trap_number = 'TBC'
                    if trap_div:
                        classes = trap_div.get('class', [])
                        for cls in classes:
                            if cls.startswith('trap') and len(cls) > 4:
                                try:
                                    trap_number = int(cls.replace('trap', ''))
                                    break
                                except Exception:
                                    pass

                    # Dog name
                    name_el = entry.select_one('div.result-dog-name-details div.name')
                    dog_name = name_el.get_text(strip=True) if name_el else 'Unknown'

                    # SP Forecast from col2
                    sp_el = entry.select_one('div.cols.dog-cols div.col.col2')
                    forecast = sp_el.get_text(strip=True) if sp_el else 'N/A'

                    # Trainer from col3 (look for after ':' or last token)
                    trainer = 'N/A'
                    tr_el = entry.select_one('div.cols.dog-cols div.col.col3')
                    if tr_el:
                        txt = tr_el.get_text(" ", strip=True)
                        m = re.search(r"(?:Trainer:|T:)\s*(.+)$", txt)
                        if m:
                            trainer = m.group(1).strip()
                        else:
                            # fallback: last words
                            parts = txt.split()
                            if parts:
                                trainer = parts[-1]

                    runners.append({
                        'Track': race_info['track'],
                        'Race_Number': race_info.get('race_number', 'TBC'),
                        'Race_Time': race_info.get('race_time', 'TBC'),
                        'Grade': grade,
                        'Distance': distance,
                        'Trap': trap_number,
                        'Dog_Name': dog_name,
                        'Form': 'N/A',
                        'SP_Forecast': forecast,
                        'Trainer': trainer,
                    })
                except Exception as e:
                    print(f"Error processing result entry: {e}")
                    continue
        except Exception as e:
            print(f"Error extracting results runners: {e}")
        return runners

    def _extract_time_from_results_page(self, soup) -> Optional[str]:
        """Extract race time from a results page.
        Prefers the pager header h3#pagerResultTime, falls back to r_time in URL fragment.
        """
        # Primary: <h3 id="pagerResultTime">11:01</h3>
        time_el = soup.select_one('h3#pagerResultTime')
        if time_el:
            text = time_el.get_text(strip=True)
            if re.match(r"^\d{1,2}:\d{2}$", text):
                return text
        # Fallback: r_time param in URL fragment (might be percent-encoded)
        try:
            from urllib.parse import unquote
            url = self.driver.current_url
            # Look for r_time=HH%3AMM or HH:MM
            m = re.search(r"[#&?]r_time=([^&#]+)", url)
            if m:
                val = unquote(m.group(1))
                # Validate format
                if re.match(r"^\d{1,2}:\d{2}$", val):
                    return val
        except Exception:
            pass
        return None
    
    def _extract_runners_from_race_card(self, race_info: Dict) -> List[Dict]:
        """Extract all runners from a race card page."""
        runners = []
        
        try:
            self._ensure_bs4()
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

    # --- Lazy import helpers -------------------------------------------------
    def _lazy_imports(self):  # pragma: no cover
        global webdriver, Service, By, WebDriverWait, EC, ChromeDriverManager
        if webdriver is None:
            from selenium import webdriver as _wd
            webdriver = _wd
        if Service is None:
            from selenium.webdriver.chrome.service import Service as _Service
            Service = _Service
        if By is None:
            from selenium.webdriver.common.by import By as _By
            By = _By
        if WebDriverWait is None:
            from selenium.webdriver.support.ui import WebDriverWait as _Wait
            WebDriverWait = _Wait
        if EC is None:
            from selenium.webdriver.support import expected_conditions as _EC
            EC = _EC
        if ChromeDriverManager is None:
            from webdriver_manager.chrome import ChromeDriverManager as _CDM
            ChromeDriverManager = _CDM

    def _ensure_bs4(self):  # pragma: no cover
        global BeautifulSoup
        if BeautifulSoup is None:
            from bs4 import BeautifulSoup as _BS
            BeautifulSoup = _BS
    
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

        def parse_grade_and_distance(text: str) -> tuple:
            g = d = 'N/A'
            try:
                # Grade in parentheses e.g. (A4) or bare token like A4, D2, S1
                m_g = re.search(r'\(([A-Z]\d{1,2})\)', text)
                if not m_g:
                    m_g = re.search(r'\b([A-Z]\d{1,2})\b', text)
                if m_g:
                    g = m_g.group(1)

                # Distance tokens like 480m, 285m, 525y, 525yd, 525yds
                m_d = re.search(r'\b(\d{2,4})\s*(m|M|yds?|YDS?|y)\b', text)
                if m_d:
                    unit = m_d.group(2)
                    # Normalize unit to site style ('m' or 'yds'/'y')
                    if unit.lower().startswith('m'):
                        d = f"{m_d.group(1)}m"
                    elif unit.lower() in {'y', 'yd', 'yds'}:
                        # Keep as appears (common on Irish distances)
                        d = f"{m_d.group(1)}y"
                else:
                    # Last resort: 3-4 digits followed by optional m without word boundary issues
                    m_d2 = re.search(r'(\d{2,4})\s*m\b', text, re.IGNORECASE)
                    if m_d2:
                        d = f"{m_d2.group(1)}m"
            except Exception:
                pass
            return g, d

        try:
            # 1) Prefer results page title
            title_el = soup.select_one('span#circle-race-title')
            if title_el:
                t = title_el.get_text(" ", strip=True)
                g, d = parse_grade_and_distance(t)
                grade = g if g != 'N/A' else grade
                distance = d if d != 'N/A' else distance

            # 2) Card page specific: parse from titleColumn2 if available
            if grade == 'N/A' or distance == 'N/A':
                col2 = soup.select_one('span#title-circle-container span.titleColumn2')
                if col2:
                    t2 = col2.get_text(" ", strip=True)
                    g2, d2 = parse_grade_and_distance(t2)
                    if grade == 'N/A' and g2 != 'N/A':
                        grade = g2
                    if distance == 'N/A' and d2 != 'N/A':
                        distance = d2

            # 3) Fallback: whole card title container text
            if grade == 'N/A' or distance == 'N/A':
                container = soup.find('span', {'id': 'title-circle-container'})
                if container:
                    t3 = container.get_text(" ", strip=True)
                    g3, d3 = parse_grade_and_distance(t3)
                    if grade == 'N/A' and g3 != 'N/A':
                        grade = g3
                    if distance == 'N/A' and d3 != 'N/A':
                        distance = d3
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
        from ..utils.browser_utils import smart_cache_bust
        smart_cache_bust(self.driver, force_aggressive=True)
        
        retry_url = race_url + f"?refresh={int(time.time())}"
        self.driver.get(retry_url)
        time.sleep(5)
        
        runners = self._extract_runners_from_race_card(race_info)
        return runners if runners else []


def extract_todays_races() -> Optional[object]:
    """Extract only today's race cards."""
    extractor = RaceCardExtractor()
    return extractor.extract_race_data(mode="today")


def extract_historical_races(start_date: str, end_date: Optional[str] = None) -> Optional[object]:
    """Extract historical race cards for a date or inclusive date range.

    Args:
        start_date: YYYY-MM-DD inclusive start date
        end_date: YYYY-MM-DD inclusive end date (defaults to start_date if None)
    """
    extractor = RaceCardExtractor()
    return extractor.extract_race_data(mode="historical", start_date=start_date, end_date=end_date)


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
