import datetime as dt
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pytz
from dataclasses import dataclass
import time
import re
from .logging import get_logger

logger = get_logger(__name__)

@dataclass
class NewsEvent:
    source: str
    currency: str
    time: dt.datetime
    name: str
    impact: str = "high"
    actual: Optional[float] = None
    forecast: Optional[float] = None
    previous: Optional[float] = None

class EliteNewsGuard:
    """Enhanced news guard with economic calendar integration"""
    
    def __init__(self, config: dict):
        self.before_minutes = config['blackout_min_before']
        self.after_minutes = config['blackout_min_after']
        self.sources = config['sources']
        self.cache_ttl = dt.timedelta(minutes=config.get('cache_ttl_minutes', 30))
        self._cache: Dict[str, Tuple[dt.datetime, List[NewsEvent]]] = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Gold-specific important events
        self.gold_critical_events = [
            "NFP", "Nonfarm Payrolls", "CPI", "FOMC", "Federal Reserve", "Interest Rate",
            "Inflation", "PCE", "Retail Sales", "GDP", "Unemployment", "Jobs Report",
            "Central Bank", "Monetary Policy", "Gold", "XAU", "Precious Metals"
        ]

    def get_events(self, currencies: List[str]) -> List[NewsEvent]:
        """Get relevant news events for currencies with enhanced filtering"""
        cache_key = dt.date.today().isoformat()
        
        # Check cache
        if cache_key in self._cache:
            cache_time, cached_events = self._cache[cache_key]
            if dt.datetime.now() - cache_time < self.cache_ttl:
                filtered_events = [e for e in cached_events if e.currency in currencies]
                return self._prioritize_events(filtered_events)
        
        # Fetch new data from all sources
        events = []
        today = dt.date.today()
        
        for source in self.sources:
            try:
                source_events = []
                if source == "forexfactory":
                    source_events = self._fetch_forexfactory_enhanced(today)
                elif source == "investing.com":
                    source_events = self._fetch_investing_enhanced(today)
                
                events.extend(source_events)
                logger.debug(f"Fetched {len(source_events)} events from {source}")
                
            except Exception as e:
                logger.error(f"News source {source} failed: {e}")
        
        # Update cache
        self._cache[cache_key] = (dt.datetime.now(), events)
        
        # Filter and prioritize
        filtered_events = [e for e in events if e.currency in currencies]
        return self._prioritize_events(filtered_events)

    def is_blackout(self, currencies: List[str]) -> Tuple[bool, Optional[NewsEvent], int]:
        """Check if we're in a news blackout period with enhanced logic"""
        now = dt.datetime.now(pytz.UTC)
        events = self.get_events(currencies)
        
        for event in events:
            # Extended blackout for critical events
            if self._is_critical_event(event):
                before_minutes = self.before_minutes * 2  # Double for critical events
                after_minutes = self.after_minutes * 1.5
            else:
                before_minutes = self.before_minutes
                after_minutes = self.after_minutes
            
            window_start = event.time - dt.timedelta(minutes=before_minutes)
            window_end = event.time + dt.timedelta(minutes=after_minutes)
            
            if window_start <= now <= window_end:
                minutes_remaining = int((window_end - now).total_seconds() // 60)
                return True, event, max(1, minutes_remaining)
        
        return False, None, 0

    def _fetch_forexfactory_enhanced(self, date: dt.date) -> List[NewsEvent]:
        """Enhanced ForexFactory fetching with better parsing"""
        events = []
        try:
            url = f"https://www.forexfactory.com/calendar?day={date.isoformat()}"
            response = self._fetch_with_retry(url)
            if not response:
                return events

            soup = BeautifulSoup(response.text, "html.parser")
            
            for row in soup.select("tr.calendar_row"):
                try:
                    # Impact level
                    impact_span = row.select_one(".calendar__impact")
                    if not impact_span:
                        continue
                    
                    impact_class = " ".join(impact_span.get("class", []))
                    if "high" not in impact_class:
                        continue

                    # Currency
                    currency_elem = row.select_one(".calendar__currency")
                    currency = currency_elem.get_text(strip=True) if currency_elem else None
                    if not currency:
                        continue

                    # Time
                    time_elem = row.select_one(".calendar__time")
                    time_str = time_elem.get_text(strip=True) if time_elem else ""
                    if not time_str or "all" in time_str.lower():
                        continue

                    # Event name
                    name_elem = row.select_one(".calendar__event-title")
                    name = name_elem.get_text(strip=True) if name_elem else "Economic Event"

                    # Parse time to UTC
                    event_time = self._parse_forexfactory_time(time_str, date)

                    events.append(NewsEvent(
                        source="forexfactory",
                        currency=currency,
                        time=event_time,
                        name=name,
                        impact="high"
                    ))
                    
                except Exception as e:
                    logger.debug(f"Failed to parse ForexFactory row: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"ForexFactory enhanced fetch error: {e}")
            
        return events

    def _fetch_investing_enhanced(self, date: dt.date) -> List[NewsEvent]:
        """Enhanced Investing.com fetching"""
        events = []
        try:
            url = f"https://www.investing.com/economic-calendar/"
            response = self._fetch_with_retry(url)
            if not response:
                return events

            # This is a simplified implementation
            # In production, you would implement full Investing.com parsing
            
        except Exception as e:
            logger.error(f"Investing.com enhanced fetch error: {e}")
            
        return events

    def _parse_forexfactory_time(self, time_str: str, date: dt.date) -> dt.datetime:
        """Parse ForexFactory time to UTC"""
        try:
            if ':' in time_str:
                hours, minutes = map(int, time_str.split(':'))
                # ForexFactory times are in EST
                est = pytz.timezone('US/Eastern')
                naive_dt = dt.datetime(date.year, date.month, date.day, hours, minutes)
                est_dt = est.localize(naive_dt)
                return est_dt.astimezone(pytz.UTC)
            else:
                raise ValueError(f"Invalid time format: {time_str}")
        except Exception as e:
            logger.warning(f"Time parsing failed for '{time_str}': {e}")
            # Fallback to current time
            return dt.datetime.now(pytz.UTC)

    def _is_critical_event(self, event: NewsEvent) -> bool:
        """Check if event is critical for gold trading"""
        event_name = event.name.upper()
        return any(keyword in event_name for keyword in [kw.upper() for kw in self.gold_critical_events])

    def _prioritize_events(self, events: List[NewsEvent]) -> List[NewsEvent]:
        """Prioritize events based on importance for gold"""
        critical_events = [e for e in events if self._is_critical_event(e)]
        other_events = [e for e in events if not self._is_critical_event(e)]
        
        return critical_events + other_events

    def _fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Robust HTTP fetching with retries and headers"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                return response
            except Exception as e:
                logger.warning(f"News fetch attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        return None

# Backward compatibility
NewsGuard = EliteNewsGuard