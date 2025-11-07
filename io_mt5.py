import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from typing import Optional, Dict
import time
from .logging import get_logger

logger = get_logger(__name__)

class MT5Client:
    """Elite MT5 client with institutional-grade data handling"""
    
    def __init__(self, max_retries: int = 5):
        self.max_retries = max_retries
        self._init_connection()
    
    def _init_connection(self):
        """Initialize MT5 with robust connection handling"""
        for attempt in range(self.max_retries):
            try:
                if not mt5.initialize():
                    error_msg = mt5.last_error()
                    raise RuntimeError(f"MT5 initialization failed: {error_msg}")
                
                # Verify connection
                account_info = mt5.account_info()
                if account_info is None:
                    raise RuntimeError("MT5 account info unavailable")
                
                logger.info(
                    "MT5 connected successfully",
                    extra={
                        "server": mt5.terminal_info().server,
                        "account": account_info.login,
                        "balance": account_info.balance
                    }
                )
                return
                
            except Exception as e:
                logger.warning(
                    f"MT5 connection attempt {attempt+1} failed",
                    extra={"error": str(e)}
                )
                if attempt == self.max_retries - 1:
                    logger.critical("Failed to connect to MT5 after all retries")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def get_rates(self, symbol: str, timeframe: int, count: int) -> Optional[pd.DataFrame]:
        """Get OHLC data with elite error handling and validation"""
        for attempt in range(self.max_retries):
            try:
                rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
                if rates is None:
                    raise ValueError(f"No data returned for {symbol} {timeframe}")
                
                if len(rates) < count * 0.7:  # 70% data quality threshold
                    raise ValueError(f"Insufficient data: {len(rates)}/{count} bars")
                
                # Convert to DataFrame with elite formatting
                df = pd.DataFrame(rates)
                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                
                # Elite column naming
                df.rename(columns={
                    'open': 'o', 'high': 'h', 'low': 'l', 
                    'close': 'c', 'tick_volume': 'v',
                    'real_volume': 'rv', 'spread': 's'
                }, inplace=True)
                
                # Data quality checks
                self._validate_data_quality(df, symbol)
                
                logger.debug(
                    f"Data fetched successfully for {symbol}",
                    extra={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "bars": len(df),
                        "date_range": f"{df['time'].min()} to {df['time'].max()}"
                    }
                )
                
                return df[['time', 'o', 'h', 'l', 'c', 'v', 'rv', 's']].dropna()
                
            except Exception as e:
                logger.warning(
                    f"Rates fetch attempt {attempt+1} failed for {symbol}",
                    extra={"error": str(e), "symbol": symbol}
                )
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(
                        f"Failed to get rates for {symbol} after all retries",
                        extra={"symbol": symbol}
                    )
                    return None
        
        return None
    
    def _validate_data_quality(self, df: pd.DataFrame, symbol: str):
        """Validate data quality for institutional trading"""
        # Check for gaps
        time_diff = df['time'].diff().dt.total_seconds()
        max_gap = time_diff.max()
        if max_gap > 3600 * 4:  # 4 hour gap
            logger.warning(f"Large time gap detected in {symbol}: {max_gap/3600:.1f}h")
        
        # Check for outliers
        price_changes = df['c'].pct_change().abs()
        outlier_threshold = price_changes.quantile(0.99)
        outliers = price_changes[price_changes > outlier_threshold]
        
        if len(outliers) > 0:
            logger.warning(
                f"Price outliers detected in {symbol}",
                extra={"outliers_count": len(outliers), "symbol": symbol}
            )
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict]:
        """Get detailed symbol information"""
        try:
            info = mt5.symbol_info(symbol)
            if info:
                return {
                    'name': info.name,
                    'point': info.point,
                    'digits': info.digits,
                    'spread': info.spread,
                    'trade_mode': info.trade_mode,
                    'swap_mode': info.swap_mode,
                    'margin_initial': info.margin_initial
                }
        except Exception as e:
            logger.error(f"Failed to get symbol info for {symbol}: {e}")
        return None

    # MT5 Timeframe Constants
    TIMEFRAME_M1 = mt5.TIMEFRAME_M1
    TIMEFRAME_M5 = mt5.TIMEFRAME_M5
    TIMEFRAME_M15 = mt5.TIMEFRAME_M15
    TIMEFRAME_H1 = mt5.TIMEFRAME_H1
    TIMEFRAME_H4 = mt5.TIMEFRAME_H4
    TIMEFRAME_D1 = mt5.TIMEFRAME_D1
    TIMEFRAME_W1 = mt5.TIMEFRAME_W1
    TIMEFRAME_MN1 = mt5.TIMEFRAME_MN1

    def __del__(self):
        """Cleanup MT5 connection"""
        try:
            mt5.shutdown()
        except:
            pass