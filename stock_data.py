import sys
from pykrx import stock
from datetime import datetime
import pandas as pd

# Ensure print statements correctly output Korean characters on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

class StockDataClient:
    """Wrapper around pykrx to fetch Korean stock market data."""
    
    @staticmethod
    def get_stock_name(stock_code: str) -> str:
        """Get the company name for a given 6-digit stock ticker."""
        return stock.get_market_ticker_name(stock_code)

    @staticmethod
    def get_ohlcv(stock_code: str, start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        Fetch High/Low/Close/Volume data for a given ticker and date range.
        Dates should be formatted as 'YYYYMMDD'.
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
            
        # pykrx returns a Pandas DataFrame with DatetimeIndex
        df = stock.get_market_ohlcv(start_date, end_date, stock_code)
        
        # Reset index to make '날짜' (Date) a standard column
        if not df.empty:
            df = df.reset_index()
            # Rename columns to English for easier internal usage and consistency
            # Default columns are usually: 날짜, 시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률
            df = df.rename(columns={
                "날짜": "date",
                "시가": "open",
                "고가": "high",
                "저가": "low",
                "종가": "close",
                "거래량": "volume",
                "등락률": "change_rate"
            })
            
        return df

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """Calculate the Relative Strength Index (RSI) for a given pandas Series."""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        
        # Exponential moving average (EMA) is standard for RSI
        avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

if __name__ == "__main__":
    # Test block
    print("Testing pykrx StockDataClient...")
    
    # 005930 is Samsung Electronics
    ticker = "005930"
    name = StockDataClient.get_stock_name(ticker)
    print(f"Ticker {ticker} belongs to: {name}")
    
    # Fetch data for the last month
    from datetime import timedelta
    end = datetime.now()
    start = end - timedelta(days=30)
    
    df = StockDataClient.get_ohlcv(
        ticker, 
        start_date=start.strftime("%Y%m%d"), 
        end_date=end.strftime("%Y%m%d")
    )
    
    print("\nRecent Price Data:")
    print(df.tail())
