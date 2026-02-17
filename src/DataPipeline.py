"""
DataPipeline.py

Contains DataPipeline class - a data pipeline for quant finance analysis 
"""

import sqlite3
import yfinance as yf
import pandas as pd
import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Data pipeline class. Handles data collection, cleaning, storage and analysis
    in an SQLite database
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        self._setup_database()

        self.assets = {
            'equities': ['AAPL', 'MSFT', 'GOOGL'],
            'equity ETFs': ['SPY', 'QQQ', 'IWM', 'VTI'],
            'bonds': ['TLT', 'IEF', 'TIP', 'SHY', 'HYG', 'LQD',
                      'IGLT.L', 'SDEU.L', 'JGBZX'],
            'commodities': ['GLD', 'SLV', 'USO', 'DBA', 'PDBC'],
            'currencies': ['UUP', 'FXE', 'FXY', 'FXA'],
            'volatility': ['^VIX']
        }

    def _setup_database(self):
        """
        Initialise SQLite database
        """

        self.connection = sqlite3.connect(self.db_path)
        cursor = self.connection.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_data (
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                adjusted_close REAL,
                asset_class TEXT,
                PRIMARY KEY (ticker, date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS returns_data (
                ticker TEXT NOT NULL,
                date DATE NOT NULL,
                return_1d REAL,
                return_5d REAL,
                return_21d REAL,
                asset_class TEXT,
                PRIMARY KEY (ticker, date)
            )
        """)

        self.connection.commit()
        logger.info(f"Database successfully initialised at {self.db_path}")

    def close_connection(self):
        """Close the connection to the database"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")

    def download_data(self, ticker: str, start: datetime.date,
                      end: datetime.date) -> Optional[pd.DataFrame]:
        """
        Downloads the data for a given date range and ticker

        Parameters
        ----------

        ticker : str 
            Ticker to download data for
        start : datetime.date
            Start date to download data for (inclusive)
        end : datetime.date 
            End date (exclusive) to download data for

        Returns
        -------
        Optional[pd.DataFrame]
            Dataframe of OHLCV data or None if download fails
        """
        # Validate start and end dates
        if end <= start:
            raise ValueError("End date must be greater than start date")

        try:
            tk = yf.Ticker(ticker)
            df = tk.history(start=start, end=end, auto_adjust=False)
        except ValueError:
            logger.error(f"Invalid ticker symbol {ticker} passed to function")
            return None
        except Exception:
            logger.error("Error in getting ticker history from yfinance")
            return None

        if df.empty == True:
            return None
        else:
            return df

    def store_price_data(self, ticker: str, dataframe: pd.DataFrame,
                         asset_class: str):
        """
        Stores the downloaded price data in the price_data SQL table

        Parameters
        ----------
        ticker : str
            The stock ticker we are adding for
        dataframe : pd.DataFrame
            DataFrame containing the data being written
        asset_class : str
            Asset class of the data
        """
        # Add rows where needed
        df = dataframe.copy()
        df['asset_class'] = asset_class
        df['ticker'] = ticker
        df['date'] = dataframe.index.date

        ordered_df = df[['ticker',
                         'date',
                         'Open',
                         'High',
                         'Low',
                         'Close',
                         'Volume',
                         'Adj Close',
                         'asset_class']]
        formatted_data = ordered_df.to_records(index=False)

        cursor = self.connection.cursor()

        cursor.executemany("""
            INSERT OR REPLACE INTO price_data (ticker, date, open, high, low, close, volume, adjusted_close, asset_class)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, formatted_data)
        self.connection.commit()

    def calculate_and_store_returns(self, ticker: str, dataframe: pd.DataFrame,
                                    asset_class: str):
        """
        Using the DataFrame for a given ticker, computes the 1d/5d/21d returns 
        and updates returns_table accordingly

        Parameters
        ----------
        ticker : str
            Ticker
        dataframe : pd.DataFrame
            Dataframe containing the historical stock data
        asset_class : str
            Asset class of the data
        """
        df = dataframe.copy()
        df['asset_class'] = asset_class
        df['ticker'] = ticker
        df['date'] = dataframe.index.date

        # This will create NaNs when there is not enough data
        df['return_1d'] = dataframe['Adj Close'].pct_change(periods=1)
        df['return_5d'] = dataframe['Adj Close'].pct_change(periods=5)
        df['return_21d'] = dataframe['Adj Close'].pct_change(periods=21)

        ordered_df = df[['ticker',
                         'date',
                         'return_1d',
                         'return_5d',
                         'return_21d',
                         'asset_class']]
        formatted_data = ordered_df.to_records(index=False)

        cursor = self.connection.cursor()

        cursor.executemany("""
            INSERT OR REPLACE INTO returns_data (ticker, date, return_1d, return_5d, return_21d, asset_class)
            VALUES (?, ?, ?, ?, ?, ?)
        """, formatted_data)
        self.connection.commit()

    def build_database(self, start: datetime.date, end: datetime.date):
        """
        Populates SQL database containing price_table and returns_table

        Parameters
        ----------
        start : datetime.date
            Start date to get data for (inclusive)
        end: datetime.date
            End date to get data for (exclusive)
        """
        for asset_class in self.assets.keys():
            for ticker in self.assets[asset_class]:
                df = self.download_data(ticker, start, end)
                if df is not None:
                    self.store_price_data(ticker, df, asset_class)
                    self.calculate_and_store_returns(ticker, df, asset_class)
