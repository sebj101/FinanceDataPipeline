"""
DataCleaner.py

Contains a data cleaning class which lives in between the download and storage
steps. Identifies missing or anomalous fields in the input DataFrame and gives a
final report on the data quality
"""

import pandas as pd
import logging
import numpy as np
import pandas_market_calendars as mcal
from yfinance import Ticker
import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Data cleaning class. Handles data cleaning and reporting.
    """

    def __init__(self, input_df: pd.DataFrame, ticker: str,
                 start: datetime.date, end: datetime.date, trading_cal: mcal.MarketCalendar,
                 anomaly_threshold: float = 0.5):
        """
        Constructor for DataCleaner

        Parameters
        ----------
        input_df : pd.DataFrame
            Input DataFrame to be cleaned
        ticker : str
            String representing the ticker
        start : datetime.date
            Start date from which we are interested in cleaning
        end : datetime.date
            End date (exclusive) up to which we are interested in cleaning
        trading_cal : mcal.MarketCalendar
            Trading calendar for day checking
        anomaly_threshold : float
            Fractional price movement threshold required to detect an anomaly
        """
        self.cleaning_report = {}
        self._anomaly_thresh = anomaly_threshold
        self._necessary_columns = {'Open', 'High',
                                   'Low', 'Close', 'Volume', 'Adj Close'}
        self._trading_cal = trading_cal

        if input_df.empty:
            raise ValueError("Received an empty DataFrame as input")

        if not self._necessary_columns.issubset(input_df.columns):
            raise ValueError("Inputted DataFrame is missing some required columns: "
                             f"{self._necessary_columns.difference(input_df.columns)}")

        # Validate start and end dates
        if end <= start:
            raise ValueError("End date must be greater than start date")

        # Check the inputted dates bracket the data. It's fine if they are
        # larger (for example due to closures) but they may not be smaller
        self._df = input_df.copy()
        self._df['date'] = self._df.index.date
        if self._df['date'].min() < start:
            raise ValueError(
                "The DataFrame data starts before the inputted start date.")
        elif self._df['date'].max() >= end:
            raise ValueError(
                "The DataFrame data ends on or after the inputted start date.")

        self._start = start
        self._end = end
        self._ticker = ticker
        self._cleaning_run = False

    def _flag_invalid_entries(self):
        """
        Scans through the data and look for any invalid prices, e.g. 0 or
        negative. Converts these to NaNs to be counted and removed
        """
        check_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
        for col in check_columns:
            self._df.loc[self._df[col] <= 0.0, col] = np.nan

        self._df.loc[self._df['Volume'] < 0.0, 'Volume'] = np.nan
        self.cleaning_report['zero_volume_days'] = len(
            self._df.loc[self._df['Volume'] == 0])

    def _count_missing_trading_days(self) -> int:
        """
        Count the number of valid trading days which should be in the data
        """
        valid_trading_dates = set(
            self._trading_cal.valid_days(self._start, self._end).date)
        dataset_dates = set(self._df['date'])
        self._missing_dates = valid_trading_dates.difference(dataset_dates)
        logger.info(
            f"Found {len(self._missing_dates):d} missing trading days in the DataFrame")
        return len(self._missing_dates)

    def _remove_invalid_entries(self):
        """
        Removes any missing days with invalid entries from an input DataFrame
        """
        initial_rows = len(self._df)
        self._df = self._df.dropna()
        self.cleaning_report['rows_removed'] = initial_rows - \
            len(self._df.dropna())

        total_rows = len(self._df)
        for col, count in self._df.isna().sum().items():
            self.cleaning_report[col] = count
            if count > 0:
                logger.warning(
                    f"Column {col} has {count:d} NaNs out of {total_rows:d} total")

    def clean_data(self):
        """
        Processes and cleans the input DataFrame of financial data from yfinance
        """
        self.cleaning_report['missing_days'] = self._count_missing_trading_days(
        )
        self._flag_invalid_entries()
        self._remove_invalid_entries()
        self._cleaning_run = True

    def get_dataframe(self) -> pd.DataFrame:
        """
        Returns the DataFrame. After running clean_data, this will be clean

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame
        """
        if not self._cleaning_run:
            logger.warning(
                "Cleaning has not been run yet. You will just get back the original dataset")
        return self._df
