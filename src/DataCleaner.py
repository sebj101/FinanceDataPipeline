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
import datetime

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Data cleaning class. Handles data cleaning and reporting.
    """

    def __init__(self, input_df: pd.DataFrame,
                 start: datetime.date, end: datetime.date, trading_cal: mcal.MarketCalendar,
                 anomaly_threshold: float = 0.5):
        """
        Constructor for DataCleaner

        Parameters
        ----------
        input_df : pd.DataFrame
            Input DataFrame to be cleaned
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

    def _flag_inconsistent_ohlc(self):
        """
        Looks for cases where the relationship between entries in a row are 
        anomalous and flag
        """
        self._df = self._df.assign(Inconsistent=False)
        self._df.loc[self._df['Low'] > self._df['High'], 'Inconsistent'] = True
        self._df.loc[self._df['Open'] >
                     self._df['High'], 'Inconsistent'] = True
        self._df.loc[self._df['Close'] >
                     self._df['High'], 'Inconsistent'] = True

        self._df.loc[self._df['Open'] < self._df['Low'], 'Inconsistent'] = True
        self._df.loc[self._df['Close'] <
                     self._df['Low'], 'Inconsistent'] = True

        n_ohlc_inconsistencies = len(self._df.loc[self._df['Inconsistent']])
        self.cleaning_report['ohlc_inconsistencies'] = n_ohlc_inconsistencies
        if n_ohlc_inconsistencies > 0:
            logger.warning("rows had invalid prices")

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

    def _flag_anomalous_moves(self):
        """
        Looks for moves in the closing price over a pre-defined threshold and 
        flags them in the DataFrame via a new column
        """
        pct_change_series = self._df['Adj Close'].pct_change(
            periods=1, fill_method=None)
        self._df['Anomalous'] = abs(pct_change_series) > self._anomaly_thresh
        # We will clean out NaNs or later on so for now, if we are comparing a
        # value with a NaN then state it's not an anomaly
        self._df.loc[pct_change_series.isna(), 'Anomalous'] = False

        n_anomalies = len(self._df.loc[self._df['Anomalous']])
        self.cleaning_report['anomalous_days'] = n_anomalies
        logger.info(f"Found {n_anomalies} anomalous days")

    def _remove_invalid_entries(self):
        """
        Removes any missing days with invalid entries from an input DataFrame
        """
        initial_rows = len(self._df)
        for col, count in self._df.isna().sum().items():
            self.cleaning_report[col] = count
            if count > 0:
                logger.warning(
                    f"Column {col} has {count:d} NaNs out of {initial_rows:d} total")

        self.cleaning_report['rows_removed'] = initial_rows - \
            len(self._df.dropna())
        self._df = self._df.dropna()

    def clean_data(self):
        """
        Processes and cleans the input DataFrame of financial data from yfinance
        """
        self.cleaning_report['missing_days'] = self._count_missing_trading_days(
        )
        self._flag_invalid_entries()
        self._flag_anomalous_moves()
        self._flag_inconsistent_ohlc()
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
