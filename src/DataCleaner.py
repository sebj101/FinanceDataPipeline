"""
DataCleaner.py

Contains a data cleaning class which lives in between the download and storage
steps. Identifies missing or anomalous fields in the input DataFrame and gives a
final report on the data quality
"""

import pandas as pd


class DataCleaner:
    """
    Data cleaning class. Handles data cleaning and reporting.
    """

    def __init__(self):
        pass

    def clean_data(self, input_df: pd.DataFrame,
                   return_report: bool = True) -> pd.DataFrame:
        """
        Processes and cleans an input DataFrame of financial data from yfinance

        Parameters
        ----------
        input_df : pd.DataFrame
            Input DataFrame to be cleaned
        return_report : bool
            Boolean for whether to return a dictionary report the data quality 
            (default true)    

        Returns
        -------
        tuple[dict, pd.DataFrame]
            Tuple containing a dictionary detailing data quality and a cleaned 
            DataFram
        """
        if input_df.empty:
            raise ValueError("Received an empty DataFrame as input")

        necessary_columns = {'Open', 'High',
                             'Low', 'Close', 'Volume', 'Adj Close'}
        if not necessary_columns.issubset(input_df.columns):
            raise ValueError(
                f"Inputted DataFrame is missing some required columns: {necessary_columns.difference(input_df.columns)}")

        df = input_df.copy()
        return df
