"""
DataPipeline.py

Contains DataPipeline class - a data pipeline for quant finance analysis 
"""

import sqlite3


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
            'bonds': ['TIP', 'IGLT', 'SDEU', 'JGB1X'],
            'commodities': ['GLD', 'SLV', 'USO'],
            'currencies': ['UUP', 'FXE'],
            'volatility': ['VIX']
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
                low, REAL,
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
        print(f"Database successfully initialised at {self.db_path}")

    def close_connection(self):
        """Close the connection to the database"""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
