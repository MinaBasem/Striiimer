import time
import random
import pandas as pd
from datetime import datetime
from typing import Literal
from enum import Enum
import traceback
import striiimer
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

#from connector_functions import postgres_connector, mysql_connector
#from connectors.postgres_connector import postgres_sender
#from .connectors import postgres_sender, connectors
#from .postgres_connector import postgres_sender
#from .striiimer_connector import connector

# Add batching
# Add connectors
# Add different types of random wait types
# Add a method for different log options


class TimeSetting(Enum):
    FIXED = "fixed"
    VARIABLE = "variable"
    
    @classmethod
    def values(cls):
        return [member.value for member in cls]

class Striiimer:
    def __init__(self, df, frequency, timeSetting="fixed"):
        """
        Initialize the simulator with a dataframe and frequency.
        
        Args:
            df (pandas.DataFrame): Input data for streaming
            frequency (int): Frequency parameter for the streaming
            timeSetting (str): Streaming option, must be "fixed" or "variable"

        Raises:
            TypeError: If df is not a pandas DataFrame
            ValueError: If df is empty or frequency is invalid
        """

        self._validate_inputs(df, frequency, timeSetting)
        self.df = df.copy()             # to avoid modifying original df
        self.frequency = frequency
        self.timeSetting = timeSetting.lower()
        #self.connector = lambda db_type, **kwargs: connector(self, db_type, **kwargs)
        self.engine_name = None
        self.engine = None
        self.db_type = None
        self.results = None
        
    def _validate_inputs(self, df, frequency, timeSetting):
        """Validate input parameters before streaming."""

        # DataFrame check
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Input must be a pandas DataFrame, got {type(df).__name__} instead"
            )
        
        # Empty DataFrame check
        if df.empty:
            raise ValueError("Input DataFrame cannot be empty")

        # checks whether frequency is valid
        if not isinstance(frequency, (int, float)) or frequency <= 0:
            raise ValueError("Frequency must be a positive number")
        
        if timeSetting.lower() not in TimeSetting.values():
            raise ValueError(
                f'Invalid option "{timeSetting}". Must be "fixed" or "variable"'
            )
        
    def stream(self, **kwargs):
        """Run the streaming with optional additional parameters."""

        if self.engine is None:
            raise ValueError("Database connection not established. Create a connection using connector() first")
        
        try:
            # Choose streaming method based on timeSetting
            if self.timeSetting == TimeSetting.FIXED.value:
                self._stream_with_fixed_delay(self.df)
            else:
                self._stream_with_variable_delay(self.df)
        except KeyboardInterrupt:
            print("\nStreaming interrupted by user")
        except Exception as e:
            print(f"Error during streaming: {str(e)}")
            traceback.print_exc()

        """if self.option == TimeSetting.FIXED.value:      # Fixed
            for index, row in df.iterrows():
                now = datetime.now()
                formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

                print(f"{formatted_now} - Row {index}: {row.to_dict()}")
                time.sleep(self.frequency)
        else:                                           # Variable
            for index, row in df.iterrows():
                frequency = random.uniform(0, self.frequency)
                print(frequency)

                now = datetime.now()
                formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

                print(f"{formatted_now} - Row {index}: {row.to_dict()}")
                time.sleep(frequency)"""

        return self.results
    
    def _stream_with_fixed_delay(self, df):
        """Handle fixed frequency streaming with consistent delays"""
        for index, row in df.iterrows():
            df_row = pd.DataFrame([row])
            time.sleep(self.frequency)
            
            if self.engine_name == 'postgres':
                df_row.to_sql("test", con=self.engine, if_exists='append', index=False)
            elif self.engine_name == 'mysql':
                df_row.to_sql("test", con=self.engine, if_exists='append', index=False)
            else:
                raise ValueError(f"Unsupported database type: {self.engine_name}")
            
            #print(df_row)
            self._print_row(index, row)
                    
    def _stream_with_variable_delay(self, df):
        """Handle variable frequency streaming with random delays"""
        for index, row in df.iterrows():
            df_row = pd.DataFrame([row])
            time.sleep(int(random.uniform(1, self.frequency)))

            if self.engine_name == 'postgres':
                df_row.to_sql("test", con=self.engine, if_exists='append', index=False)
            elif self.engine_name == 'mysql':
                df_row.to_sql("test", con=self.engine, if_exists='append', index=False)
            else:
                raise ValueError(f"Unsupported database type: {self.engine_name}")

            #print(df_row)
            self._print_row(index, row)

    def _print_row(self, index, row):
        """Format and print a single row of data"""
        now = datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"{formatted_now} - Row {index}: {row.to_dict()}")

    def _log_row(self, index, row):
        """Format and print a single row of data"""
        now = datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
    
        print(f"{formatted_now} - Row {index}: {row.to_dict()}")

    def postgres_connector(self, db_name, db_user, db_password, db_host, db_port):

        conn_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        try:
            self.engine_name, self.engine = 'postgres', create_engine(conn_url)
            return self.engine
        except SQLAlchemyError as e:
            print(f"Error creating engine or connecting to Postgres database: {e}")
            self.engine = None
        except Exception as e:
            print(f"Unexpected error: {e}")
            self.engine = None 

    def mysql_connector(self, db_name, db_user, db_password, db_host, db_port):

        conn_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"        
        try:
            self.engine_name, self.engine = 'mysql', create_engine(conn_url)
            return self.engine
        except SQLAlchemyError as e:
            print(f"Error creating engine or connecting to the MySQL database: {e}")
            self.engine = None
        except Exception as e:
            print(f"Unexpected error: {e}")
            self.engine = None