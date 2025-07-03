import pandas as pd
import numpy as np
import os
import glob
import datetime
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s -%(message)s',
    handlers=[
        logging.FileHandler("index_transforation.log"),
        logging.StreamHandler()
    ]
)

# Define the regions and related data for our indexes
INDEX_METADATA = {
    "S&P_500": {
        "country": "United States",
        "region": "North America",
        "continent": "North America",
        "currency": "USD",
        "latitude": 40.7128,  # New York
        "longitude": -74.0060
    },
    "Dow_Jones": {
        "country": "United States",
        "region": "North America",
        "continent": "North America",
        "currency": "USD",
        "latitude": 40.7128,  # New York
        "longitude": -74.0060
    },
    "NASDAQ_Composite": {
        "country": "United States",
        "region": "North America",
        "continent": "North America",
        "currency": "USD",
        "latitude": 40.7128,  # New York
        "longitude": -74.0060
    },
    "FTSE_100": {
        "country": "United Kingdom",
        "region": "Europe",
        "continent": "Europe",
        "currency": "GBP",
        "latitude": 51.5074,  # London
        "longitude": -0.1278
    },
    "DAX_40": {
        "country": "Germany",
        "region": "Europe",
        "continent": "Europe",
        "currency": "EUR",
        "latitude": 50.1109,  # Frankfurt
        "longitude": 8.6821
    },
    "CAC_40": {
        "country": "France",
        "region": "Europe",
        "continent": "Europe",
        "currency": "EUR",
        "latitude": 48.8566,  # Paris
        "longitude": 2.3522
    },
    "JSE_Top_40": {
        "country": "South Africa",
        "region": "Africa",
        "continent": "Africa",
        "currency": "ZAR",
        "latitude": -26.2041,  # Johannesburg
        "longitude": 28.0473
    },
    "Nikkei_225": {
        "country": "Japan",
        "region": "Asia-Pacific",
        "continent": "Asia",
        "currency": "JPY",
        "latitude": 35.6762,  # Tokyo
        "longitude": 139.6503
    },
    "Hang_Seng": {
        "country": "Hong Kong",
        "region": "Asia-Pacific",
        "continent": "Asia",
        "currency": "HKD",
        "latitude": 22.3193,  # Hong Kong
        "longitude": 114.1694
    },
    "EURO_STOXX_50": {
        "country": "Eurozone",
        "region": "Europe",
        "continent": "Europe",
        "currency": "EUR",
        "latitude": 50.1109,  # Frankfurt (ECB location)
        "longitude": 8.6821
    }
}

class SimpleIndexTransformer:
    def __init__(self, data_dir="data/raw_data/market_indexes", output_dir="data/processed_data", reference_date=None):
        """
        Inititalize the transformer.

        Parameters:
        ------------
        data_dir : str
            Directory containing the raw index data CSV files
        output_dir: str
            Directory where transformed data will be saved
        reference_date : str or None
            Date to use as reference for normalization (e.g., '2020-01-01)
            If None, the earliest date in the data will be used
        """
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.reference_date = reference_date

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Initialize data structures
        self.raw_data = None
        self.transformed_data = None

    def load_data(self):
        """Load data from combined file (preferred) or individual CSV files"""
        logging.info(f"Loading data from {self.data_dir}")
    
        # First priority: Use the combined file if it exists
        combined_file = os.path.join(self.data_dir, "all_indexes_combined.csv")
    
        if os.path.exists(combined_file):
            logging.info(f"Using combined file: {combined_file}")
            df = pd.read_csv(combined_file)
            logging.info(f"Loaded combined file with {len(df)} rows")
        
        else:
            # Only if combined file doesn't exist: load individual files
            logging.info("Combined file not found, loading individual CSV files...")
        
            csv_files = glob.glob(os.path.join(self.data_dir, "*.csv"))
        
            if not csv_files:
                raise ValueError(f"No CSV files found in {self.data_dir}")
        
            dfs = []
            for file in csv_files:
                try:
                    file_df = pd.read_csv(file)
                    dfs.append(file_df)
                    logging.info(f"Loaded {os.path.basename(file)} with {len(file_df)} rows")
                except Exception as e:
                    logging.error(f"Error loading {file}: {str(e)}")
                    continue
        
            if not dfs:
                raise ValueError("No valid CSV files could be loaded")
        
            df = pd.concat(dfs, ignore_index=True)
            logging.info(f"Combined {len(csv_files)} individual files into {len(df)} rows")
    
        # Validate required columns
        required_columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'Index_Name', 'Ticker']
        missing_columns = [col for col in required_columns if col not in df.columns]
    
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            logging.error(f"Available columns: {df.columns.tolist()}")
            raise ValueError(f"Required columns missing: {missing_columns}")
    
        # Convert data types
        numeric_columns = ['Close', 'High', 'Low', 'Open', 'Volume']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
        # Sort for consistent processing
        df = df.sort_values(['Index_Name', 'Date']).reset_index(drop=True)
    
        self.raw_data = df
    
        logging.info(f"Data loading complete: {len(df)} rows, {df['Index_Name'].nunique()} unique indexes")
        return self.raw_data
    
    def clean_data(self):
        """
        Clean the data (handle missing values, etc.)
        """
        logging.info("Cleaning data...")

        # Make a copy to avoid modifying the raw data
        self.transformed_data = self.raw_data.copy()

        # Remove rows with NaN in critical columns
        critical_columns = ['Close', 'Open', 'High', 'Low']
        original_count = len(self.transformed_data)
        self.transformed_data = self.transformed_data.dropna(subset=critical_columns)
        dropped_count = original_count - len(self.transformed_data)

        if dropped_count > 0:
            logging.warning(f"Dropped {dropped_count} rows with mmissing values in critical columns")

        # Fill missing volumes with 0
        if 'Volume' in self.transformed_data.columns:
            self.transformed_data['Volume'] = self.transformed_data['Volume'].fillna(0)

        # Add metadata columns from INDEX_METADATA dictionary
        self.transformed_data['Country'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('country', 'Unknown'))
        
        self.transformed_data['Region'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('region', 'Unknown'))
        
        self.transformed_data['Continent'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('continent', 'Unknown'))
        
        self.transformed_data['Currency'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('currency', 'Unknown'))
        
        self.transformed_data['Latitude'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('latitude', 0))
        
        self.transformed_data['Longitude'] = self.transformed_data['Index_Name'].map(
            lambda x: INDEX_METADATA.get(x, {}).get('longitude', 0))
        
        # Create date hierarchy columns
        self.transformed_data['Year'] = self.transformed_data['Date'].dt.year
        self.transformed_data['Quarter'] = self.transformed_data['Date'].dt.quarter
        self.transformed_data['Month'] = self.transformed_data['Date'].dt.month
        self.transformed_data['Week'] = self.transformed_data['Date'].dt.isocalendar().week
        self.transformed_data['Day'] = self.transformed_data['Date'].dt.day
        self.transformed_data['Day_of_Week'] = self.transformed_data['Date'].dt.dayofweek
        self.transformed_data['Day_Name'] = self.transformed_data['Date'].dt.day_name()
        
        logging.info("Data cleaning complete")
        return self.transformed_data
    

    def calculate_performance_metrics(self):
        """Calculate various performance metrics"""
        logging.info("Calculating performance metrics...")
        
        # Initialize columns for metrics
        self.transformed_data['Daily_Return'] = 0.0
        self.transformed_data['Cumulative_Return'] = 0.0
        self.transformed_data['MA_50'] = 0.0
        self.transformed_data['MA_200'] = 0.0
        self.transformed_data['Volatility_10D'] = 0.0
        self.transformed_data['Volatility_30D'] = 0.0
        
        # Group by index name to process each index separately
        for index_name, group in self.transformed_data.groupby('Index_Name'):
            # Sort by date
            group = group.sort_values('Date')
            idx = group.index
            
            # Calculate daily returns first
            self.transformed_data.loc[idx, 'Daily_Return'] = group['Close'].pct_change() * 100
            
            # Calculate cumulative returns from first day
            first_close = group['Close'].iloc[0]
            self.transformed_data.loc[idx, 'Cumulative_Return'] = (
                (group['Close'] / first_close - 1) * 100
            )
            
            # Calculate moving averages
            self.transformed_data.loc[idx, 'MA_50'] = group['Close'].rolling(window=50).mean()
            self.transformed_data.loc[idx, 'MA_200'] = group['Close'].rolling(window=200).mean()
            
            # Now calculate volatility using the daily returns we computed
            returns = self.transformed_data.loc[idx, 'Daily_Return']
            self.transformed_data.loc[idx, 'Volatility_10D'] = returns.rolling(window=10).std()
            self.transformed_data.loc[idx, 'Volatility_30D'] = returns.rolling(window=30).std()
            
            logging.info(f"Calculated performance metrics for {index_name}")
        
        # Fill NaN values in calculated metrics with 0
        metrics_columns = ['Daily_Return', 'Cumulative_Return', 'MA_50', 'MA_200', 
                          'Volatility_10D', 'Volatility_30D']
        
        for col in metrics_columns:
            self.transformed_data[col] = self.transformed_data[col].fillna(0)
        
        logging.info("Performance metrics calculation complete")
        return self.transformed_data
    
    def normalize_indexes(self):
        """Normalize index values to a common base (100)"""
        logging.info("Normalizing index values...")
        
        # Determine reference date for normalization
        if self.reference_date is None:
            # Use the earliest common date across all indexes
            common_dates = {}
            for index_name, group in self.transformed_data.groupby('Index_Name'):
                common_dates[index_name] = group['Date'].min()
            
            self.reference_date = max(common_dates.values())
            logging.info(f"Using {self.reference_date} as reference date for normalization")
        else:
            self.reference_date = pd.to_datetime(self.reference_date)
        
        # Normalize each index (set to 100 at reference date)
        for index_name, group in self.transformed_data.groupby('Index_Name'):
            # Find the reference value for this index
            try:
                reference_value = group[group['Date'] >= self.reference_date]['Close'].iloc[0]
                
                # Calculate normalized value (base 100)
                self.transformed_data.loc[group.index, 'Normalized_Value'] = (
                    group['Close'] / reference_value * 100
                )
                
                logging.info(f"Normalized {index_name} with reference value {reference_value}")
                
            except (IndexError, KeyError) as e:
                logging.warning(f"Could not normalize {index_name}: {str(e)}")
                # Set to NaN for indexes that don't have data on reference date
                self.transformed_data.loc[group.index, 'Normalized_Value'] = np.nan
        
        logging.info("Normalization complete")
        return self.transformed_data
    

    def add_market_events(self):
        """Add major market events as annotations"""
        logging.info("Adding market events...")
        
        # Define some major global market events
        events = [
            {'date': '2020-03-11', 'event': 'WHO Declares COVID-19 Pandemic', 'type': 'Global Health'},
            {'date': '2020-03-23', 'event': 'Fed Announces Unlimited QE', 'type': 'Monetary Policy'},
            {'date': '2021-01-20', 'event': 'Biden Inauguration', 'type': 'Political'},
            {'date': '2021-11-08', 'event': 'First COVID Vaccine Approved', 'type': 'Global Health'},
            {'date': '2022-02-24', 'event': 'Russia-Ukraine Conflict Begins', 'type': 'Geopolitical'},
            {'date': '2022-03-16', 'event': 'Fed Begins Rate Hike Cycle', 'type': 'Monetary Policy'},
            {'date': '2023-03-10', 'event': 'Silicon Valley Bank Collapse', 'type': 'Financial Crisis'},
            {'date': '2023-07-07', 'event': 'Global COVID Emergency Ends', 'type': 'Global Health'},
            {'date': '2023-10-07', 'event': 'Middle East Conflict Escalation', 'type': 'Geopolitical'},
            {'date': '2024-01-04', 'event': 'Bitcoin ETF Approval', 'type': 'Financial Markets'}
        ]
        
        # Convert to DataFrame
        events_df = pd.DataFrame(events)
        events_df['date'] = pd.to_datetime(events_df['date'])
        
        # Save events to a separate file
        events_df.to_csv(os.path.join(self.output_dir, 'market_events.csv'), index=False)
        logging.info(f"Saved {len(events_df)} market events to market_events.csv")
        
        return events_df
    
   
    
    def save_transformed_data(self):
        """Save the transformed data to CSV files"""
        logging.info("Saving transformed data...")
        
        # Save the full transformed dataset
        full_file = os.path.join(self.output_dir, 'transformed_indexes.csv')
        self.transformed_data.to_csv(full_file, index=False)
        logging.info(f"Saved full transformed dataset to {full_file}")
        
        # Save a separate file for each index
        for index_name, group in self.transformed_data.groupby('Index_Name'):
            index_file = os.path.join(self.output_dir, f'{index_name}_transformed.csv')
            group.to_csv(index_file, index=False)
            logging.info(f"Saved {index_name} data to {index_file}")
        
        # Save a summary file with key metrics by index
        summary = self.transformed_data.groupby('Index_Name').agg({
            'Close': ['mean', 'min', 'max', 'std'],
            'Daily_Return': ['mean', 'min', 'max', 'std'],
            'Volatility_30D': ['mean', 'min', 'max'],
            'Date': ['min', 'max']
        }).reset_index()
        
        summary_file = os.path.join(self.output_dir, 'index_summary.csv')
        summary.to_csv(summary_file)
        logging.info(f"Saved summary metrics to {summary_file}")
        
        return full_file
    
    def process_all(self):
        """Run the full transformation pipeline"""
        logging.info("Starting transformation pipeline...")
        
        # Load the data
        self.load_data()
        
        # Clean the data
        self.clean_data()
        
        # Calculate performance metrics
        self.calculate_performance_metrics()
        
        # Normalize indices
        self.normalize_indexes()
        
        # Add market events
        self.add_market_events() 
        
        # Save transformed data
        output_file = self.save_transformed_data()
        
        logging.info(f"Transformation pipeline complete. Output saved to {output_file}")
        return output_file
    
# Run the transformation if script is executed directly
if __name__ == "__main__":
    transformer = SimpleIndexTransformer(
        data_dir="data/raw_data/market_indexes",
        output_dir="data/processed_data"
    )
    
    # Process all data
    output_file = transformer.process_all()
    
    print(f"\nTransformation complete!")
    print(f"Transformed data saved to: {output_file}")
