"""
src/transformers/green_bonds_transformer.py

Green Bonds/ESG ETF Data Transformer
Transforms raw green bond ETF data into database-ready format with technical indicators
"""

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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("green_bonds_transformation.log"),
        logging.StreamHandler()
    ]
)

# ETF metadata with comprehensive information
ETF_METADATA = {
    "iShares_Global_Green_Bond": {
        "ticker": "BGRN",
        "name": "iShares Global Green Bond ETF",
        "category": "Green Bonds",
        "asset_class": "Fixed Income",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.20,
        "inception_date": "2018-07-03",
        "benchmark": "Bloomberg MSCI Green Bond Index",
        "description": "Tracks green bonds from around the world"
    },
    "Franklin_Liberty_Green_Bond": {
        "ticker": "FLGR",
        "name": "Franklin Liberty US Green Bond ETF",
        "category": "Green Bonds",
        "asset_class": "Fixed Income",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "Franklin Templeton",
        "expense_ratio": 0.15,
        "inception_date": "2021-09-21",
        "benchmark": "Bloomberg US Green Bond Index",
        "description": "US-focused green bonds ETF"
    },
    "VanEck_Green_Bond": {
        "ticker": "GRNB",
        "name": "VanEck Green Bond ETF",
        "category": "Green Bonds",
        "asset_class": "Fixed Income",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "VanEck",
        "expense_ratio": 0.20,
        "inception_date": "2017-03-09",
        "benchmark": "S&P Green Bond Index",
        "description": "Global green bond exposure"
    },
    "Schwab_US_Large_Cap_Growth": {
        "ticker": "SCHG",
        "name": "Schwab US Large-Cap Growth ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "Charles Schwab",
        "expense_ratio": 0.04,
        "inception_date": "2009-12-11",
        "benchmark": "Dow Jones US Large-Cap Growth Total Stock Market Index",
        "description": "Large-cap growth stocks with ESG considerations"
    },
    "iShares_MSCI_USA_ESG_Select": {
        "ticker": "ESGU",
        "name": "iShares MSCI USA ESG Select ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.25,
        "inception_date": "2016-01-12",
        "benchmark": "MSCI USA Extended ESG Select Index",
        "description": "US companies with high ESG ratings"
    },
    "iShares_MSCI_EAFE_ESG_Select": {
        "ticker": "ESGD",
        "name": "iShares MSCI EAFE ESG Select ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "International",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.25,
        "inception_date": "2016-01-12",
        "benchmark": "MSCI EAFE Extended ESG Select Index",
        "description": "International developed market ESG stocks"
    },
    "SPDR_SSGA_Gender_Diversity": {
        "ticker": "SHE",
        "name": "SPDR SSGA Gender Diversity ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "State Street",
        "expense_ratio": 0.20,
        "inception_date": "2016-03-16",
        "benchmark": "SSGA Gender Diversity Index",
        "description": "Companies with gender diversity in leadership"
    },
    "iShares_MSCI_KLD_400_Social": {
        "ticker": "DSI",
        "name": "iShares MSCI KLD 400 Social ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.25,
        "inception_date": "2006-04-19",
        "benchmark": "MSCI KLD 400 Social Index",
        "description": "US companies meeting ESG criteria"
    },
    "iShares_Global_Clean_Energy": {
        "ticker": "ICLN",
        "name": "iShares Global Clean Energy ETF",
        "category": "Clean Energy",
        "asset_class": "Equity",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.42,
        "inception_date": "2008-06-24",
        "benchmark": "S&P Global Clean Energy Index",
        "description": "Clean energy companies globally"
    },
    "Invesco_WilderHill_Clean_Energy": {
        "ticker": "PBW",
        "name": "Invesco WilderHill Clean Energy ETF",
        "category": "Clean Energy",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "Invesco",
        "expense_ratio": 0.70,
        "inception_date": "2005-03-03",
        "benchmark": "WilderHill Clean Energy Index",
        "description": "US clean energy companies"
    },
    "First_Trust_Clean_Edge_Green": {
        "ticker": "QCLN",
        "name": "First Trust Clean Edge Green Energy ETF",
        "category": "Clean Energy",
        "asset_class": "Equity",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "First Trust",
        "expense_ratio": 0.60,
        "inception_date": "2007-02-08",
        "benchmark": "NASDAQ Clean Edge Green Energy Index",
        "description": "Global clean energy and technology companies"
    },
    "Invesco_Water_Resources": {
        "ticker": "PHO",
        "name": "Invesco Water Resources ETF",
        "category": "Water/Environmental",
        "asset_class": "Equity",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "Invesco",
        "expense_ratio": 0.60,
        "inception_date": "2005-12-06",
        "benchmark": "NASDAQ OMX US Water Index",
        "description": "Water infrastructure and technology companies"
    },
    "Invesco_Global_Water": {
        "ticker": "PIO",
        "name": "Invesco Global Water ETF",
        "category": "Water/Environmental",
        "asset_class": "Equity",
        "geographic_focus": "Global",
        "currency": "USD",
        "issuer": "Invesco",
        "expense_ratio": 0.75,
        "inception_date": "2007-06-13",
        "benchmark": "NASDAQ OMX Global Water Index",
        "description": "Global water-related companies"
    },
    "Vanguard_ESG_US_Stock": {
        "ticker": "ESGV",
        "name": "Vanguard ESG U.S. Stock ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "Vanguard",
        "expense_ratio": 0.12,
        "inception_date": "2018-09-18",
        "benchmark": "FTSE US All Cap Choice Index",
        "description": "Broad US market with ESG screening"
    },
    "iShares_MSCI_USA_ESG_Select": {
        "ticker": "SUSL",
        "name": "iShares MSCI USA ESG Select ETF",
        "category": "ESG Equity",
        "asset_class": "Equity",
        "geographic_focus": "United States",
        "currency": "USD",
        "issuer": "BlackRock",
        "expense_ratio": 0.25,
        "inception_date": "2017-04-03",
        "benchmark": "MSCI USA Extended ESG Select Index",
        "description": "US companies with superior ESG profiles"
    }
}

class GreenBondsTransformer:
    def __init__(self, data_dir="data/raw_data/green_bonds", output_dir="data/processed_data", reference_date=None):
        """
        Initialize the Green Bonds transformer.

        Parameters:
        ------------
        data_dir : str
            Directory containing the raw green bonds data CSV files
        output_dir: str
            Directory where transformed data will be saved
        reference_date : str or None
            Date to use as reference for normalization (e.g., '2020-01-01')
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
        logging.info(f"Loading green bonds data from {self.data_dir}")
    
        # First priority: Use the combined file if it exists
        combined_file = os.path.join(self.data_dir, "all_green_bonds_combined.csv")
    
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
        required_columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'ETF_Name', 'Ticker']
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
        df = df.sort_values(['ETF_Name', 'Date']).reset_index(drop=True)
    
        self.raw_data = df
    
        logging.info(f"Data loading complete: {len(df)} rows, {df['ETF_Name'].nunique()} unique ETFs")
        return self.raw_data
    
    def clean_data(self):
        """Clean the data (handle missing values, etc.)"""
        logging.info("Cleaning green bonds data...")

        # Make a copy to avoid modifying the raw data
        self.transformed_data = self.raw_data.copy()

        # Remove rows with NaN in critical columns
        critical_columns = ['Close', 'Open', 'High', 'Low']
        original_count = len(self.transformed_data)
        self.transformed_data = self.transformed_data.dropna(subset=critical_columns)
        dropped_count = original_count - len(self.transformed_data)

        if dropped_count > 0:
            logging.warning(f"Dropped {dropped_count} rows with missing values in critical columns")

        # Fill missing volumes with 0
        if 'Volume' in self.transformed_data.columns:
            self.transformed_data['Volume'] = self.transformed_data['Volume'].fillna(0)

        # Add metadata columns from ETF_METADATA dictionary
        self.transformed_data['Category'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('category', 'Unknown'))
        
        self.transformed_data['Asset_Class'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('asset_class', 'Unknown'))
        
        self.transformed_data['Geographic_Focus'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('geographic_focus', 'Unknown'))
        
        self.transformed_data['Currency'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('currency', 'USD'))
        
        self.transformed_data['Issuer'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('issuer', 'Unknown'))
        
        self.transformed_data['Expense_Ratio'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('expense_ratio', 0))
        
        self.transformed_data['Full_Name'] = self.transformed_data['ETF_Name'].map(
            lambda x: ETF_METADATA.get(x, {}).get('name', x))
        
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
        self.transformed_data['MA_20'] = 0.0
        self.transformed_data['MA_50'] = 0.0
        self.transformed_data['MA_200'] = 0.0
        self.transformed_data['Volatility_10D'] = 0.0
        self.transformed_data['Volatility_30D'] = 0.0
        self.transformed_data['RSI_14'] = 0.0
        self.transformed_data['Price_Change_1M'] = 0.0
        self.transformed_data['Price_Change_3M'] = 0.0
        self.transformed_data['Price_Change_1Y'] = 0.0
        
        # Group by ETF name to process each ETF separately
        for etf_name, group in self.transformed_data.groupby('ETF_Name'):
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
            self.transformed_data.loc[idx, 'MA_20'] = group['Close'].rolling(window=20).mean()
            self.transformed_data.loc[idx, 'MA_50'] = group['Close'].rolling(window=50).mean()
            self.transformed_data.loc[idx, 'MA_200'] = group['Close'].rolling(window=200).mean()
            
            # Calculate volatility using the daily returns
            returns = self.transformed_data.loc[idx, 'Daily_Return']
            self.transformed_data.loc[idx, 'Volatility_10D'] = returns.rolling(window=10).std()
            self.transformed_data.loc[idx, 'Volatility_30D'] = returns.rolling(window=30).std()
            
            # Calculate RSI (14-day)
            self.transformed_data.loc[idx, 'RSI_14'] = self._calculate_rsi(group['Close'], 14)
            
            # Calculate price changes over different periods
            self.transformed_data.loc[idx, 'Price_Change_1M'] = group['Close'].pct_change(periods=21) * 100  # ~1 month
            self.transformed_data.loc[idx, 'Price_Change_3M'] = group['Close'].pct_change(periods=63) * 100  # ~3 months
            self.transformed_data.loc[idx, 'Price_Change_1Y'] = group['Close'].pct_change(periods=252) * 100  # ~1 year
            
            logging.info(f"Calculated performance metrics for {etf_name}")
        
        # Fill NaN values in calculated metrics with 0
        metrics_columns = [
            'Daily_Return', 'Cumulative_Return', 'MA_20', 'MA_50', 'MA_200', 
            'Volatility_10D', 'Volatility_30D', 'RSI_14', 'Price_Change_1M',
            'Price_Change_3M', 'Price_Change_1Y'
        ]
        
        for col in metrics_columns:
            self.transformed_data[col] = self.transformed_data[col].fillna(0)
        
        logging.info("Performance metrics calculation complete")
        return self.transformed_data
    
    def _calculate_rsi(self, prices, window=14):
        """Calculate RSI (Relative Strength Index)"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def normalize_etfs(self):
        """Normalize ETF values to a common base (100)"""
        logging.info("Normalizing ETF values...")
        
        # Determine reference date for normalization
        if self.reference_date is None:
            # Use the earliest common date across all ETFs
            common_dates = {}
            for etf_name, group in self.transformed_data.groupby('ETF_Name'):
                common_dates[etf_name] = group['Date'].min()
            
            self.reference_date = max(common_dates.values())
            logging.info(f"Using {self.reference_date} as reference date for normalization")
        else:
            self.reference_date = pd.to_datetime(self.reference_date)
        
        # Normalize each ETF (set to 100 at reference date)
        for etf_name, group in self.transformed_data.groupby('ETF_Name'):
            # Find the reference value for this ETF
            try:
                reference_value = group[group['Date'] >= self.reference_date]['Close'].iloc[0]
                
                # Calculate normalized value (base 100)
                self.transformed_data.loc[group.index, 'Normalized_Value'] = (
                    group['Close'] / reference_value * 100
                )
                
                logging.info(f"Normalized {etf_name} with reference value {reference_value}")
                
            except (IndexError, KeyError) as e:
                logging.warning(f"Could not normalize {etf_name}: {str(e)}")
                # Set to NaN for ETFs that don't have data on reference date
                self.transformed_data.loc[group.index, 'Normalized_Value'] = np.nan
        
        logging.info("Normalization complete")
        return self.transformed_data
    
    def add_esg_classification(self):
        """Add ESG classification and sustainability metrics"""
        logging.info("Adding ESG classification...")
        
        # ESG scoring based on category and focus
        def get_esg_score(category, asset_class):
            """Assign ESG scores based on category"""
            if category == 'Green Bonds':
                return 95  # Highest ESG score
            elif category == 'ESG Equity':
                return 85
            elif category == 'Clean Energy':
                return 90
            elif category == 'Water/Environmental':
                return 80
            else:
                return 50  # Default score
        
        self.transformed_data['ESG_Score'] = self.transformed_data.apply(
            lambda row: get_esg_score(row['Category'], row['Asset_Class']), axis=1
        )
        
        # Add sustainability themes
        def get_sustainability_theme(category):
            """Map categories to sustainability themes"""
            theme_mapping = {
                'Green Bonds': 'Climate Finance',
                'ESG Equity': 'Responsible Investing',
                'Clean Energy': 'Renewable Energy',
                'Water/Environmental': 'Resource Conservation'
            }
            return theme_mapping.get(category, 'General ESG')
        
        self.transformed_data['Sustainability_Theme'] = self.transformed_data['Category'].map(
            get_sustainability_theme
        )
        
        # Add risk classification
        def get_risk_level(asset_class, category):
            """Assign risk levels"""
            if asset_class == 'Fixed Income':
                return 'Low'
            elif category == 'Clean Energy':
                return 'High'  # Clean energy tends to be more volatile
            else:
                return 'Medium'
        
        self.transformed_data['Risk_Level'] = self.transformed_data.apply(
            lambda row: get_risk_level(row['Asset_Class'], row['Category']), axis=1
        )
        
        logging.info("ESG classification complete")
        return self.transformed_data
    
    def save_transformed_data(self):
        """Save the transformed data to CSV files"""
        logging.info("Saving transformed data...")
        
        # Save the full transformed dataset
        full_file = os.path.join(self.output_dir, 'transformed_green_bonds.csv')
        self.transformed_data.to_csv(full_file, index=False)
        logging.info(f"Saved full transformed dataset to {full_file}")
        
        # Save a separate file for each ETF
        for etf_name, group in self.transformed_data.groupby('ETF_Name'):
            etf_file = os.path.join(self.output_dir, f'{etf_name}_transformed.csv')
            group.to_csv(etf_file, index=False)
            logging.info(f"Saved {etf_name} data to {etf_file}")
        
        # Save a summary file with key metrics by ETF
        summary = self.transformed_data.groupby('ETF_Name').agg({
            'Close': ['mean', 'min', 'max', 'std'],
            'Daily_Return': ['mean', 'min', 'max', 'std'],
            'Volatility_30D': ['mean', 'min', 'max'],
            'ESG_Score': 'first',
            'Category': 'first',
            'Asset_Class': 'first',
            'Expense_Ratio': 'first',
            'Date': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        summary.columns = ['_'.join(col).strip() if col[1] else col[0] for col in summary.columns]
        
        summary_file = os.path.join(self.output_dir, 'green_bonds_summary.csv')
        summary.to_csv(summary_file, index=False)
        logging.info(f"Saved summary metrics to {summary_file}")
        
        return full_file
    
    def process_all(self):
        """Run the full transformation pipeline"""
        logging.info("Starting green bonds transformation pipeline...")
        
        # Load the data
        self.load_data()
        
        # Clean the data
        self.clean_data()
        
        # Calculate performance metrics
        self.calculate_performance_metrics()
        
        # Normalize ETFs
        self.normalize_etfs()
        
        # Add ESG classification
        self.add_esg_classification()
        
        # Save transformed data
        output_file = self.save_transformed_data()
        
        logging.info(f"Green bonds transformation pipeline complete. Output saved to {output_file}")
        return output_file

# Run the transformation if script is executed directly
if __name__ == "__main__":
    transformer = GreenBondsTransformer(
        data_dir="data/raw_data/green_bonds",
        output_dir="data/processed_data"
    )
    
    # Process all data
    output_file = transformer.process_all()
    
    print(f"\nTransformation complete!")
    print(f"Transformed data saved to: {output_file}")