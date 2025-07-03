import yfinance as yf
import pandas as pd
import logging
import os
import time
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def download_single_etf(symbol, name, start_date, end_date, output_dir):
    """
    Download data for a single green bond ETF
    """
    try:
        logging.info(f"Starting download for {name} ({symbol})...")

        # Download data
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)

        if df.empty:
            logging.error(f"No data was retrieved for {name} ({symbol})")
            return False
        
        # Reset index to make Date a column (important!)
        df.reset_index(inplace=True)

        # Add metadata columns
        df['ETF_Name'] = name
        df['Ticker'] = symbol

        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Create organized directory structure
        raw_data_dir = os.path.join(output_dir, "raw_data", "green_bonds")
        os.makedirs(raw_data_dir, exist_ok=True) 

        filename = f"{name}_{start_date}_to_{end_date}.csv"
        filepath = os.path.join(raw_data_dir, filename)

        # Save to CSV
        df.to_csv(filepath, index=False)

        logging.info(f"✅ Successfully saved {len(df)} days of data for {name}")
        logging.info(f"✅ Data saved to: {filepath}")

        # Basic validation
        try:
            sample = pd.read_csv(filepath)
            if 'Date' in sample.columns:
                date_min = sample['Date'].min()
                date_max = sample['Date'].max()
                latest_close = sample['Close'].iloc[-1] if 'Close' in sample.columns else 'N/A'

                logging.info(f"✅ Validation successful for {name}:")
                logging.info(f"   Date range: {date_min} to {date_max}")
                logging.info(f"   Latest close: {latest_close}")
                logging.info(f"   Total rows: {len(sample)}")
            else:
                logging.warning(f"Date column not found in {name}, but file saved successfully")
        except Exception as validation_error:
            logging.warning(f"Validation failed for {name} but file was saved: {str(validation_error)}")
        
        return True

    except Exception as e:
        logging.error(f"❌ Error downloading data for {name} ({symbol}): {str(e)}")
        return False


def collect_all_green_bonds_sequentially(start_date, end_date, output_dir, delay_seconds=3):
    """
    Collect all green bond ETFs one by one with delays between downloads
    """

    # Define green bond and ESG ETFs to download
    GREEN_BOND_ETFS = [
        # Major Green Bond ETFs
        {"symbol": "BGRN", "name": "iShares_Global_Green_Bond"},
        {"symbol": "FLGR", "name": "Franklin_Liberty_Green_Bond"},
        {"symbol": "GRNB", "name": "VanEck_Green_Bond"},
        {"symbol": "SCHG", "name": "Schwab_US_Large_Cap_Growth"},  # ESG focused
        
        # ESG/Sustainable ETFs
        {"symbol": "ESGU", "name": "iShares_MSCI_USA_ESG_Select"},
        {"symbol": "ESGD", "name": "iShares_MSCI_EAFE_ESG_Select"},
        {"symbol": "SHE", "name": "SPDR_SSGA_Gender_Diversity"},
        {"symbol": "DSI", "name": "iShares_MSCI_KLD_400_Social"},
        
        # Clean Energy ETFs
        {"symbol": "ICLN", "name": "iShares_Global_Clean_Energy"},
        {"symbol": "PBW", "name": "Invesco_WilderHill_Clean_Energy"},
        {"symbol": "QCLN", "name": "First_Trust_Clean_Edge_Green"},
        
        # Water/Environmental ETFs
        {"symbol": "PHO", "name": "Invesco_Water_Resources"},
        {"symbol": "PIO", "name": "Invesco_Global_Water"},
        
        # Broad ESG ETFs
        {"symbol": "ESGV", "name": "Vanguard_ESG_US_Stock"},
        {"symbol": "SUSL", "name": "iShares_MSCI_USA_ESG_Select"}
    ]

    # Track results
    successful_downloads = []
    failed_downloads = []
    total_etfs = len(GREEN_BOND_ETFS)

    logging.info(f"Starting sequential download of {total_etfs} green bond/ESG ETFs")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Delay between downloads: {delay_seconds} seconds")
    logging.info("=" * 60)

    # Download each ETF one by one
    for i, etf_info in enumerate(GREEN_BOND_ETFS, 1):
        symbol = etf_info['symbol']
        name = etf_info['name']

        logging.info(f"[{i}/{total_etfs}] Processing {name}")
        logging.info("-" * 40)

        # Attempt download
        success = download_single_etf(
            symbol=symbol,
            name=name,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir
        )

        # Record result
        if success:
            successful_downloads.append({"name": name, "symbol": symbol})
            logging.info(f"✅ {name} completed successfully")
        else:
            failed_downloads.append({"name": name, "symbol": symbol})
            logging.warning(f"❌ {name} failed to download")

        # Add delay between downloads (except for the last one)
        if i < total_etfs:
            logging.info(f"Waiting {delay_seconds} seconds before the next download...")
            time.sleep(delay_seconds)

        logging.info("=" * 60)

    # Final summary
    logging.info("DOWNLOAD SUMMARY")
    logging.info("=" * 60)
    logging.info(f"Total ETFs processed: {total_etfs}")
    logging.info(f"Successful downloads: {len(successful_downloads)}")
    logging.info(f"Failed downloads: {len(failed_downloads)}")

    if successful_downloads:
        logging.info("\n✅ Successfully downloaded:")
        for item in successful_downloads:
            logging.info(f"   - {item['name']} ({item['symbol']})")

    if failed_downloads:
        logging.warning("\n❌ Failed to download:")
        for item in failed_downloads:
            logging.warning(f"   - {item['name']} ({item['symbol']})")

        logging.info("\nAlternative approaches for failed downloads:")
        logging.info("1. Try different ticker symbols")
        logging.info("2. Check if ETF is still active")
        logging.info("3. Verify ticker symbols on Yahoo Finance")

    return successful_downloads, failed_downloads


def create_combined_file(output_dir):
    """
    Combine all individual CSV files into one master file
    """
    logging.info("Creating combined file...")

    # Look in the correct subdirectory where files were actually saved
    raw_data_dir = os.path.join(output_dir, "raw_data", "green_bonds")
    
    if not os.path.exists(raw_data_dir):
        logging.warning(f"Directory not found: {raw_data_dir}")
        return None

    csv_files = [f for f in os.listdir(raw_data_dir) if f.endswith('.csv') and not f.startswith('all_')]

    if not csv_files:
        logging.warning("No CSV files found to combine")
        return None

    all_dfs = []
    for file in csv_files:
        filepath = os.path.join(raw_data_dir, file)
        try:
            df = pd.read_csv(filepath)
            all_dfs.append(df)
            logging.info(f"Added {file} to combined dataset ({len(df)} rows)")
        except Exception as e:
            logging.error(f"Error reading {file}: {str(e)}")

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_filepath = os.path.join(raw_data_dir, 'all_green_bonds_combined.csv')
        combined_df.to_csv(combined_filepath, index=False)

        logging.info(f"✅ Combined dataset created with {len(combined_df)} total rows")
        logging.info(f"✅ Saved to: {combined_filepath}")

        # Show summary by ETF
        if 'ETF_Name' in combined_df.columns:
            summary = combined_df.groupby('ETF_Name').size().reset_index(name='row_count')
            logging.info("Data summary by ETF:")
            for _, row in summary.iterrows():
                logging.info(f"   - {row['ETF_Name']}: {row['row_count']} days")

        return combined_filepath

    return None


def verify_files_exist(output_dir):
    """
    Verify that CSV files actually exist and can be read properly
    """
    logging.info("Verifying downloaded files...")

    raw_data_dir = os.path.join(output_dir, "raw_data", "green_bonds")
    
    if not os.path.exists(raw_data_dir):
        logging.error(f"Raw data directory not found: {raw_data_dir}")
        return False

    csv_files = [f for f in os.listdir(raw_data_dir) if f.endswith('.csv')]

    if not csv_files:
        logging.error("No CSV files found in output directory!")
        return False

    valid_files = 0
    for file in csv_files:
        filepath = os.path.join(raw_data_dir, file)
        try:
            df = pd.read_csv(filepath)
            logging.info(f"✅ {file}: {len(df)} rows, columns: {list(df.columns)}")
            valid_files += 1
        except Exception as e:
            logging.error(f"❌ {file}: Error reading - {str(e)}")

    logging.info(f"File verification complete: {valid_files}/{len(csv_files)} files are valid")
    return valid_files > 0


# Main execution
if __name__ == "__main__":
    # Set date range (5 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5*365)

    # Output directory
    output_dir = "data"

    # Convert dates to strings
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    logging.info("GREEN BOND ETF DATA COLLECTION")
    logging.info("=" * 60)

    # Collect all ETFs sequentially
    successful, failed = collect_all_green_bonds_sequentially(
        start_date=start_date_str,
        end_date=end_date_str,
        output_dir=output_dir,
        delay_seconds=3  # 3 second delay between downloads
    )

    # Verify files exist and are readable
    files_valid = verify_files_exist(output_dir)

    # Create combined file if we have valid files
    if files_valid:
        combined_file = create_combined_file(output_dir)
        if combined_file:
            logging.info(f"✅ All data collection complete!")
            logging.info(f"✅ Individual files and combined file saved in: {output_dir}")
        else:
            logging.warning("Combined file creation failed")
    else:
        logging.error("No valid files found to process")

    logging.info("Script execution completed.")