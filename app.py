"""
app.py

Flask web application for MMF & Fixed Deposit data entry
Provides web forms that integrate with your database loader
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import logging
from datetime import datetime
import os
import sys

# Add your project path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your loader
from src.pipelines.mmf_FD_pipeline import SimpleMMFFixedDepositLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production

# Initialize loader
loader = SimpleMMFFixedDepositLoader()

@app.route('/')
def index():
    """Main dashboard"""
    try:
        # Get current rate summary
        rate_summary = loader._get_rate_summary()
        return render_template('index.html', rate_summary=rate_summary)
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return render_template('index.html', rate_summary={})

@app.route('/add_fixed_deposit', methods=['GET', 'POST'])
def add_fixed_deposit():
    """Add fixed deposit form"""
    if request.method == 'POST':
        try:
            # Get form data
            fd_data = {
                'bank': request.form['bank'],
                'yield_per_annum': float(request.form['yield_per_annum']),
                'minimum_amount': float(request.form['minimum_amount']),
                'term_months': int(request.form['term_months']),
                'penalty_severity': request.form['penalty_severity'],
                'penalty_description': request.form['penalty_description'],
                'source': request.form['source'],
                'currency': request.form['currency']
            }
            
            # Load data
            records_loaded = loader._load_fixed_deposits([fd_data])
            
            if records_loaded > 0:
                flash(f'Successfully added fixed deposit for {fd_data["bank"]}!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Failed to add fixed deposit. Please try again.', 'error')
                
        except Exception as e:
            logger.error(f"Error adding fixed deposit: {e}")
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('add_fixed_deposit.html')

@app.route('/add_mmf', methods=['GET', 'POST'])
def add_mmf():
    """Add money market fund form"""
    if request.method == 'POST':
        try:
            # Get form data
            mmf_data = {
                'company': request.form['company'],
                'product_name': request.form['product_name'],
                'average_annual_yield': float(request.form['average_annual_yield']),
                'withholding_tax': float(request.form['withholding_tax']),
                'minimum_investment': float(request.form['minimum_investment']),
                'management_fee': float(request.form['management_fee']),
                'currency': request.form['currency']
            }
            
            # Calculate net yield
            gross_yield = mmf_data['average_annual_yield']
            withholding_tax = mmf_data['withholding_tax']
            management_fee = mmf_data['management_fee']
            
            # Net yield = (Gross yield - Management fee) * (1 - Withholding tax rate)
            net_yield = (gross_yield - management_fee) * (1 - withholding_tax / 100)
            mmf_data['net_yield'] = net_yield
            
            # Load data
            records_loaded = loader._load_money_market_funds([mmf_data])
            
            if records_loaded > 0:
                flash(f'Successfully added MMF for {mmf_data["company"]}!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Failed to add MMF. Please try again.', 'error')
                
        except Exception as e:
            logger.error(f"Error adding MMF: {e}")
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('add_mmf.html')

@app.route('/bulk_upload', methods=['GET', 'POST'])
def bulk_upload():
    """Bulk upload from CSV/Excel"""
    if request.method == 'POST':
        try:
            file = request.files['file']
            data_type = request.form['data_type']
            
            if file and file.filename.endswith(('.csv', '.xlsx')):
                # Save uploaded file
                filename = f"uploads/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
                os.makedirs('uploads', exist_ok=True)
                file.save(filename)
                
                # Process file based on type
                if data_type == 'fixed_deposits':
                    records_loaded = process_fd_file(filename)
                elif data_type == 'mmf_kes':
                    records_loaded = process_mmf_file(filename, 'KES')
                elif data_type == 'mmf_usd':
                    records_loaded = process_mmf_file(filename, 'USD')
                else:
                    flash('Invalid data type selected.', 'error')
                    return redirect(url_for('bulk_upload'))
                
                flash(f'Successfully loaded {records_loaded} records from {file.filename}!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Please upload a valid CSV or Excel file.', 'error')
                
        except Exception as e:
            logger.error(f"Error in bulk upload: {e}")
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('bulk_upload.html')

@app.route('/api/rates')
def api_rates():
    """API endpoint to get current rates"""
    try:
        rate_summary = loader._get_rate_summary()
        return jsonify({
            'status': 'success',
            'data': rate_summary,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting rates: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/add_fd', methods=['POST'])
def api_add_fd():
    """API endpoint to add fixed deposit"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['bank', 'yield_per_annum', 'minimum_amount', 'term_months', 'penalty_severity']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Set defaults
        fd_data = {
            'bank': data['bank'],
            'yield_per_annum': float(data['yield_per_annum']),
            'minimum_amount': float(data['minimum_amount']),
            'term_months': int(data['term_months']),
            'penalty_severity': data['penalty_severity'],
            'penalty_description': data.get('penalty_description', ''),
            'source': data.get('source', 'web_form'),
            'currency': data.get('currency', 'KES')
        }
        
        # Load data
        records_loaded = loader._load_fixed_deposits([fd_data])
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully added fixed deposit for {fd_data["bank"]}',
            'records_loaded': records_loaded
        })
        
    except Exception as e:
        logger.error(f"API error adding fixed deposit: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/add_mmf', methods=['POST'])
def api_add_mmf():
    """API endpoint to add money market fund"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['company', 'product_name', 'average_annual_yield', 'management_fee']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'status': 'error',
                    'message': f'Missing required field: {field}'
                }), 400
        
        # Set defaults and calculate net yield
        mmf_data = {
            'company': data['company'],
            'product_name': data['product_name'],
            'average_annual_yield': float(data['average_annual_yield']),
            'withholding_tax': float(data.get('withholding_tax', 15.0)),
            'minimum_investment': float(data.get('minimum_investment', 1000)),
            'management_fee': float(data['management_fee']),
            'currency': data.get('currency', 'KES')
        }
        
        # Calculate net yield
        gross_yield = mmf_data['average_annual_yield']
        withholding_tax = mmf_data['withholding_tax']
        management_fee = mmf_data['management_fee']
        
        net_yield = (gross_yield - management_fee) * (1 - withholding_tax / 100)
        mmf_data['net_yield'] = net_yield
        
        # Load data
        records_loaded = loader._load_money_market_funds([mmf_data])
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully added MMF for {mmf_data["company"]}',
            'records_loaded': records_loaded,
            'calculated_net_yield': net_yield
        })
        
    except Exception as e:
        logger.error(f"API error adding MMF: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def process_fd_file(filename):
    """Process fixed deposit CSV/Excel file"""
    import pandas as pd
    
    # Read file
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    
    # Convert to required format
    fd_data = []
    for _, row in df.iterrows():
        fd_record = {
            'bank': row['Bank'],
            'yield_per_annum': float(row['Yield_Per_Annum']),
            'minimum_amount': float(row['Minimum_Amount']),
            'term_months': int(row['Term_Months']),
            'penalty_severity': row['Penalty_Severity'],
            'penalty_description': row.get('Penalty_Description', ''),
            'source': row.get('Source', 'bulk_upload'),
            'currency': row.get('Currency', 'KES')
        }
        fd_data.append(fd_record)
    
    # Load data
    return loader._load_fixed_deposits(fd_data)

def process_mmf_file(filename, currency):
    """Process MMF CSV/Excel file"""
    import pandas as pd
    
    # Read file
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    
    # Convert to required format
    mmf_data = []
    for _, row in df.iterrows():
        mmf_record = {
            'company': row['Company'],
            'product_name': row['Product_Name'],
            'average_annual_yield': float(row['Average_Annual_Yield']),
            'withholding_tax': float(row.get('Withholding_Tax', 15.0)),
            'minimum_investment': float(row['Minimum_Investment']),
            'management_fee': float(row['Management_Fee']),
            'currency': currency
        }
        
        # Calculate net yield
        gross_yield = mmf_record['average_annual_yield']
        withholding_tax = mmf_record['withholding_tax']
        management_fee = mmf_record['management_fee']
        
        net_yield = (gross_yield - management_fee) * (1 - withholding_tax / 100)
        mmf_record['net_yield'] = net_yield
        
        mmf_data.append(mmf_record)
    
    # Load data
    return loader._load_money_market_funds(mmf_data)

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    # Create static directory for CSS/JS
    os.makedirs('static', exist_ok=True)
    
    print("🌐 Starting MMF & Fixed Deposit Web Application...")
    print("📊 Dashboard: http://localhost:5000")
    print("🏦 Add Fixed Deposit: http://localhost:5000/add_fixed_deposit")
    print("📈 Add MMF: http://localhost:5000/add_mmf")
    print("📤 Bulk Upload: http://localhost:5000/bulk_upload")
    print("🔌 API Rates: http://localhost:5000/api/rates")
    
    app.run(debug=True, host='0.0.0.0', port=5001)