# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 09:06:23 2025

@author: kelangovan
"""

from flask import Flask, request, render_template, send_file
import pandas as pd
import glob
import os
from werkzeug.utils import secure_filename
from datetime import datetime
import pytz

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "converted_data"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# Ensure upload and output directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Fetch form data
        wd = request.form['directory']  # Directory path entered by the user
        column_date = request.form['column_date']
        format_date = request.form['format_date']
        date_in_columns = int(request.form['date_in_columns'])
        date_columns = request.form['date_columns']
        date_columns = {item.split(":")[0]: item.split(":")[1] for item in date_columns.split(",")} if date_columns else {}
        resample = int(request.form['resample'])
        res = request.form['res']
        convert_from_utc = int(request.form['convert_from_utc'])
        tz = int(request.form['tz'])
        header_row = int(request.form['header_row'])
        skiprows_number = list(map(int, request.form['skiprows_number'].split(",")))
        rename_from = request.form['rename_from']
        rename_to = request.form['rename_to']
        rename_column_names_irr = {rename_from: rename_to} if rename_from and rename_to else {}
        drop_column_names = request.form['drop_column_names'].split(",") if request.form['drop_column_names'] else []
        file_identifier = request.form['file_identifier']
        separator = request.form['separator']
        
        try:
            # Call process_file with the selected directory and other arguments
            output_filepath = process_file(
                wd=wd,  # Use the provided working directory
                column_date=column_date,
                format_date=format_date,
                date_in_columns=date_in_columns,
                date_columns=date_columns,
                resample=resample,
                res=res,
                convert_from_utc=convert_from_utc,
                tz=tz,
                header_row=header_row,
                skiprows_number=skiprows_number,
                rename_column_names_irr=rename_column_names_irr,
                drop_column_names=drop_column_names,
                file_identifier=file_identifier,
                separator=separator
            )
            return send_file(output_filepath, as_attachment=True)
        except Exception as e:
            return f"Error processing file: {str(e)}"
    return '''
    <!doctype html>
    <html>
        <body>
            <h2>Upload a file for conversion</h2>
            <form action="/" method="post" enctype="multipart/form-data">
                <label>Directory Path:</label> <input type="text" name="directory" placeholder="Enter directory path"><br>
                <br>
                <label>Column Date:</label> <input type="text" name="column_date" value="Timestamp"><br>
                <br>
                <label>Date Format:</label> <input type="text" name="format_date" value="%Y-%m-%d %H:%M"><br>
                <br>
                <label>Date In Columns:</label> <input type="number" name="date_in_columns" value="0"><br>
                <br>
                <label>Date Columns (comma separated, e.g., '%YYYY:year,MO:month'): </label>
                <input type="text" name="date_columns" value="'%YYYY':'year','MO':'month','DA':'day','HO':'hour','MI':'minute'"><br>
                <br>
                <label>Resample (1 for yes, 0 for no):</label> <input type="number" name="resample" value="0"><br>
                <br>
                <label>Resample Interval (e.g., '1min'):</label> <input type="text" name="res" value="1min"><br>
                <br>
                <label>Convert from UTC (1 for yes, 0 for no):</label> <input type="number" name="convert_from_utc" value="0"><br>
                <br>
                <label>Time Zone Offset (e.g., 0 for UTC, 3 for UTC+03:00):</label> <input type="number" name="tz" value="0"><br>
                <br>
                <label>Header Row:</label> <input type="number" name="header_row" value="0"><br>
                <br>
                <label>Skip Rows (comma separated):</label> <input type="text" name="skiprows_number" value="1"><br>
                <br>
                <label>Rename Column:</label> <input type="text" name="rename_from" placeholder="Old Name"> → <input type="text" name="rename_to" placeholder="New Name"><br>
                <br>
                <label>Drop Columns (comma separated):</label> <input type="text" name="drop_column_names" value="Comments"><br>
                <br>
                <label>File Identifier (e.g., *.csv):</label> <input type="text" name="file_identifier" value="*.csv"><br>
                <br>
                <label>Separator:</label> <input type="text" name="separator" value=","><br>
                <br>
                <br>
                <input type="submit" value="Convert Files">
            </form>
        </body>
    </html>
    '''

def process_file(wd, column_date, format_date, date_in_columns, date_columns, resample, res, 
                  convert_from_utc, tz, header_row, skiprows_number, rename_column_names_irr, 
                  drop_column_names, file_identifier, separator):
    # Use glob to find all files matching the pattern in the specified working directory
    files = glob.glob(wd + file_identifier)

    # Read each file into a DataFrame and combine them
    dfs = []
    for f in files:
        if f.endswith('.xlsx'):
            dfi = pd.read_excel(f, header=header_row, skiprows=skiprows_number, sheet_name=None)
        else:
            dfi = pd.read_csv(f, sep=separator, header=header_row, skiprows=skiprows_number, encoding='unicode_escape')
        
        dfs.append(dfi)

    # Combine all DataFrames into one
    df = pd.concat(dfs, ignore_index=True)
    
    # Handle Date Columns (e.g., separate year, month, day, hour, minute)
    if date_in_columns:
        df['year'] = df[date_columns.get('%YYYY', 'year')]
        df['month'] = df[date_columns.get('MO', 'month')]
        df['day'] = df[date_columns.get('DA', 'day')]
        df['hour'] = df[date_columns.get('HO', 'hour')]
        df['minute'] = df[date_columns.get('MI', 'minute')]
        df[column_date] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
    
    # Convert date format
    df[column_date] = pd.to_datetime(df[column_date], format=format_date, errors='coerce')
    
    # Handle resampling if needed
    if resample:
        df.set_index(column_date, inplace=True)
        df = df.resample(res).mean()  # Resampling by the given frequency
    
    # Timezone Conversion (if specified)
    if convert_from_utc:
        df[column_date] = df[column_date].dt.tz_localize('UTC').dt.tz_convert(pytz.FixedOffset(tz*60))  # Convert UTC to given timezone
    
    # Drop specified columns
    df.drop(columns=[col for col in drop_column_names if col in df.columns], inplace=True)
    
    # Rename columns if needed
    df.rename(columns=rename_column_names_irr, inplace=True)
    
    # Create the header line as in your script
    header = [
        "# header",  # Modify this to whatever header text you want
    ]
    header = '\n'.join(header) + '\n'
    
    # Ensure the directory exists
    output_dir = os.path.join(wd, "01_converted_data")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Extract the base name from the first file to create the output filename
    base_name = os.path.basename(files[0])
    # Strip the leading directory and identifier part and ensure correct extraction
    base_name_cleaned = base_name.replace(wd, '').replace(file_identifier, '').strip('-')
    
    # Remove the file extension (.csv, .xlsx, etc.)
    base_name_cleaned = os.path.splitext(base_name_cleaned)[0]  # This removes the file extension
    
    # If there is a proper base filename, use it; otherwise, fallback to using the original filename
    output_filename = base_name_cleaned + ".dat" if base_name_cleaned else base_name + ".dat"
    
    output_filepath = os.path.join(output_dir, output_filename)
    
    # Save the processed file as .dat with the header line
    with open(output_filepath, "w", newline='') as f:
        # Write the header lines at the top of the file
        f.write(header)
        # Write the DataFrame content to the file, without the row index
        df.to_csv(f, index=False, header=df.columns.values, na_rep='nan', escapechar='"')
    
    print(f"Saved {output_filepath}")
    return output_filepath

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000))
