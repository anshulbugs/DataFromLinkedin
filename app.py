from flask import Flask, request, make_response, render_template
import psutil
import os
import pandas as pd
import requests
import io
import time
import csv
import codecs
import signal

app = Flask(__name__)

# Set a maximum request timeout (in seconds)
app.config['REQUEST_TIMEOUT'] = 30

# Set a maximum number of concurrent requests
app.config['MAX_CONCURRENT_REQUESTS'] = 10

# Initialize a variable to track the number of active requests
active_requests = 0

def get_process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 ** 2) 

def make_api_call(url):
    try:
        start_time = time.time()
        api_url = f'https://pulse.aptask.com/api/2.0/linkedin/get-details-from-linkedin-url?linkedInUrl={url}'
        with requests.get(api_url, stream=True, timeout=app.config['REQUEST_TIMEOUT']) as response:
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            
            # Process the streamed response
            response_data = response.json()
            
        end_time = time.time()
        time_taken = end_time - start_time
        print("API call took", time_taken, "seconds")
        return response_data
    except Exception as e:
        print(f'Error fetching details for {url}: {e}')
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_csv', methods=['POST'])
def process_csv():
    global active_requests

    # Check if maximum concurrent requests limit has been reached
    if active_requests >= app.config['MAX_CONCURRENT_REQUESTS']:
        return 'Error: Maximum number of concurrent requests reached. Please try again later.', 503

    # Increment the active requests counter
    active_requests += 1

    try:
        # Get the uploaded CSV file
        csv_file = request.files['file']

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)

        # Check if the 'LinkedIn Profile' column exists
        if 'LinkedIn Profile' not in df.columns:
            return 'Error: The CSV file does not have a "LinkedIn Profile" column.', 400

        # Open a new CSV file for writing with UTF-8 encoding
        with codecs.open('processed_data.csv', 'w', encoding='utf-8') as csvfile:
            fieldnames = df.columns.tolist() + ['emailsApi', 'phoneNumbersApi']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator='\n')
            writer.writeheader()

            for index, row in df.iterrows():
                url = row['LinkedIn Profile']
                response_data = make_api_call(url)
                if response_data:
                    email_str = ', '.join(response_data.get('emails', []))
                    phone_number_str = ', '.join(response_data.get('phoneNumbers', []))
                    row['emailsApi'] = email_str
                    row['phoneNumbersApi'] = phone_number_str

                # Convert the row to a dictionary of strings
                row_dict = {col: str(value) for col, value in row.items()}
                writer.writerow(row_dict)

        # Send the CSV file as a response
        with codecs.open('processed_data.csv', 'r', encoding='utf-8') as csvfile:
            csv_data = csvfile.read()
        response = make_response(csv_data)
        response.headers['Content-Disposition'] = 'attachment; filename=processed_data.csv'
        response.headers['Content-Type'] = 'text/csv; charset=utf-8'
        return response
    finally:
        # Decrement the active requests counter
        active_requests -= 1

if __name__ == '__main__':
    app.run(debug=True)
