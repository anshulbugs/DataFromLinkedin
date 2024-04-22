from flask import Flask, request, make_response, render_template
import pandas as pd
import requests
import io
import time
import csv
import codecs

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_csv', methods=['POST'])
def process_csv():
    # Get the uploaded CSV file
    csv_file = request.files['file']

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Check if the 'LinkedIn Profile' column exists
    if 'LinkedIn Profile' not in df.columns:
        return 'Error: The CSV file does not have a "LinkedIn Profile" column.', 400

    # Open a new CSV file for writing with UTF-8 encoding
    with codecs.open('processed_data.csv', 'w', encoding='utf-8', newline='') as csvfile:
        fieldnames = df.columns.tolist() + ['emailsApi', 'phoneNumbersApi']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for index, row in df.iterrows():
            url = row['LinkedIn Profile']
            try:
                start_time = time.time()
                api_url = f'https://pulse.aptask.com/api/2.0/linkedin/get-details-from-linkedin-url?linkedInUrl={url}'
                response = requests.get(api_url, stream=True)  # Enable streaming
                response.raise_for_status()  # Raise an exception for non-2xx status codes

                # Process the streamed response
                response_data = response.json()
                email_str = ', '.join(response_data.get('emails', []))
                phone_number_str = ', '.join(response_data.get('phoneNumbers', []))

                row['emailsApi'] = email_str
                row['phoneNumbersApi'] = phone_number_str

                # Convert the row to a dictionary of strings
                row_dict = {col: str(value) for col, value in row.items()}
                writer.writerow(row_dict)

                end_time = time.time()
                time_taken = end_time - start_time
                print("API call took", time_taken, "seconds")
            except Exception as e:
                print(f'Error fetching details for {url}: {e}')
                # Write the row with empty email and phone number fields
                row['emailsApi'] = ''
                row['phoneNumbersApi'] = ''

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

if __name__ == '__main__':
    app.run(debug=True)