import pandas as pd
import re
import requests
import boto3
from io import StringIO
from datetime import datetime
import json

def lambda_handler(event, context):
    # Retrieve text from the URL
    url = 'https://pdreport.slocity.org/policelog/rpcdsum.txt'
    response = requests.get(url)
    text = response.text

    # Filtering lines
    filtered_lines = []
    for line in text.split('\n'):
        if not all(char == '-' or char == '=' or char == '\'' or char == ';' or char == ',' or char == '•' or
                   char == '`' or char == '_' or char == '"' or char.isspace() for char in line):
            filtered_lines.append(line)
            if "CALL COMMENTS:" in line:
                filtered_lines.append("-" * len(line) + "\n")

    text = '\n'.join(filtered_lines)
    filtered_text_lines = text.split('\n')[3:]
    filtered_text = '\n'.join(filtered_text_lines)

    # Split text into incidents using the row of "-" signs as delimiter
    incidents = re.split(r'\n-{2,}', filtered_text)

    # Function to extract information for each incident
    def extract_incident_info(incident_text):
        incident_id = re.search(r'\b(\d{9})\b', incident_text)
        incident_id = incident_id.group(1) if incident_id else ''
        date = re.search(r'\b(\d{2}/\d{2}/\d{2})\b', incident_text)
        date = date.group(1) if date else ''
        received = re.search(r'Received:?(\d{2}:\d{2})', incident_text, re.IGNORECASE)
        received = received.group(1) if received else ''
        dispatched = re.search(r'Dispatched:?(\d{2}:\d{2})', incident_text, re.IGNORECASE)
        dispatched = dispatched.group(1) if dispatched else ''
        arrived = re.search(r'Arrived:?(\d{2}:\d{2})', incident_text, re.IGNORECASE)
        arrived = arrived.group(1) if arrived else ''
        cleared = re.search(r'Cleared:?(\d{2}:\d{2})', incident_text, re.IGNORECASE)
        cleared = cleared.group(1) if cleared else ''
        incident_type = re.search(r'Type:? (.+?)\s+Location:', incident_text, re.IGNORECASE)
        incident_type = incident_type.group(1) if incident_type else ''
        address = re.search(r'Addr:? (.+?)\s+Clearance Code:', incident_text, re.IGNORECASE)
        address = address.group(1) if address else ''
        comment = re.search(r'CALL COMMENTS:? (.+)', incident_text, re.IGNORECASE)
        comment = comment.group(1).strip() if comment else ''

        return [incident_id, date, received, dispatched, arrived, cleared, incident_type, address, comment]

    # Extract information for each incident and append to a list
    incident_data = [extract_incident_info(incident_text) for incident_text in incidents]

    # Create DataFrame
    df = pd.DataFrame(incident_data, columns=['IncidentID', 'Date', 'Received', 'Dispatched', 'Arrived', 'Cleared', 'Type', 'Address', 'Comment'])

    def extract_grid(address):
        match = re.search(r'GRID\s+([A-Z]-\d+)', address)
        if match:
            return match.group(1)
        else:
            return 'N/A'

    df['Address'] = df['Address'].fillna('')
    df['Grid'] = df['Address'].apply(extract_grid)

    # Save df to a CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    # Upload to S3
    s3_resource = boto3.resource('s3')
    bucket_name = '1aslopd'
    file_name = 'Final_Logs_Combined/police-log-{}.csv'.format(datetime.now().strftime('%Y-%m-%d'))
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())

    return {
        'statusCode': 200, 
        'body': json.dumps('Successfully processed and uploaded police log data.')
    }
