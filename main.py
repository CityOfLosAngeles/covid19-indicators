"""
Entrypoint script to schedule notebook 
and send email. 

Author: @hunterowens
"""

import papermill as pm 
import pandas as pd
import datetime
import civis
import os

# check dir
if not os.path.exists('outputs'):
    os.makedirs('outputs') 

output_path = f'./outputs/{str(datetime.datetime.now().date())}-corona.ipynb'
# execute notebook 
pm.execute_notebook(
   './notebooks/coronavirus-indicators.ipynb',
   output_path,
   parameters=dict(start_date="4/15/2020")
)

# shell out, run NB Convert 

cmd  = f"jupyter nbconvert --to html --no-input --no-prompt {output_path}"

output_file = f'./outputs/{str(datetime.datetime.now().date())}-corona.html'

os.system(cmd)
# send an email with the HTML 
html = open(output_file, 'r').read().splitlines()

import boto3
from botocore.exceptions import ClientError

# Replace sender@example.com with your "From" address.
# This address must be verified with Amazon SES.
SENDER = "ITAData@lacity.org"

# Replace recipient@example.com with a "To" address. If your account 
# is still in the sandbox, this address must be verified.
RECIPIENT = "itadata@lacity.org"

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-west-2"

# The subject line for the email.
SUBJECT = "Amazon SES Test (SDK for Python)"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Amazon SES Test (Python)\r\n"
             "This email was sent with Amazon SES using the "
             "AWS SDK for Python (Boto)."
            )

BODY_HTML = "<p>test</p>"

                  

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses',region_name=AWS_REGION)

# Try to send the email.
try:
    #Provide the contents of the email.
    response = client.send_email(
        Destination={
            'ToAddresses': [
                RECIPIENT,
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=SENDER,
        # If you are not using a configuration set, comment or delete the
        # following line
    )
# Display an error if something goes wrong.	
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:"),
    print(response['MessageId'])
            