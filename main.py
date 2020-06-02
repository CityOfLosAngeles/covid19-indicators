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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3

import os, sys
sys.path.append(os.getcwd())
# check dir
if not os.path.exists('outputs'):
    os.makedirs('outputs') 

output_path = f'./outputs/{str(datetime.datetime.now().date())}-corona.ipynb'
# execute notebook 
pm.execute_notebook(
   '/app/notebooks/county-city-indicators.ipynb',
   output_path,
   parameters=dict(start_date="4/15/2020"),
   cwd='/app/notebooks'
)

# shell out, run NB Convert 
output_format = 'PDFviaHTML'
cmd  = f"jupyter nbconvert --to {output_format} --no-input --no-prompt {output_path}"

output_file = f'./outputs/{str(datetime.datetime.now().date())}-corona.pdf'

os.system(cmd)

# Replace sender@example.com with your "From" address.
# This address must be verified with Amazon SES.
SENDER = "ITAData@lacity.org"

# Replace recipient@example.com with a "To" address. If your account 
# is still in the sandbox, this address must be verified.
RECIPIENT = "itadata@lacity.org"


# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-west-2"

# The subject line for the email.
SUBJECT = f"Coronavirus Stats for {str(datetime.datetime.now().date())}"

# The full path to the file that will be attached to the email.
ATTACHMENT = output_file

# The email body for recipients with non-HTML email clients.
BODY_TEXT = "Hello,\r\nPlease see the attached file for a status update on Coronavirus related indicators."

# The HTML body of the email.
BODY_HTML =  """\
<html>
<head></head>
<body>
<p>Please see the attached file for a status update on Coronavirus related indicators</p>
</body>
</html>
"""

# The character encoding for the email.
CHARSET = "utf-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses',region_name=AWS_REGION)

# Create a multipart/mixed parent container.
msg = MIMEMultipart('mixed')
# Add subject, from and to lines.
msg['Subject'] = SUBJECT 
msg['From'] = SENDER 
msg['To'] = RECIPIENT

# Create a multipart/alternative child container.
msg_body = MIMEMultipart('alternative')

# Encode the text and HTML content and set the character encoding. This step is
# necessary if you're sending a message with characters outside the ASCII range.
textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)

# Add the text and HTML parts to the child container.
msg_body.attach(textpart)
msg_body.attach(htmlpart)

# Define the attachment part and encode it using MIMEApplication.
att = MIMEApplication(open(ATTACHMENT, 'rb').read())

# Add a header to tell the email client to treat this part as an attachment,
# and to give the attachment a name.
att.add_header('Content-Disposition','attachment',filename=os.path.basename(ATTACHMENT))

# Attach the multipart/alternative child container to the multipart/mixed
# parent container.
msg.attach(msg_body)

# Add the attachment to the parent container.
msg.attach(att)
#print(msg)
try:
    #Provide the contents of the email.
    response = client.send_raw_email(
        Source=SENDER,
        Destinations=[
            RECIPIENT
        ],
        RawMessage={
            'Data':msg.as_string(),
        }
    )
# Display an error if something goes wrong.	
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:"),
    print(response['MessageId'])
