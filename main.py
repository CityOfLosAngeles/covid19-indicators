"""
Entrypoint script to schedule notebook 
and send email. 

Author: @hunterowens
"""
import datetime
import os
import sys
import time

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import boto3
import civis
import pandas as pd
import papermill as pm 

sys.path.append(os.getcwd())
# check dir
if not os.path.exists('outputs'):
    os.makedirs('outputs') 

output_path = f'./outputs/{str(datetime.datetime.now().date())}-coronavirus-stats.ipynb'
output_path2 = f'./outputs/simpler-notebook2.ipynb'

print(os.environ.get("AWS_ACCESS_KEY_ID"))

# Try executing the notebook. If it fails due to data being incomplete,
# try again in an hour, for a maximum of ten hours.
MAX_TRIES = 10
RETRY = 60 * 60
for i in range(MAX_TRIES):
    try:
        pm.execute_notebook(
           '/app/notebooks/simpler-notebook.ipynb',
           output_path,
           cwd='/app/notebooks'
        )
        print("Ran notebook1")

        pm.execute_notebook(
           '/app/notebooks/simpler-notebook2.ipynb',
           output_path2,
           cwd='/app/notebooks'
        )
        print("Ran notebook2")
        break
    except pm.PapermillExecutionError as e:
        if "Data incomplete" in e.evalue:
            print(f"Data incomplete, trying again in {RETRY} seconds")
            time.sleep(RETRY)
        else:
            raise e
else:
    raise RuntimeError(f"Unable to get fresh data after {MAX_TRIES} tries.")

# shell out, run NB Convert 
output_format = 'PDFviaHTML'
cmd  = f"jupyter nbconvert --to {output_format} --no-input --no-prompt {output_path}"
cmd2 = f"jupyter nbconvert --to {output_format} --no-input --no-prompt {output_path2}"

output_file = f'./outputs/simpler-notebook2.pdf'

os.system(cmd)
print("Finish shelling #1")
os.system(cmd2)
print("Finish shelling #2")

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
BODY_TEXT = "Hello,\r\nPlease see the attached file for a status update on coronavirus-related indicators."

print(BODY_TEXT)

# The HTML body of the email.
BODY_HTML =  """\
<html>
<head></head>
<body>
<p>Please see the attached file for a status update on coronavirus-related indicators.</p>
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
