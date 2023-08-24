import os.path
import boto3
import email
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# getting the environment variables from lambda config
sns_notify = os.environ['SNSTopicArn']
env = os.environ["ENVIRONMENT"]
sender = os.environ["SENDER"]
recipients = os.environ["RECIPIENT"].split(',') # converting env variable which is in string to list
recipients_str = ", ".join(recipients) # just used for logging purpose


# Create low level resource service clients for S3 ans SES
s3Client = boto3.client("s3")
sesClient = boto3.client("ses", region_name="us-west-2")
snsClient = boto3.client("sns", region_name="us-west-2")

# Setting up logger for event logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

utc_now_str = datetime.now().strftime('%Y-%m-%d')


def notifymsg(sub, msg):
    snsClient.publish(TopicArn=sns_notify, Message=msg, Subject=sub)
    logger.info("**************** [INFO] SNS Notification Sent *************************")


def lambda_handler(event, context):
    """
    sends bulk email with attachments(.xslx/.pdf/.doc/.ppt) to recipients provided.
    """
    # retrieving file name and bucket name from s3 event
    fileobj = event["Records"][0]
    bucket_name = str(fileobj['s3']['bucket']['name'])
    key = str(fileobj['s3']['object']['key'])
    # if there is no s3 event based trigger and want to run code manually, hardcode below values as:
    # bucket_name = "whg-s3-usw2a-blueterminal-dev"
    # key = "pms_stay/stay_data_error_table/stay_data_error_report/stay_data_error_report_20220404.xlsx"

    file_name = os.path.basename(key)
    tmp_file_name = '/tmp/' + file_name
    s3Client.download_file(bucket_name, key, tmp_file_name)
    attachment = tmp_file_name
    logger.info(f"attaching the report: {attachment}")

    subject = f"Stay Data Errors Report: {env}"
    body_text = f"Hello All,\n\nPlease find the attached file for 'stay data errors' report generated on {utc_now_str} (UTC)."

    msg = MIMEMultipart()
    # Add subject, from and to lines.
    msg['subject'] = subject
    msg['From'] = sender
    # msg['To'] = recipients # this line is throwing error. So commented. Infact, we dont need it.
    textpart = MIMEText(body_text)
    msg.attach(textpart)
    att = MIMEApplication(open(attachment, 'rb').read())
    # attachment will now have prefix 'tmp', so get rid of it using 'replace' function
    att.add_header('Content-Disposition', 'attachment', filename=attachment.replace("tmp", ""))
    msg.attach(att)
    # logger.info(f"msg is: {msg}") # here, msg will be printed in binary format
    try:
        response = sesClient.send_raw_email(
            Source=sender,
            Destinations=recipients,
            RawMessage={'Data': msg.as_string()}
        )
    except Exception as e:
        logger.exception(e)
        f_msg = f"Unable to trigger the email for 'stay data errors' report beacuse of the failure: \n{str(e)}"
        f_sub = "Stay data errors report Email Notification failed"
        logger.info("trying to send failure alert")
        notifymsg(f_sub, f_msg)
        raise SystemExit(e)
    else:
        logger.info(f"Email sent to recipients: {recipients_str} \nMessage ID is: {response['MessageId']}")