import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Send Email with Spark DataFrame") \
    .getOrCreate()

# Example DataFrame
data = [("John", 30), ("Jane", 25), ("Smith", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Convert DataFrame to Pandas DataFrame for easier handling
pandas_df = df.toPandas()

# Create message container
msg = MIMEMultipart()
msg['From'] = 'katsreen100@gmail.com'
msg['To'] = 'katsreen100@gmail.com'
msg['Subject'] = 'DataFrame as Attachment'

# Add text message to the email body
body = "Hello,\n\nPlease find the attached DataFrame.\n\nRegards,\nSender"
msg.attach(MIMEText(body, 'plain'))

# Create attachment
attachment = MIMEApplication(pandas_df.to_csv(index=False))
attachment.add_header('Content-Disposition', 'attachment', filename='data.csv')
msg.attach(attachment)

# Connect to Outlook SMTP server and send email
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'katsreen100@gmail.com'
smtp_password = 'Kuwait1@'

with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.send_message(msg)

print("Email sent successfully!")