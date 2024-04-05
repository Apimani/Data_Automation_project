import smtplib

smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'katsreen100@gmail.com'
smtp_password = 'Kuwait1@'

try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    print("SMTP server configuration is correct.")
    server.quit()
except Exception as e:
    print("SMTP server configuration is incorrect:", e)
