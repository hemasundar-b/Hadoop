import smtplib
import email.utils
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart

fromaddr = "from Email Address"
toaddr = "to Email Address"
msg = MIMEMultipart()
msg['From'] = email.utils.formataddr(('Hemasundar Battina', fromaddr))
msg['To'] = toaddr
msg['Subject'] = "Python email"

body = "Python test mail"
msg.attach(MIMEText(body, 'plain'))

user=user
password =password

server = smtplib.SMTP()
server.connect("smtp.outlook.office365.com",587)
server.ehlo()
server.starttls()
server.login(user,password)
text = msg.as_string()
server.sendmail(fromaddr,toaddr,text)
