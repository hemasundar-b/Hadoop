import smtplib
user=username
pass=password
server = smtplib.SMTP()
server.connect("smtp.outlook.office365.com",587)
server.ehlo()
server.starttls()
server.login(user,pass)
server.sendmail("hemasundar.battina@exusia.com","hemasundar.battina@exusia.com","this is a testmessage")
