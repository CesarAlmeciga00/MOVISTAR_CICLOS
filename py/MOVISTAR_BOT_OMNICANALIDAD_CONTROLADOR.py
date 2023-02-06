from LOAD_GESTION_BOT import *
gestionBot = gesBot
from LOAD_MASIVOS_SMS import *
lSms = sms
from LOAD_MASIVOS_EMAIL import *
lEmail = email

start_time = time.time()

ip = ""
port = "3306"
user = ""
password = ""
bbdd = ""
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connectionGesBot = sqlEngine.connect()
connectionSms = sqlEngine.connect()
connectionEmail = sqlEngine.connect()





gesBot = threading.Thread(target = gestionBot.loadGesBot, args = [connectionGesBot])
sms = threading.Thread(target = lSms.loadSms, args = [connectionSms])
email = threading.Thread(target = lEmail.loadEmail, args = [connectionEmail])

gesBot.start()
time.sleep(10)
sms.start()
time.sleep(5)
email.start()
gesBot.join()
sms.join()
email.join()

connectionGesBot.close()
connectionSms.close()
connectionEmail.close()



print("EJECUCIÓN BOT Y OMNICANALIDAD MOVISTAR COLOMBIA FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
