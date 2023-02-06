from LOAD_CARTERAS import *
cartera = carteras
from LOAD_PAGOS import *
pago = pagos
from LOAD_PAGOS_VAC import *
pagovac = pagosVac

start_time = time.time()

ip = ""
port = ""
user = ""
password = ""
bbdd = ""




carteras = threading.Thread(target = cartera.loadCarteras, args = (ip, user, password, bbdd))
pagos = threading.Thread(target = pago.loadPagos, args = (ip, user, password, bbdd))
pagosVac = threading.Thread(target = pagovac.loadPagosVac, args = (ip, user, password, bbdd))

carteras.start()
time.sleep(10)
pagos.start()
time.sleep(10)
pagosVac.start()
time.sleep(10)
carteras.join()
pagos.join()
pagosVac.join()





print("EJECUCIÓN PAGOS-CARTERA MOVISTAR COLOMBIA FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
