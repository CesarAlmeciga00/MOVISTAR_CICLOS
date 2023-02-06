from LOAD_ASIGNACION_SCL import *
asgScl = asignacionScl
from LOAD_ASIGNACION_FS import *
asgFs = asignacionFs

start_time = time.time()

ip = ""
port = ""
user = ""
password = ""
bbdd = ""
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connectionSp1 = sqlEngine.connect()
connectionSp2 = sqlEngine.connect()
connectionScl = sqlEngine.connect()
connectionFs = sqlEngine.connect()



asignacionScl = threading.Thread(target = asgScl.loadAsgScl, args = [connectionScl])
asignacionFs = threading.Thread(target = asgFs.loadAsgFs, args = [connectionFs])


asignacionScl.start()
time.sleep(3)
asignacionFs.start()
asignacionScl.join()
asignacionFs.join()

connectionScl.close()
connectionFs.close()




spUnion = "CALL `bbdd_cs_bog_movistar_ciclos`.`sp_union_asignaciones_movistar_ciclos`();"
executeQuery(spUnion, connectionSp1)


spFinal = "CALL `bbdd_cs_bog_movistar_ciclos`.`sp_final_movistar_ciclos`();"
executeQuery(spFinal, connectionSp2)


connectionSp1.close()
connectionSp2.close()




print("EJECUCIÓN ASIGNACIÓN MOVISTAR CICLOS FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
