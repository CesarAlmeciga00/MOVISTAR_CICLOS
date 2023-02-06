from importsAndFunctions import *

class carteras:
  def loadCarteras(ip, user_, password, bbdd):
    start_time = time.time()
    try:
      fileDir("C:\\MOVISTAR_CICLOS\\02_CRUDOS\\CARTERAS")

      # Read in the file
      with open('SPOOL_SALIDA_CARTERA_FS.txt', 'r') as file :
        filedata = file.read()
      # Replace the target string
      filedata = filedata.replace('| ', '')
      # Write the file out again
      with open('SPOOL_SALIDA_CARTERA_FS.txt', 'w') as file:
        file.write(filedata)
    except:
        print("No existe el archivo")

    try:
      connection = MySQLdb.Connect(host=ip, user=user_, passwd=password, db=bbdd)
      cursor = connection.cursor()
      try:
        truncate = "TRUNCATE bbdd_cs_bog_movistar_ciclos.tb_cartera_fs_movistar_ciclos;"
        disable_key = "ALTER TABLE tb_cartera_fs_movistar_ciclos drop index id_cuenta;"
        cursor.execute(truncate)
        cursor.execute(disable_key)
        connection.commit()
      except:
        print("No existe el índice")

      try:  
        set1 = "set global unique_checks = 0;"
        set2 = "set global foreign_key_checks = 0;"
        cursor.execute(set1)
        cursor.execute(set2)
        connection.commit()
        query = "LOAD DATA LOCAL INFILE 'C:/MOVISTAR_CICLOS/02_CRUDOS/CARTERAS/SPOOL_SALIDA_CARTERA_FS.txt' INTO TABLE tb_cartera_fs_movistar_ciclos FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
        cursor.execute(query)
        connection.commit()
        set1 = "set global unique_checks = 1;"
        set2 = "set global foreign_key_checks = 1;"
        enable_key = "ALTER TABLE `bbdd_cs_bog_movistar_ciclos`.`tb_cartera_fs_movistar_ciclos` ADD INDEX `id_cuenta` USING BTREE (`CUENTA_FS`) VISIBLE;"
        cursor.execute(set1)
        cursor.execute(set2)
        cursor.execute(enable_key)
        connection.commit()
      except:
        print("No se completó el cargue")
      connection.close()
    except:
      print("Conexión errada")


    print("EJECUCIÓN LOAD_CARTERAS FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
