from importsAndFunctions import *


class pagos:
    def loadPagos(ip, user_, password, bbdd):
        start_time = time.time()
        try:
            connection = MySQLdb.Connect(host=ip, user=user_, passwd=password, db=bbdd)
            cursor = connection.cursor()
            try:
                truncate = "TRUNCATE bbdd_cs_bog_movistar_ciclos.tb_pagos_movistar_ciclos;"
                cursor.execute(truncate)
                connection.commit()
                set1 = "set global unique_checks = 0;"
                set2 = "set global foreign_key_checks = 0;"
                disable_key1 = "ALTER TABLE tb_pagos_movistar_ciclos drop index id_cliente;"
                disable_key2 = "ALTER TABLE tb_pagos_movistar_ciclos drop index id_fecha;"
                cursor.execute(set1)
                cursor.execute(set2)
                cursor.execute(disable_key1)
                cursor.execute(disable_key2)
                connection.commit()
            except:
                print("No existe el índice")
            
            try:
                query = """LOAD DATA LOCAL INFILE 'C:/MOVISTAR_CICLOS/02_CRUDOS/PAGOS/PAGOS_MOVIL.txt' 
                            INTO TABLE
                            tb_pagos_movistar_ciclos 
                            FIELDS TERMINATED BY '|' 
                            LINES TERMINATED BY '\n'
                            IGNORE 1 LINES
                            (cod_cliente, @var1, valor_pago, @var2)
                            SET fecha_pago = DATE_FORMAT(STR_TO_DATE(@var1, '%d/%m/%Y'), '%Y-%m-%d'),
                            clase_pago = if(@var2 = "", null, @var2);"""
                cursor.execute(query)
                connection.commit()
                set1 = "set global unique_checks = 1;"
                set2 = "set global foreign_key_checks = 1;"
                enable_key1 = "ALTER TABLE `bbdd_cs_bog_movistar_ciclos`.`tb_pagos_movistar_ciclos` ADD INDEX `id_cliente` USING BTREE (`cod_cliente`) VISIBLE;"
                enable_key2 = "ALTER TABLE `bbdd_cs_bog_movistar_ciclos`.`tb_pagos_movistar_ciclos` ADD INDEX `id_fecha` USING BTREE (`fecha_pago` DESC) VISIBLE;"
                cursor.execute(enable_key1)
                cursor.execute(enable_key2)
                cursor.execute(set1)
                cursor.execute(set2)
                connection.commit()
            except:
                print("No se completó el cargue")
            connection.close()
        except:
            print("Conexión errada")

        print("EJECUCIÓN LOAD_PAGOS FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
