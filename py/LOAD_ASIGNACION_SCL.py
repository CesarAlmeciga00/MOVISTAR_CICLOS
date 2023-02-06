from importsAndFunctions import *



class asignacionScl:
    
    def loadAsgScl(connection):
        start_time = time.time()
        df_asignacion = pd.DataFrame()

        try:
            try:
                fileDir("C:\\MOVISTAR_CICLOS\\02_CRUDOS\\ASIGNACION\\SCL")
            except:
                print("No se encuentra la ruta")

            try:
                for filename in glob.glob("*Bloqueo*.txt"):
                    read_txt = pd.read_csv(filename, encoding='utf-8', sep='|', quoting=csv.QUOTE_NONE)
                    read_txt['FILENAME'] = filename
                    df_asignacion = pd.concat([read_txt, df_asignacion], axis=0)

                df_asignacion['CICLO'] = df_asignacion.FILENAME.str.findall('C[0-9]+').apply(''.join)
                df_asignacion['FECHA_ASIGNACION'] = df_asignacion.FILENAME.str.findall('_[0-9]+').apply(''.join)
                df_asignacion = df_asignacion.drop('FILENAME', axis=1)
                df_asignacion = df_asignacion.drop_duplicates(subset='COD_CLIENTE', keep="last")

            except:
                print("Estructura erronea/No se encontró el archivo")

            try:
                sql = "TRUNCATE TABLE tb_asignacion_scl_hoy_movistar_ciclos;"
                executeQuery(sql, connection)
            except:
                print("No se pudo truncar la tabla tb_asignacion_scl_hoy_movistar_ciclos")

            try:
                df_asignacion.to_sql("tb_asignacion_scl_hoy_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)
            except:
                print("No se completó el cargue")
        except:
            print("ejecución fallida")
        
        print("EJECUCIÓN LOAD_ASIGNACION_SCL FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
            


        


