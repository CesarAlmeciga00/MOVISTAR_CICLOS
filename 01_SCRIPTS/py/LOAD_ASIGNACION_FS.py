from importsAndFunctions import *



class asignacionFs:
    def loadAsgFs(connection):
        start_time = time.time()

        try:
            fileDir("C:\\MOVISTAR_CICLOS\\02_CRUDOS\\ASIGNACION\\FS")
            os.system('del *.txt')
            os.system(r'"C:\Program Files\7-Zip\7z.exe" e *.zip')
            os.system('del *pyme*.txt')
        except:
            print("No se encuentra la ruta")


        try:
            read_txt = [pd.read_csv(filename, encoding='utf-8', sep='|', quoting=csv.QUOTE_NONE, on_bad_lines = 'warn') for filename in glob.glob("*Bloqueo*.txt")]
            df_asignacion = pd.concat(read_txt, axis=0)
            df_asignacion = df_asignacion.drop_duplicates(subset='NUM_CELULAR', keep="last")
            df_asignacion['FEC_ESTADO'] = pd.to_datetime(df_asignacion['FEC_ESTADO'], format="%d/%m/%Y")
            df_asignacion['FEC_ESTADO_CRM'] = pd.to_datetime(df_asignacion['FEC_ESTADO_CRM'], format="%d/%m/%Y")
            df_asignacion['FEC_ASIGNA'] = pd.to_datetime(df_asignacion['FEC_ASIGNA'], format="%d/%m/%Y")
            df_asignacion['FEC_CIERRE'] = pd.to_datetime(df_asignacion['FEC_CIERRE'], format="%d/%m/%Y")
            df_asignacion['FEC_VENCIMIENTO'] = pd.to_datetime(df_asignacion['FEC_VENCIMIENTO'], format="%d/%m/%Y")
        except:
            print("Estructura erronea/No se encontró el archivo")
        
        try:
            sql = "TRUNCATE TABLE tb_asignacion_fs_hoy_movistar_ciclos;"
            executeQuery(sql, connection)
            df_asignacion.to_sql("tb_asignacion_fs_hoy_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)
        except:
            print("No se pudo truncar la tabla tb_asignacion_fs_hoy_movistar_ciclos")


        print("EJECUCIÓN LOAD_ASIGNACION_FS FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))