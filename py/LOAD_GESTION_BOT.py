from importsAndFunctions import *



class gesBot:
    def loadGesBot(connection):

        start_time = time.time()
        df_gestionBot = pd.DataFrame()

       
        try:
            fileDir("C:\\MOVISTAR_CICLOS\\02_CRUDOS\\GESTION")
        except:
            print("Archivo no encontrado")
        try:
            df_gestionBot = pd.read_csv("GESTIONES_BOT.csv", encoding='latin-1', sep=';', quoting=csv.QUOTE_NONE, on_bad_lines = 'warn')
            
        except:
            print("Estructara de archivo gestiones bot incorrecta")

        df_gestionBot['FECHA'] = pd.to_datetime(df_gestionBot['FECHA'], format="%d/%m/%Y")

        delete_df = df_gestionBot[['FECHA']]
        delete_df['FECHA'] = pd.to_datetime(delete_df['FECHA'], format="%d/%m/%Y")
        delete_df['FECHA'] = delete_df['FECHA'].dt.strftime('%Y-%m-%d')

        delete_df = delete_df.drop_duplicates()
        delete_df = delete_df.reset_index()

        try:
            for index, row in delete_df.iterrows():
                query_delete = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_gestion_bot_movistar_ciclos`
                WHERE FECHA = '""" + str(row['FECHA']) + """';"""
                connection.execute(query_delete)
        except:
            print("la tabla tb_gestion_bot_movistar_ciclos no existe")

        try:
            df_gestionBot.to_sql("tb_gestion_bot_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)
        except:
            print("No se completó el cargue")
   

        

        print("EJECUCIÓN LOAD_GESTION_BOT FINALIZADA. \nTIEMPO TOTAL EMPLEADO:" + "--- %s segundos ---" % (time.time() - start_time))
        

