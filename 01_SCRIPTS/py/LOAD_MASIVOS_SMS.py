from importsAndFunctions import *

class sms:
        
    def loadSms(connection):

        start_time = time.time()

        
        fileDir("C:\\MOVISTAR_CICLOS\\02_CRUDOS\\MASIVOS")
        try:
            xlsx =pd.ExcelFile('SMS.xlsx', engine='openpyxl')
            df_masivos = xlsx.parse(index_col=None)
            df_masivos  = df_masivos.iloc[: , :35]
            df_masivos['Communication Start Date'] = pd.to_datetime(df_masivos['Communication Start Date'], format="%d/%m/%Y %H:%M:%S")
            df_masivos['Send At'] = pd.to_datetime(df_masivos['Send At'], format="%d/%m/%Y %H:%M:%S")
            df_masivos['Done At'] = pd.to_datetime(df_masivos['Done At'], format="%d/%m/%Y %H:%M:%S")



            delete_df = df_masivos[['Send At']]
            delete_df['Send At'] = pd.to_datetime(delete_df['Send At'], format="%d/%m/%Y %H:%M:%S")
            delete_df['Send At'] = delete_df['Send At'].dt.strftime('%Y-%m-%d')

            delete_df = delete_df.drop_duplicates()
            delete_df = delete_df.reset_index()
        except:
            print("Error en lectura de archivo")


        try:

            for index, row in delete_df.iterrows():
                query_delete = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_masivos_sms_movistar_ciclos`
                    WHERE date(`SEND_AT`) = date('"""+str(row['Send At'])+"""');"""
                connection.execute(query_delete)
        except: 
            print("Error en eliminación de información")


        try:

            df_masivos.rename(columns = {'Send At':'SEND_AT'}, inplace = True)
            df_masivos.rename(columns = {'To':'To_'}, inplace = True)

            df_masivos.to_sql("tb_masivos_sms_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)
        except:
            print("Error en cargue de información")

        

        print("--- %s seconds ---" % (time.time() - start_time))