from importsAndFunctions import *

pd.options.mode.chained_assignment = None

link_gestiones = (r"C:\MOVISTAR_CICLOS\02_CRUDOS\GESTION")
os.chdir(link_gestiones)




ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_ciclos"
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connection = sqlEngine.connect()

xlsx = pd.ExcelFile('MOVISTAR_INBOUND_Detalle_gestion.xlsx', engine='openpyxl')
df_gestion = xlsx.parse('Report', skiprows=1, skipfooter = 2, index_col=None)

df_gestion['FECHA_GESTION'] = pd.to_datetime(df_gestion['FECHA_GESTION'], format="%d/%m/%Y")
df_gestion['HORA_GESTION'] = pd.to_datetime(df_gestion['HORA_GESTION'], format="%H:%M:%S")

df_gestion['FECHA_FIN_GESTION'] = pd.to_datetime(df_gestion['FECHA_GESTION'], format="%d/%m/%Y")
df_gestion['HORA_FIN_GESTION'] = pd.to_datetime(df_gestion['HORA_GESTION'], format="%H:%M:%S")

df_gestion['COD_CUENTA'] = df_gestion['COD_CUENTA'].fillna(1)
df_gestion['COD_CUENTA'] = df_gestion['COD_CUENTA'].astype('string')
df_gestion['COD_CUENTA'] = df_gestion['COD_CUENTA'].str[:10]

df_gestion['COD_CUENTA'] = df_gestion.COD_CUENTA.str.findall('[0-9]+').apply(''.join)


delete_df = df_gestion[['FECHA_GESTION']]
delete_df['FECHA_GESTION'] = pd.to_datetime(delete_df['FECHA_GESTION'], format="%d/%m/%Y")
# delete_df['FECHA_GESTION'] = delete_df['FECHA_GESTION'].dt.strftime('%Y-%m-%d')

delete_df = delete_df.drop_duplicates()
delete_df = delete_df.reset_index()


for index, row in delete_df.iterrows():
    query_delete = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_pentaho_gestion_movistar_ciclos`
    WHERE FECHA_GESTION = '""" + str(row['FECHA_GESTION']) + """';"""
    connection.execute(query_delete)


df_gestion.to_sql("tb_pentaho_gestion_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)

connection.close()

