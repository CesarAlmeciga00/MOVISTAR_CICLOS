from importsAndFunctions import *


start_time = time.time()

pd.options.mode.chained_assignment = None

link_gestiones = (r"C:\MOVISTAR_CICLOS\02_CRUDOS\VARIOS")
os.chdir(link_gestiones)




ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_ciclos"
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connection = sqlEngine.connect()

xlsx = pd.ExcelFile('ARBOL.xlsx', engine='openpyxl')
df_arbol = xlsx.parse(index_col=None)

sql = "TRUNCATE TABLE tb_arbol_tipificacion_movistar_ciclos;"
executeQuery(sql, connection)

df_arbol.to_sql("tb_arbol_tipificacion_movistar_ciclos", connection, if_exists = 'append', index=None, chunksize=10000)

connection.close()

print("--- %s seconds ---" % (time.time() - start_time))