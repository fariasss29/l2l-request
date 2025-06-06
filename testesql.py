import pyodbc
import pandas as pd

server   = 'cafds401'
database = 'MDACESSO'
username = 'MDREAD'
password = 'Eu2y10@qVo5p'
driver_name = 'SQL Server'  

conn_str = (
    f"DRIVER={{{driver_name}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)

sql = """
    SELECT
        NU_MATRICULA   AS NU_MATRICULA,
        NM_PESSOA      AS NM_PESSOA,
        CD_EQUIPAMENTO AS CD_EQUIPAMENTO,
        DS_EQUIPAMENTO AS DS_EQUIPAMENTO,
        DT_REQUISICAO  AS DT_REQUISICAO
    FROM LOG_ACESSO la
    WHERE 
        la.NU_DATA_REQUISICAO = CONVERT(INT, CONVERT(varchar(8), GETDATE(), 112))
    AND la.DS_AREA_DESTINO = 'Externa Portaria'
    AND la.CD_EQUIPAMENTO IN (11, 12, 13, 14)
"""

df = pd.read_sql(sql, conn)
print(df.head())
conn.close()
