import urllib3
import requests
import pandas as pd
import pyodbc
from datetime import datetime

# para tirar as mensagem chata de segurança de http 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

L2L_API_BASE = "https://astemo-am.leading2lean.com/api/1.0"
L2L_API_KEY  = "lBfcswtYB4pQUEKLnBv39jxtA2doZRxV"
SITE_ID      = 2950

SERVER_NAME   = "cafds401"
DATABASE_NAME = "MDACESSO"
USERNAME      = "MDREAD"
PASSWORD      = "Eu2y10@qVo5p"
DRIVER_NAME   = "SQL Server"

CONN_STR = (
    f"DRIVER={{{DRIVER_NAME}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)
CONN = pyodbc.connect(CONN_STR)

SQL_CATRAÇA_SAÍDA = """
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

# aqui ele lê as saídas na catraca
def get_saidas_catraca_df() -> pd.DataFrame:
    df = pd.read_sql(SQL_CATRAÇA_SAÍDA, CONN)
    if df.empty:
        return df
    # converte NU_MATRICULA para int para remover o .0, depois transforma em string e normaliza
    df["NU_MATRICULA"] = df["NU_MATRICULA"].astype(float).astype("Int64")
    df["matricula_UP"] = (
        df["NU_MATRICULA"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.strip()
        .str.upper()
    )
    return df

# chama API para pegar dispatches atuais
def get_open_dispatches_df_real() -> pd.DataFrame:
    url = f"{L2L_API_BASE}/dispatches/current_dispatched_resources/"
    params = {
        "auth": L2L_API_KEY,
        "site": SITE_ID
    }
    resp = requests.get(url, params=params, verify=False)
    resp.raise_for_status()
    json_data = resp.json()
    if not json_data.get("success", False):
        raise RuntimeError(f"Falha ao buscar dispatches: {json_data.get('error')}")

    records = []
    for dispatch in json_data.get("data", []):
        disp_id   = dispatch.get("id")
        disp_num  = dispatch.get("dispatchnumber")
        disp_last = dispatch.get("lastupdated")
        for resource in dispatch.get("resources", []):
            records.append({
                "dispatch_id":    disp_id,
                "dispatchnumber": disp_num,
                "lastupdated":    disp_last,
                "res_id":         resource.get("id"),
                "res_loginid":    resource.get("loginid"),
                "res_fullname":   resource.get("fullname"),
                "res_assigned":   resource.get("assigned"),
            })
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df["res_loginid_UP"] = df["res_loginid"].astype(str).str.strip().str.upper()
    return df[[
        "dispatch_id",
        "dispatchnumber",
        "lastupdated",
        "res_id",
        "res_loginid",
        "res_fullname",
        "res_assigned",
        "res_loginid_UP",
    ]].copy()

# API real: busca o dispatchTechnicianId
def get_dispatchtechnician_id(dispatch_id: int, user_id: int) -> int | None:
    url = f"{L2L_API_BASE}/dispatchtechnicians/"
    params = {
        "auth": L2L_API_KEY,
        "site": SITE_ID,
        "dispatchid": dispatch_id
    }
    resp = requests.get(url, params=params, verify=False)
    resp.raise_for_status()
    json_data = resp.json()
    if not json_data.get("success", False):
        raise RuntimeError(f"Falha ao buscar dispatchTechnicians: {json_data.get('error')}")

    for item in json_data.get("data", []):
        if item.get("user") == user_id:
            return item.get("id")
    return None

# aqui o POST que tira o recurso da dispatch
def remove_resource_from_dispatch(dispatch_technician_id: int) -> bool:
    url = f"{L2L_API_BASE}/dispatchtechnicians/complete/{dispatch_technician_id}/"
    data = {
        "auth": L2L_API_KEY,
        "site": SITE_ID
    }
    resp = requests.post(url, data=data, verify=False)
    content_type = resp.headers.get("Content-Type", "")

    if "application/json" not in content_type:
        print(f"[Erro] Resposta inesperada (não JSON) ao tentar remover {dispatch_technician_id}.")
        print("Conteúdo retornado (início):", resp.text[:200].replace("\n", " "))
        return False

    json_data = resp.json()
    if json_data.get("success", False):
        print(f"[OK] DispatchTechnician {dispatch_technician_id} removido com sucesso.")
        return True
    else:
        print(f"[Falha] Não foi possível remover {dispatch_technician_id}: {json_data.get('error')}")
        return False

def main():
    df_saidas = get_saidas_catraca_df()
    if df_saidas.empty:
        print("Nenhuma saída no dia de hoje encontrada na LOG_ACESSO.")
        return
    print(f"Saídas na catraca (reais): {len(df_saidas)} registro(s).")

    # listar últimas 10 matrículas normalizadas do banco
    print("\n--- Últimas 10 matrículas do banco (normalizadas) ---")
    print(df_saidas[["NU_MATRICULA", "matricula_UP"]].tail(10).to_string(index=False))

    try:
        df_dispatch_recursos = get_open_dispatches_df_real()
    except Exception as e:
        print(f"[Erro] Ao buscar dispatches abertos: {e}")
        return
    if df_dispatch_recursos.empty:
        print("Nenhum dispatch aberto retornado pela API L2L.")
        return
    print(f"Recursos em dispatches abertos: {len(df_dispatch_recursos)} linha(s).")

    # listar últimas 10 logins normalizados da API
    print("\n--- Últimos 10 logins de dispatch da API (normalizados) ---")
    print(df_dispatch_recursos[["res_loginid", "res_loginid_UP"]].tail(10).to_string(index=False))

    df_merge = pd.merge(
        df_saidas[["matricula_UP"]],
        df_dispatch_recursos[["dispatch_id", "res_loginid_UP"]],
        how="inner",
        left_on="matricula_UP",
        right_on="res_loginid_UP",
    )
    if df_merge.empty:
        print("Nenhuma correspondência entre saída de catraca e recursos em dispatch.")
        return

    combos = df_merge[["dispatch_id", "res_loginid_UP"]].drop_duplicates().values.tolist()
    print(f"Combinações a processar (dispatch_id, loginid_UP): {combos}")

    for dispatch_id, loginid_UP in combos:
        row = df_dispatch_recursos[
            (df_dispatch_recursos["dispatch_id"] == dispatch_id) &
            (df_dispatch_recursos["res_loginid_UP"] == loginid_UP)
        ]
        if row.empty:
            print(f"[Aviso] Não achei res_id para user {loginid_UP} no dispatch {dispatch_id}.")
            continue
        user_id = int(row.iloc[0]["res_id"])
       
        try:
            dt_id = get_dispatchtechnician_id(dispatch_id, user_id)
        except Exception as e:
            print(f"[Erro] Ao buscar dispatchTechnician para dispatch {dispatch_id} e user {user_id}: {e}")
            continue

        if dt_id is None:
            print(f"[Aviso] DispatchTechnician não encontrado para dispatch {dispatch_id}, user {user_id}.")
            continue

        remove_resource_from_dispatch(dt_id)

    print("Rotina de remoção de recursos finalizada.")

if __name__ == "__main__":
    main()
