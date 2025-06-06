import requests
import pandas as pd

L2L_API_URL  = 'https://astemo-am.leading2lean.com/api/1.0/dispatches/current_dispatched_resources/'
L2L_API_KEY  = 'lBfcswtYB4pQUEKLnBv39jxtA2doZRxV'
SITE_ID      = 2950

resp = requests.get(
    L2L_API_URL,
    params={'auth': L2L_API_KEY, 'site': SITE_ID},
    verify=False
)
resp.raise_for_status()
data = resp.json().get('data', [])

df_rec = pd.json_normalize(
    data,
    record_path=['resources'],
    meta=['dispatchnumber', 'lastupdated'],  
    record_prefix='res_',
    meta_prefix='meta_'
)

print(df_rec[['res_id', 'res_loginid', 'res_fullname', 'meta_dispatchnumber']].head(10))
