#Load all the libraries 

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from snowflake.snowpark.session import Session
import keyring
from py_topping.data_connection.sharepoint import da_tran_SP365
import pandas as pd
import datetime
import os
import numpy as np
# Gets the version

#Connection to snowflake
ctx = snowflake.connector.connect(
    user='soniya.gosavi@westjet.com',
    account = "westjetdev.west-us-2.azure",
    host='westjetdev.west-us-2.azure.snowflakecomputing.com',
    authenticator='externalbrowser',
    role= "GRP_ADS_SALES_DATA_NONPRD",
    database= "DM_SALESDATA_QA",
    schema= "DBO",
    warehouse= "SALESDATA_WH"
    )

connection_parameters = {
    "account": "westjetdev.west-us-2.azure",
    "user": 'soniya.gosavi@westjet.com',
    "role": "GRP_ADS_SALES_DATA_NONPRD",
    "database": "DM_SALESDATA_QA",
    "schema": "DBO",
    "warehouse": "SALESDATA_WH",
    "host":'westjetdev.west-us-2.azure.snowflakecomputing.com',
    "authenticator":'externalbrowser'
}


session = Session.builder.configs(connection_parameters).create()

#Transformations starts here.
#Change the datatype of the columns
df = session.sql('select * from "DM_SALESDATA_QA"."DBO"."SALES_BCV_BTCA"')
sqldf = df.to_pandas()

sqldf[["FINAL_ADM_AMT_USD","FINAL_ADM_AMT_LCY","ARLN_FARE","ARLN_TAX","ARLN_COMM","ARLN_TAX_COMM",
  "ARLN_CP","ARLN_MF","ARLN_TOTAL","AGNT_FARE","AGNT_TAX","AGNT_COMM",
  "AGNT_TAX_COMM","AGNT_CP","AGNT_MF","DIFF_FARE",
  "AGNT_TOTAL","DIFF_TAX","DIFF_COMM","DIFF_TAX_COMM","DIFF_CP",
  "DIFF_MF","ADM_AMT","TKT_ADM_AMT_LCY","ADM_FEE_LCY","TKT_ADM_AMT_USD","ADM_FEE_USD"]] = sqldf[["FINAL_ADM_AMT_USD","FINAL_ADM_AMT_LCY","ARLN_FARE","ARLN_TAX","ARLN_COMM","ARLN_TAX_COMM",
  "ARLN_CP","ARLN_MF","ARLN_TOTAL","AGNT_FARE","AGNT_TAX","AGNT_COMM",
  "AGNT_TAX_COMM","AGNT_CP","AGNT_MF","DIFF_FARE",
  "AGNT_TOTAL","DIFF_TAX","DIFF_COMM","DIFF_TAX_COMM","DIFF_CP",
  "DIFF_MF","ADM_AMT","TKT_ADM_AMT_LCY","ADM_FEE_LCY","TKT_ADM_AMT_USD","ADM_FEE_USD"]].astype(float).round(2)
sqldf[["AUDIT_TYPE","ADM_BATCH","AGNT_IATA_CD", "POS_CNTRY", "ADM_SUBREASON","STATUS_CURRENT","STATUS_CURRENT2","CURRENCY","REF_DOC_NBR_PRIME","TOUR_CD","TKT_GDS","FCMI","FCPI","AUTOPRICED","DIFF_TAX_COMM_TYPE","TAX_DETAIL","ADM_STATUS_CD"]] = sqldf[["AUDIT_TYPE","ADM_BATCH","AGNT_IATA_CD", "POS_CNTRY", "ADM_SUBREASON","STATUS_CURRENT","STATUS_CURRENT2","CURRENCY","REF_DOC_NBR_PRIME","TOUR_CD","TKT_GDS","FCMI","FCPI","AUTOPRICED","DIFF_TAX_COMM_TYPE","TAX_DETAIL","ADM_STATUS_CD"]].astype(pd.StringDtype())
sqldf[["ADM_MONTH","ADM_CNT","ADM_ID","ADM_NBR","DOC_NBR_PRIME","CARR_CD"]] = sqldf[["ADM_MONTH","ADM_CNT","ADM_ID","ADM_NBR","DOC_NBR_PRIME","CARR_CD"]].apply(pd.to_numeric, errors="coerce")
cols = ["TKT_ISSUE_DATE","ADM_STATUS_UPD_TS","STATUS_DATE"]
sqldf[cols] = sqldf[cols].apply(pd.to_datetime)


keyring.set_password("sales_tech_sharepoint", "client_id", "f0548aa7-e200-4972-b055-41b4567a55a0")
keyring.set_password("sales_tech_sharepoint", "client_secret", "2oK9fHR4RqWnrJrA6wWmHfzWXj0EKSgYDvZ9l4hbVY4=")
bcv_tech_url = "https://westjet.sharepoint.com/sites/Waivers-Project/"
sp = da_tran_SP365(site_url = bcv_tech_url,
client_id = keyring.get_password("sales_tech_sharepoint", "client_id"),
client_secret = keyring.get_password("sales_tech_sharepoint", "client_secret"))

log_file_dir = "https://westjet.sharepoint.com/:x:/r/sites/Waivers-Project/Shared%20Documents/General/ADM-ACM/"

download_path = sp.create_link(log_file_dir+"BCV_BTCA"+".xlsx")
sp.download(sharepoint_location =download_path, local_location = "C:\\SONIYA\\Excel_Files\\NGSD\\BCV_BTCA_LATEST.xlsx")

os.chdir("C:\\SONIYA\\Excel_Files\\NGSD")


df=pd.read_excel("BCV_BTCA_LATEST.xlsx",sheet_name="Data",na_filter=False)
df1=df
df1.columns = map(str.upper, df1.columns)
df1[["FINAL_ADM_AMT_USD","FINAL_ADM_AMT_LCY","ARLN_FARE","ARLN_TAX","ARLN_COMM","ARLN_TAX_COMM",
  "ARLN_CP","ARLN_MF","ARLN_TOTAL","AGNT_FARE","AGNT_TAX","AGNT_COMM",
  "AGNT_TAX_COMM","AGNT_CP","AGNT_MF","DIFF_FARE",
  "AGNT_TOTAL","DIFF_TAX","DIFF_COMM","DIFF_TAX_COMM","DIFF_CP",
  "DIFF_MF","ADM_AMT","TKT_ADM_AMT_LCY","ADM_FEE_LCY","TKT_ADM_AMT_USD","ADM_FEE_USD"]] = df1[["FINAL_ADM_AMT_USD","FINAL_ADM_AMT_LCY","ARLN_FARE","ARLN_TAX","ARLN_COMM","ARLN_TAX_COMM",
  "ARLN_CP","ARLN_MF","ARLN_TOTAL","AGNT_FARE","AGNT_TAX","AGNT_COMM",
  "AGNT_TAX_COMM","AGNT_CP","AGNT_MF","DIFF_FARE",
  "AGNT_TOTAL","DIFF_TAX","DIFF_COMM","DIFF_TAX_COMM","DIFF_CP",
  "DIFF_MF","ADM_AMT","TKT_ADM_AMT_LCY","ADM_FEE_LCY","TKT_ADM_AMT_USD","ADM_FEE_USD"]].astype(float).round(2)
df1[["AUDIT_TYPE","ADM_BATCH","AGNT_IATA_CD", "POS_CNTRY", "ADM_SUBREASON","STATUS_CURRENT","STATUS_CURRENT2","CURRENCY","REF_DOC_NBR_PRIME","TOUR_CD","TKT_GDS","FCMI","FCPI","AUTOPRICED","DIFF_TAX_COMM_TYPE","TAX_DETAIL","ADM_STATUS_CD"]] = df1[["AUDIT_TYPE","ADM_BATCH","AGNT_IATA_CD", "POS_CNTRY", "ADM_SUBREASON","STATUS_CURRENT","STATUS_CURRENT2","CURRENCY","REF_DOC_NBR_PRIME","TOUR_CD","TKT_GDS","FCMI","FCPI","AUTOPRICED","DIFF_TAX_COMM_TYPE","TAX_DETAIL","ADM_STATUS_CD"]].astype(pd.StringDtype())
df1[["ADM_MONTH","ADM_CNT","ADM_ID","ADM_NBR","DOC_NBR_PRIME","CARR_CD"]] = df1[["ADM_MONTH","ADM_CNT","ADM_ID","ADM_NBR","DOC_NBR_PRIME","CARR_CD"]].apply(pd.to_numeric, errors="coerce")
cols = ["TKT_ISSUE_DATE","ADM_STATUS_UPD_TS","STATUS_DATE"]
df1[cols] = df1[cols].apply(pd.to_datetime)


df1 = df1.replace(np.nan, None, regex=True)
df1 = df1.drop(["COLUMN1"], axis=1, errors="ignore")

df2 = pd.merge(df1,sqldf, how="left", indicator=True)
df3 = df2[df2["_merge"].eq("left_only")].drop(["_merge"], axis=1)
write_pandas(ctx, df3, schema='DBO',database= 'DM_SALESDATA_STG',table_name="SALES_BCV_BTCA",index=False,chunk_size=500,overwrite=False)
#df3.to_sql("Sales_BCV_BTCA",engine,database= "DM_SALESDATA_STG", schema="Sandbox", if_exists="append", index=False,chunksize=500)
print("BCV_BTCA new upload success")