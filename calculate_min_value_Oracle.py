import cx_Oracle
import pandas as pd
import numpy as np
import configparser
import copy

config = configparser.ConfigParser()
config.read('settings_Oracle.ini')
server = config.get('login', 'server')
service = config.get('login', 'service')
username = config.get('login', 'username')
password = config.get('login', 'password')

# CONNESSIONE ####
connection = cx_Oracle.connect(username + "/" + password + "@" + server + "/" + service)

select_app_vibra = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, " \
                   "timestamp, value, value_min, allerta, allerta_blocco FROM VW_RAM_APP_VIBRA"

dataframe = pd.read_sql_query(select_app_vibra, connection)

impianti = dataframe.IMPIANTO.unique()
cursor = connection.cursor()
cursor.prepare("UPDATE VW_RAM_APP_VIBRA SET value_min=:1 " \
            "WHERE impianto = :2 and apparecchiatura = :3 and strumento=:4")

for impianto in impianti:

    dataframe_impianto = copy.deepcopy(dataframe[dataframe.IMPIANTO == impianto])
    apparecchiature = dataframe_impianto.APPARECCHIATURA.unique()
    rows = []
    for apparecchiatura in apparecchiature:

        dataframe_apparecchiatura = copy.deepcopy(dataframe_impianto[dataframe_impianto.APPARECCHIATURA
                                                                     == apparecchiatura])
        strumenti = dataframe_apparecchiatura.STRUMENTO.unique()

        for strumento in strumenti:

            dataframe_strumento = copy.deepcopy(dataframe_apparecchiatura[dataframe_apparecchiatura.STRUMENTO
                                                                          == strumento])
            max_v = np.max(dataframe_strumento.VALUE)
            value_min = max_v * 0.1
            row = [value_min, impianto, apparecchiatura, strumento]
            rows.append(row)

cursor.executemany(None, rows)
connection.commit()
connection.close()
