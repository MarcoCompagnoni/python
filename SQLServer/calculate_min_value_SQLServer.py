import pypyodbc
import pandas as pd
import numpy as np
import configparser
import copy

config = configparser.ConfigParser()
config.read('settings_SQLServer.ini')
server = config.get('login', 'server')
database = config.get('login', 'database')
username = config.get('login', 'username')
password = config.get('login', 'password')

connection = pypyodbc.connect('Driver={SQL Server};Server=' + server + ';Database=' +
                              database + ';uid=' + username + ';pwd=' + password)

select_app_vibra = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, " \
                   "timestamp, value, value_min, allerta, allerta_blocco FROM VW_RAM_APP_VIBRA "

dataframe = pd.read_sql_query(select_app_vibra, connection)

impianti = dataframe.impianto.unique()
cursor = connection.cursor()
update = "UPDATE VW_RAM_APP_VIBRA SET value_min=? " \
            "WHERE impianto = ? and apparecchiatura = ? and strumento=?"

for impianto in impianti:

    dataframe_impianto = copy.deepcopy(dataframe[dataframe['impianto'] == impianto])
    apparecchiature = dataframe_impianto.apparecchiatura.unique()

    for apparecchiatura in apparecchiature:

        dataframe_apparecchiatura = copy.deepcopy(dataframe_impianto[dataframe_impianto['apparecchiatura']
                                                                     == apparecchiatura])
        strumenti = dataframe_apparecchiatura.strumento.unique()

        for strumento in strumenti:

            dataframe_strumento = copy.deepcopy(dataframe_apparecchiatura[dataframe_apparecchiatura['strumento']
                                                                          == strumento])
            max_v = np.max(dataframe_strumento['value'])
            value_min = max_v * 0.1
            values = [value_min, impianto, apparecchiatura, strumento]
            cursor.execute(update, values)
connection.commit()
connection.close()
