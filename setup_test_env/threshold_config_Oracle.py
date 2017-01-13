import cx_Oracle
import pandas as pd
import configparser
from datetime import datetime

connection = None


def main():

    global connection

    config = configparser.ConfigParser()
    config.read('settings_Oracle.ini')
    server = config.get('login', 'server')
    service = config.get('login', 'service')
    username = config.get('login', 'username')
    password = config.get('login', 'password')

    # CONNESSIONE ####
    connection = cx_Oracle.connect(username+"/"+password+"@"+server+"/"+service)

    select = ("SELECT DISTINCT tipologia_controllo, impianto, apparecchiatura, strumento from VW_RAM_APP_VIBRA")
    dataset = pd.read_sql_query(select, connection)

    cursor = connection.cursor()
    cursor.prepare("INSERT INTO RAM_APP_VIBRA_CONF_THRESHOLD (tipologia_controllo, impianto, apparecchiatura, strumento, " \
             "risoluzione_temporale, from_date, to_date, delta) " \
             "VALUES (:1,:2,:3,:4,:5,to_date(:6,'yyyy-mm-dd'),to_date(:7,'yyyy-mm-dd'),:8)")
    rows = []

    impianti = dataset.IMPIANTO.unique()
    for impianto in impianti:

        i_dataset = dataset[dataset.IMPIANTO==impianto]
        apparecchiature = i_dataset.APPARECCHIATURA.unique()

        for apparecchiatura in apparecchiature:

            a_dataset = i_dataset[i_dataset.APPARECCHIATURA == apparecchiatura]
            strumenti = a_dataset.STRUMENTO.unique()

            for strumento in strumenti:

                s_dataset = a_dataset[a_dataset.STRUMENTO == strumento]

                tc = s_dataset.iloc[0].TIPOLOGIA_CONTROLLO
                risoluzione_temporale = "HOUR"
                from_date = ('2016-09-01')
                final_date = ('2016-12-31')
                delta = 0.2

                row = [tc, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, final_date, delta]
                rows.append(row)

    cursor.executemany(None, rows)

    connection.commit()
    connection.close()

if __name__ == '__main__':
    main()