import cx_Oracle
import pandas as pd
import configparser
from datetime import datetime

connection = None
a=1

def main():

    global connection

    config = configparser.ConfigParser()
    config.read('settings_Oracle.ini')
    server = config.get('login', 'server')
    service = config.get('login', 'service')
    username = config.get('login', 'username')
    password = config.get('login', 'password')

    # CONNESSIONE ####
    connection = cx_Oracle.connect(username + "/" + password + "@" + server + "/" + service)

# GET CONFIGURAZIONI THRESHOLD ####
    select_conf_threshold = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale," \
                            " from_date, to_date, delta FROM RAM_APP_VIBRA_CONF_THRESHOLD ORDER BY " \
                            "tipologia_controllo, impianto, apparecchiatura, strumento asc"
    conf_threshold = pd.read_sql_query(select_conf_threshold, connection)

    conf_threshold_number_of_rows = len(conf_threshold.index)

    for i in range(0, conf_threshold_number_of_rows):
        timestamp = datetime.now()
        row = conf_threshold.iloc[i]
        tipologia_controllo = row.TIPOLOGIA_CONTROLLO
        if tipologia_controllo is None:
            tipologia_controllo = '%'
        impianto = row.IMPIANTO
        if impianto is None:
            impianto = "%"
        apparecchiatura = row.APPARECCHIATURA
        if apparecchiatura is None:
            apparecchiatura = '%'
        strumento = row.STRUMENTO
        if strumento is None:
            strumento = '%'
        risoluzione_temporale = row.RISOLUZIONE_TEMPORALE
        from_date = row.FROM_DATE
        to_date = row.TO_DATE
        delta = row.DELTA

# CALCOLO SOGLIA ####
        select_app_vibra_soglia = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, avg(value) AS " \
            "media_valori FROM VW_RAM_APP_VIBRA WHERE tipologia_controllo LIKE :1 AND impianto LIKE :2 AND " \
                                  "apparecchiatura LIKE :3 AND strumento LIKE :4 AND risoluzione_temporale=:5 AND value " \
                                  ">= value_min AND timestamp BETWEEN :6 AND :7 GROUP BY tipologia_controllo, impianto," \
                                  " apparecchiatura, strumento"

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, to_date]
        connection = cx_Oracle.connect(username + "/" + password + "@" + server + "/" + service)
        soglia_dataset = pd.read_sql_query(select_app_vibra_soglia, connection, params=values)

        insert_soglia_into_db(soglia_dataset, delta, timestamp)


def insert_soglia_into_db(dataframe, delta, timestamp):
    global a

    cursor = connection.cursor()
    cursor.prepare("BEGIN INSERT INTO RAM_ANACONDA_APP_THRESHOLD (tipologia_controllo, impianto, apparecchiatura, strumento, "\
        "soglia, timestamp) VALUES (:1,:2,:3,:4,:5,:12);"
        "EXCEPTION WHEN DUP_VAL_ON_INDEX "
        "THEN "
        "UPDATE RAM_ANACONDA_APP_THRESHOLD SET soglia = :6, timestamp = :7 WHERE tipologia_controllo=:8 AND impianto=:9 "\
        "AND apparecchiatura=:10 AND strumento=:11; END;")

    number_of_rows = len(dataframe.index)
    rows = []
    for i in range(0, number_of_rows):
        row = dataframe.iloc[i]

        tipologia_controllo = row.TIPOLOGIA_CONTROLLO
        impianto = row.IMPIANTO
        apparecchiatura = row.APPARECCHIATURA
        strumento = row.STRUMENTO
        soglia = row.MEDIA_VALORI
        soglia *= (1+delta)

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, soglia, timestamp, soglia, timestamp,
                  tipologia_controllo, impianto, apparecchiatura, strumento]
        rows.append(row)
    cursor.executemany(None, rows)
    connection.commit()
    connection.close()
if __name__ == '__main__':
    main()
