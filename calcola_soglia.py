import pypyodbc
import pandas as pd
import configparser
from datetime import datetime

connection = None


def main():

    global connection

    config = configparser.ConfigParser()
    config.read('settings.ini')
    server = config.get('login', 'server')
    database = config.get('login', 'database')
    username = config.get('login', 'username')
    password = config.get('login', 'password')

# CONNESSIONE ####
    connection = pypyodbc.connect('Driver={SQL Server};Server='+server+';Database=' +
                                  database+';uid='+username+';pwd='+password)

# GET CONFIGURAZIONI THRESHOLD ####
    select_conf_threshold = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, "\
        "to_date, delta FROM RAM_ANACONDA_APP_VIBRA_CONF_THRESHOLD ORDER BY tipologia_controllo, impianto, "\
        "apparecchiatura, strumento asc"
    conf_threshold = pd.read_sql_query(select_conf_threshold, connection)

    conf_threshold_number_of_rows = len(conf_threshold.index)

    for i in range(0, conf_threshold_number_of_rows):
        timestamp = datetime.now()
        row = conf_threshold.iloc[i]
        tipologia_controllo = row['tipologia_controllo']
        if tipologia_controllo is None:
            tipologia_controllo = '%'
        impianto = row["impianto"]
        if impianto is None:
            impianto = "%"
        apparecchiatura = row["apparecchiatura"]
        if apparecchiatura is None:
            apparecchiatura = '%'
        strumento = row["strumento"]
        if strumento is None:
            strumento = '%'
        risoluzione_temporale = row["risoluzione_temporale"]
        from_date = row["from_date"]
        to_date = row["to_date"]
        delta = row["delta"]

# CALCOLO SOGLIA ####
        select_app_vibra_soglia = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, avg(value) AS " \
            "media_valori FROM VW_RAM_APP_VIBRA WHERE tipologia_controllo LIKE ? AND impianto LIKE ? AND apparecchiatura LIKE ?" \
            " AND strumento LIKE ? AND risoluzione_temporale=? AND timestamp BETWEEN ? AND ? " \
            "GROUP BY tipologia_controllo, impianto, apparecchiatura, strumento" \

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, to_date]
        soglia_dataset = pd.read_sql_query(select_app_vibra_soglia, connection, params=values)

        insert_soglia_into_db(soglia_dataset, delta, timestamp)


def insert_soglia_into_db(dataframe, delta, timestamp):

    cursor = connection.cursor()
    sql_command = "IF EXISTS (SELECT 1 FROM RAM_ANACONDA_APP_THRESHOLD WHERE tipologia_controllo=? AND impianto=? AND "\
        "apparecchiatura=? AND strumento=? AND timestamp=?)"\
        " UPDATE RAM_ANACONDA_APP_THRESHOLD SET soglia = ?, timestamp = ? WHERE tipologia_controllo=? AND impianto=? "\
        "AND apparecchiatura=? AND strumento=?"\
        " ELSE "\
        "INSERT INTO RAM_ANACONDA_APP_THRESHOLD (tipologia_controllo, impianto, apparecchiatura, strumento, "\
        "soglia, timestamp) "\
        "VALUES (?,?,?,?,?,?)"

    number_of_rows = len(dataframe.index)

    for i in range(0, number_of_rows):
        row = dataframe.iloc[i]

        tipologia_controllo = row['tipologia_controllo']
        impianto = row['impianto']
        apparecchiatura = row['apparecchiatura']
        strumento = row['strumento']
        soglia = row['media_valori']
        soglia *= (1+delta)

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, soglia, timestamp,
                  tipologia_controllo, impianto, apparecchiatura, strumento, tipologia_controllo, impianto,
                  apparecchiatura, strumento, soglia, timestamp]
        cursor.execute(sql_command, values)
    connection.commit()
if __name__ == '__main__':
    main()
