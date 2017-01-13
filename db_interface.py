import cx_Oracle
import configparser
import pandas as pd


conn = None


def get_connection():
    global conn

    # create oracle connection if not exists
    if not conn:
        config = configparser.ConfigParser()
        config.read('settings_Oracle.ini')
        server = config.get('login', 'server')
        service = config.get('login', 'service')
        username = config.get('login', 'username')
        password = config.get('login', 'password')
        conn = cx_Oracle.connect(username + "/" + password + "@" + server + "/" + service)
    return conn


def close():
    connection = get_connection()
    connection.close()


def get_vw_app_vibra_data(values):
    connection = get_connection()
    return pd.read_sql_query("SELECT tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, value, "
                             "value_min, allerta, allerta_blocco "
                             "FROM VW_RAM_APP_VIBRA "
                             "WHERE timestamp > to_date(:1,'yyyy-mm-dd') AND risoluzione_temporale = :2 "
                             "AND value >= value_min "
                             "ORDER BY timestamp", connection, params=values)


def get_data_and_calculate_threshold(values):
    connection = get_connection()
    return pd.read_sql_query("SELECT tipologia_controllo, impianto, apparecchiatura, strumento, avg(value) "
                             "AS media_valori "
                             "FROM VW_RAM_APP_VIBRA "
                             "WHERE tipologia_controllo LIKE :1 AND impianto LIKE :2 AND apparecchiatura LIKE :3 "
                             "AND strumento LIKE :4 AND risoluzione_temporale=:5 AND value >= value_min "
                             "AND timestamp BETWEEN :6 AND :7 "
                             "GROUP BY tipologia_controllo, impianto, apparecchiatura, strumento", connection,
                             params=values)


def get_conf_threshold():
    connection = get_connection()
    return pd.read_sql_query("SELECT tipologia_controllo, impianto, apparecchiatura, strumento, "
                             "risoluzione_temporale, from_date, to_date, delta "
                             "FROM RAM_APP_VIBRA_CONF_THRESHOLD "
                             "ORDER BY tipologia_controllo, impianto, apparecchiatura, strumento asc",
                             connection)


def get_conf_analytics():
    connection = get_connection()
    return pd.read_sql_query("SELECT tipo_calcolo, progressivo, calcolo, time_win "
                             "FROM RAM_APP_VIBRA_CONF_ANALYTICS", connection)


def insert_threshold_into_db(dataframe, delta, timestamp):
    connection = get_connection()
    cursor = connection.cursor()

    # upsert into threshold table
    cursor.prepare("BEGIN "
                   "INSERT INTO RAM_ANACONDA_APP_THRESHOLD (tipologia_controllo, impianto, apparecchiatura, "
                   "strumento, soglia, timestamp) "
                   "VALUES (:1,:2,:3,:4,:5,:12);"
                   "EXCEPTION WHEN DUP_VAL_ON_INDEX "
                   "THEN "
                   "UPDATE RAM_ANACONDA_APP_THRESHOLD "
                   "SET soglia = :6, timestamp = :7 WHERE tipologia_controllo=:8 AND impianto=:9 "
                   "AND apparecchiatura=:10 AND strumento=:11; "
                   "END;")

    number_of_rows = len(dataframe.index)
    rows = []
    for i in range(0, number_of_rows):
        row = dataframe.iloc[i]

        tipologia_controllo = row.TIPOLOGIA_CONTROLLO
        impianto = row.IMPIANTO
        apparecchiatura = row.APPARECCHIATURA
        strumento = row.STRUMENTO
        soglia = row.MEDIA_VALORI
        soglia *= (1 + delta)

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, soglia, timestamp, soglia, timestamp,
               tipologia_controllo, impianto, apparecchiatura, strumento]
        rows.append(row)
    cursor.executemany(None, rows)
    connection.commit()


def insert_moving_average(rows, moving_average_label):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.prepare("BEGIN "
                   "INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, "
                   "timestamp, value, {}) "
                   "VALUES (:1,:2,:3,:4,:5,:6,:7);"
                   "EXCEPTION WHEN DUP_VAL_ON_INDEX "
                   "THEN "
                   "UPDATE RAM_ANACONDA_APP_VIBRA "
                   "SET {}=:8 "
                   "WHERE tipologia_controllo=:9 AND impianto=:10 AND apparecchiatura=:11 AND strumento=:12 "
                   "AND timestamp=:13; "
                   "END;".format(moving_average_label, moving_average_label))
    cursor.executemany(None, rows)
    connection.commit()


def insert_linear_regression(rows, trend_coefficient_label, trend_pvalue_label, trend_value_label):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.prepare("BEGIN "
                   "INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, "
                   "timestamp, value, {}, {}, {}) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9);"
                   "EXCEPTION WHEN DUP_VAL_ON_INDEX "
                   "THEN "
                   "UPDATE RAM_ANACONDA_APP_VIBRA "
                   "SET {}=:10,{}=:11,{}=:12 "
                   "WHERE tipologia_controllo=:13 AND impianto=:14 AND apparecchiatura=:15 AND strumento=:16 "
                   "AND timestamp=:17; "
                   "END;".format(trend_coefficient_label, trend_pvalue_label, trend_value_label,
                                 trend_coefficient_label, trend_pvalue_label, trend_value_label))
    cursor.executemany(None, rows)
    connection.commit()
