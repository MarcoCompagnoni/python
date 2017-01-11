import pypyodbc
import pandas as pd
import numpy as np
import configparser
import copy
import time
import cx_Oracle

from datetime import datetime
from datetime import timedelta
from scipy import stats

# TODO codice per aggiungere solo i valori di oggi

connection = None

def get_connection():
    global connection

    if not connection:
        config = configparser.ConfigParser()
        config.read('settings_Oracle.ini')
        server = config.get('login', 'server')
        service = config.get('login', 'service')
        username = config.get('login', 'username')
        password = config.get('login', 'password')

        connection = cx_Oracle.connect(username + "/" + password + "@" + server + "/" + service)
    return connection

def analytics(conf_analytics):
    # connessione a SQLServer per performance
    connection_sql_server = pypyodbc.connect(
        'Driver={SQL Server};Server=localhost;Database=marco_db;uid=marco;pwd=CiaoCiao91')
    select_app_vibra_sql_server = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, " \
                                  "timestamp, value, value_min, allerta, allerta_blocco FROM VW_RAM_APP_VIBRA " \
                                  "WHERE timestamp > ? AND risoluzione_temporale = ? AND value >= value_min ORDER BY timestamp"

    number_of_rows = len(conf_analytics.index)
    for i in range(0, number_of_rows):

        time_inizio_operazione = time.time()

        row = conf_analytics.iloc[i]
        tipo_calcolo = row.TIPO_CALCOLO
        progressivo = row.PROGRESSIVO
        calcolo = row.CALCOLO
        time_win = row.TIME_WIN

        today = datetime.now()

        # GET DATA ####
        select_app_vibra = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, " \
                           "timestamp, value, value_min, allerta, allerta_blocco FROM VW_RAM_APP_VIBRA " \
                           "WHERE timestamp > to_date(:1,'yyyy-mm-dd') AND risoluzione_temporale = :2 AND value >= value_min ORDER BY timestamp"

        time_start_query = (today - timedelta(days=int(time_win)))
        time_start_analytics = time_start_query

        print("eseguo il calcolo " + tipo_calcolo + " (" + calcolo + ") dal giorno " + time_start_query.strftime(
            "%Y-%m-%d"))

        if tipo_calcolo == "mavg" and (calcolo == "sma" or calcolo == "wma"):
            time_start_query = (today - timedelta(days=int(2 * time_win)))
            if calcolo == "sma":
                time_start_analytics = time_start_query

        risoluzione_temporale = "HOUR"
        values = [time_start_query, risoluzione_temporale]

        print("querydb")
        # datasource = pd.read_sql_query(select_app_vibra, connection, params=values)

        # TODO sto leggendo i dati da SQLServer perchè Oracle è TROPPO lento
        datasource = pd.read_sql_query(select_app_vibra_sql_server, connection_sql_server, params=values)
        old = datasource.columns.values
        new = []
        for column in old:
            new.append(column.upper())
        datasource.columns = new

        print("got data")

        if tipo_calcolo == "trend":
            if calcolo == "lregr":
                regressione_lineare(datasource, time_start_analytics, progressivo)
        elif tipo_calcolo == "mavg":
            media_mobile(datasource, time_win, calcolo, time_start_analytics, progressivo)

        time_fine_operazione = time.time()
        time_elapsed_operazione = time_fine_operazione - time_inizio_operazione
        seconds = int(time_elapsed_operazione) % 60
        minutes = int((time_elapsed_operazione / 60) % 60)
        hours = int((time_elapsed_operazione / (60 * 24)) % 24)
        print("time: " + str(hours) + "h " + str(minutes) + "m " + str(seconds) + "s\n")

def main():

    time_inizio = time.time()

    connection = get_connection()

    conf_analytics = pd.read_sql_query("SELECT tipo_calcolo, progressivo, calcolo, time_win "\
        "FROM RAM_APP_VIBRA_CONF_ANALYTICS", connection)

    analytics(conf_analytics)

    connection.close()
    print("Finito!")

    time_fine = time.time()
    time_elapsed = time_fine - time_inizio
    seconds = int(time_elapsed) % 60
    minutes = int((time_elapsed / 60) % 60)
    hours = int((time_elapsed / (60 * 24)) % 24)

    print("tempo totale: " + str(hours) + "h " + str(minutes) + "m " + str(seconds) + "s")

def weighted_moving_average(dataframe_strumento, time_win, start_time):
    first_time = dataframe_strumento.iloc[0].TIMESTAMP

    # TODO calcola pesi in base al delta del tempo -da scegliere-

    dataframe_strumento['h_since'] = (
        dataframe_strumento.TIMESTAMP - first_time).astype('timedelta64[h]')

    number_of_rows = len(dataframe_strumento.index)
    wma = pd.DataFrame()
    dataframe_strumento = dataframe_strumento.reset_index(drop=True)

    index_row_time_start = dataframe_strumento[dataframe_strumento.TIMESTAMP > start_time].index.values
    if len(index_row_time_start) == 0:
        return
    else:
        index_row_time_start = index_row_time_start[0]

    for i in range(index_row_time_start, number_of_rows):
        row = dataframe_strumento.iloc[i]

        timestamp_row = row.TIMESTAMP
        timestamp_start = timestamp_row - timedelta(days=int(time_win))
        dataframe_wma = copy.deepcopy(dataframe_strumento)

        dataframe_wma = dataframe_wma[dataframe_wma.TIMESTAMP >= timestamp_start]
        dataframe_wma = dataframe_wma[dataframe_wma.TIMESTAMP <= timestamp_row]

        values = dataframe_wma.VALUE.tolist()
        weights = dataframe_wma['h_since'].tolist()

        moving_average = np.average(values, weights=weights)

        copy_row = copy.deepcopy(row)
        copy_row['moving_average'] = moving_average
        wma = wma.append(copy_row)
    return wma

def media_mobile(dataframe, time_win, tipo_media_mobile, start_time, progressivo):

    impianti = dataframe.IMPIANTO.unique()
    frames = []
    for impianto in impianti:

        dataframe_impianto = copy.deepcopy(dataframe[dataframe.IMPIANTO == impianto])
        apparecchiature = dataframe_impianto.APPARECCHIATURA.unique()

        for apparecchiatura in apparecchiature:

            dataframe_apparecchiatura = copy.deepcopy(dataframe_impianto[dataframe_impianto.APPARECCHIATURA
                                                                         == apparecchiatura])
            strumenti = dataframe_apparecchiatura.STRUMENTO.unique()

            for strumento in strumenti:

                dataframe_strumento = copy.deepcopy(dataframe_apparecchiatura[dataframe_apparecchiatura.STRUMENTO
                                                                              == strumento])
                if tipo_media_mobile == "sma":

                    # TODO sto assumento che abbiamo TUTTI i valori orari nel periodo considerato
                    # window 2* time_win perchè time_win è in day mentre la granuralità del dataset è in 12h
                    dataframe_strumento['moving_average'] = dataframe_strumento.VALUE.rolling(window=24*time_win,
                                                                                                 center=False).mean()
                    dataframe_strumento = dataframe_strumento[dataframe_strumento['moving_average'].notnull()]

                elif tipo_media_mobile == "wma":

                    wma = weighted_moving_average(dataframe_strumento, time_win, start_time)
                    frames.append(wma)

                elif tipo_media_mobile == "ema":

                    # TODO sto assumento che abbiamo TUTTI i valori orari nel periodo considerato

                    dataframe_strumento['moving_average'] = dataframe_strumento.VALUE.ewm(ignore_na=False,
                                                                                             adjust=True, min_periods=0,
                                                                                             span=time_win).mean()

                frames.append(dataframe_strumento)

    if len(frames) == 0:
        print("not enough values, change time_win in the config table")
        return

    result = pd.concat(frames)
    result = result[result['moving_average'].notnull()]
    print("insert mavg into db")
    insert_moving_average_into_db(result, progressivo)

def regressione_lineare(dataframe, start_time, progressivo):

    impianti = dataframe.IMPIANTO.unique()
    dataframe['days_since'] = (dataframe.TIMESTAMP - start_time).astype('timedelta64[D]')

    frames = []

    for impianto in impianti:

        dataframe_impianto = copy.deepcopy(dataframe[dataframe.IMPIANTO == impianto])
        apparecchiature = dataframe_impianto.APPARECCHIATURA.unique()

        for apparecchiatura in apparecchiature:

            dataframe_apparecchiatura = copy.deepcopy(dataframe_impianto[dataframe_impianto.APPARECCHIATURA
                                                                         == apparecchiatura])
            strumenti = dataframe_apparecchiatura.STRUMENTO.unique()

            for strumento in strumenti:

                dataframe_strumento = copy.deepcopy(dataframe_apparecchiatura[dataframe_apparecchiatura.STRUMENTO
                                                                              == strumento])
                slope, intercept, r_value, p_value, std_err = stats.linregress(dataframe_strumento['days_since'],
                                                                               dataframe_strumento.VALUE)

                dataframe_strumento['trend'] = slope * dataframe_strumento['days_since'] + intercept
                dataframe_strumento['coef'] = slope
                dataframe_strumento['p_value'] = p_value

                frames.append(dataframe_strumento)

    if len(frames) == 0:
        print("not enough values, change time_win in the config table")
        return

    result = pd.concat(frames)

    print("insert lregr into db")
    insert_linear_regression_into_db(result, progressivo)

def insert_linear_regression_into_db(dataframe, progressivo):

    trend_coefficient_label = "trend_coef_" + str(progressivo)
    trend_pvalue_label = "trend_pvalue_" + str(progressivo)
    trend_value_label = "trend_value_" + str(progressivo)

    cursor = connection.cursor()

    cursor.prepare("BEGIN INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, " \
        "timestamp, value, {}, {}, {}) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9);" \
                  "EXCEPTION WHEN DUP_VAL_ON_INDEX THEN " \
                  "UPDATE RAM_ANACONDA_APP_VIBRA SET {}=:10,{}=:11,{}=:12 " \
        "WHERE tipologia_controllo=:13 AND impianto=:14 AND apparecchiatura=:15 AND strumento=:16 AND timestamp=:17; END;".format(trend_coefficient_label,
                                                                          trend_pvalue_label, trend_value_label,
                                                                          trend_coefficient_label, trend_pvalue_label,
                                                                          trend_value_label))

    number_of_rows = len(dataframe.index)
    rows = []
    for i in range(0, number_of_rows):

        input_row = dataframe.iloc[i]

        tipologia_controllo = input_row.TIPOLOGIA_CONTROLLO
        impianto = input_row.IMPIANTO
        apparecchiatura = input_row.APPARECCHIATURA
        strumento = input_row.STRUMENTO
        timestamp = input_row.TIMESTAMP
        trend_coef = input_row['coef']
        trend_pvalue = input_row['p_value']
        trend_value = input_row['trend']
        value = input_row.VALUE

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, value, trend_coef, trend_pvalue,
                  trend_value, trend_coef, trend_pvalue,
                  trend_value, tipologia_controllo, impianto, apparecchiatura, strumento, timestamp]
        rows.append(row)

    print("execute upsert!")
    cursor.executemany(None, rows)
    print("fatto")
    connection.commit()

def insert_moving_average_into_db(dataframe, progressivo):

    moving_average_label = "mavg_" + str(progressivo)

    cursor = connection.cursor()
    cursor.prepare("BEGIN INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, " \
        "timestamp, value, {}) VALUES (:1,:2,:3,:4,:5,:6,:7); " \
                  "EXCEPTION WHEN DUP_VAL_ON_INDEX THEN " \
                  "update RAM_ANACONDA_APP_VIBRA set {}=:8 " \
        "where tipologia_controllo=:9 and impianto=:10 and apparecchiatura=:11 and strumento=:12 and timestamp=:13; END;".format(moving_average_label, moving_average_label))

    number_of_rows = len(dataframe.index)
    rows = []
    for i in range(0, number_of_rows):
        input_row = dataframe.iloc[i]

        tipologia_controllo = input_row.TIPOLOGIA_CONTROLLO
        impianto = input_row.IMPIANTO
        apparecchiatura = input_row.APPARECCHIATURA
        strumento = input_row.STRUMENTO
        timestamp = input_row.TIMESTAMP
        moving_average = input_row['moving_average']
        value = input_row.VALUE

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, value, moving_average, moving_average,
                  tipologia_controllo, impianto, apparecchiatura, strumento, timestamp]
        rows.append(row)
    print("execute upsert!")
    cursor.executemany(None, rows)
    print("fatto")
    connection.commit()

if __name__ == '__main__':
    main()
