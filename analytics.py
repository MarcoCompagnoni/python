import pypyodbc
import pandas as pd
import numpy as np
import configparser
import copy
import time
from datetime import datetime
from datetime import timedelta
from scipy import stats


connection = None


# TODO riorganizzare codice in funzioni
def main():

    time_inizio = time.time()

    global connection
    config = configparser.ConfigParser()
    config.read('settings.ini')
    server = config.get('login', 'server')
    database = config.get('login', 'database')
    username = config.get('login', 'username')
    password = config.get('login', 'password')

# CONNESSIONE ####
    connection = pypyodbc.connect('Driver={SQL Server};Server=' + server + ';Database=' +
                                  database + ';uid=' + username + ';pwd=' + password)

# GET CONFIGURAZIONI ANALYTICS ####
    select_conf_analytics = "SELECT tipo_calcolo, progressivo, calcolo, time_win "\
        "FROM RAM_ANACONDA_APP_VIBRA_CONF_ANALYTICS"

    conf_analytics = pd.read_sql_query(select_conf_analytics, connection)

    number_of_rows = len(conf_analytics.index)

    for i in range(0, number_of_rows):
        time_inizio_operazione = time.time()

        row = conf_analytics.iloc[i]
        tipo_calcolo = row["tipo_calcolo"]
        progressivo = row['progressivo']
        calcolo = row['calcolo']
        time_win = row['time_win']

        today = datetime.now()

# GET DATA ####
        select_app_vibra = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento, " \
            "timestamp, value, value_min, allerta, allerta_blocco FROM VW_RAM_APP_VIBRA " \
            "WHERE timestamp > ? AND risoluzione_temporale = ? and strumento = 'M024VI1051X' ORDER BY timestamp"

# TODO organizzare meglio questo blocco di codice
        time_start = (today - timedelta(days=int(time_win)))

        if tipo_calcolo == "mavg" and (calcolo == "sma" or calcolo == "wma"):
            time_start = (today - timedelta(days=int(2*time_win)))
        time_str = time_start.strftime("%Y-%m-%d")
        if tipo_calcolo == "mavg" and (calcolo == "wma"):
            time_start = (today - timedelta(days=int(time_win)))

        print("eseguo il calcolo " + tipo_calcolo + " (" + calcolo + ") dal giorno " + time_start.strftime("%Y-%m-%d"))

# TODO mettere hour per effettuare il calcolo per ora. Per adesso non ho i dati orari
        risoluzione_temporale = "HOUR"
        values = [time_str, risoluzione_temporale]

        datasource = pd.read_sql_query(select_app_vibra, connection, params=values)

        if tipo_calcolo == "trend":
            if calcolo == "lregr":
                regressione_lineare(datasource, time, progressivo)
        elif tipo_calcolo == "mavg":
            if calcolo == "sma":
                media_mobile(datasource, time_win, calcolo, time, progressivo)

        time_fine_operazione = time.time()
        time_elapsed_operazione = time_fine_operazione - time_inizio_operazione
        seconds = int(time_elapsed_operazione) % 60
        minutes = int((time_elapsed_operazione/60) % 60)
        hours = int((time_elapsed_operazione/(60*24)) % 24)

        print("time: "+str(hours)+"h "+str(minutes)+"m "+str(seconds)+"s\n")

    connection.close()
    print("Finito!")

    time_fine = time.time()
    time_elapsed = time_fine - time_inizio
    seconds = int(time_elapsed) % 60
    minutes = int((time_elapsed / 60) % 60)
    hours = int((time_elapsed / (60 * 24)) % 24)

    print("tempo totale: " + str(hours) + "h " + str(minutes) + "m " + str(seconds) + "s")


def media_mobile(dataframe, time_win, tipo_media_mobile, start_time, progressivo):

    impianti = dataframe.impianto.unique()
    frames = []
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
                if tipo_media_mobile == "sma":

                    # window 2* time_win perchè time_win è in day mentre la granuralità del dataset è in 12h
                    dataframe_strumento['moving_average'] = dataframe_strumento['value'].rolling(window=24*time_win,
                                                                                                 center=False).mean()
                    dataframe_strumento = dataframe_strumento[dataframe_strumento['moving_average'].notnull()]

                elif tipo_media_mobile == "wma":

                    first_time = dataframe_strumento.iloc[0]['timestamp']

                    # TODO calcola pesi in base al delta del tempo -da scegliere-
                    dataframe_strumento['h_since'] = (
                        dataframe_strumento.timestamp - first_time).astype('timedelta64[h]')

                    number_of_rows = len(dataframe_strumento.index)
                    wma = pd.DataFrame()
                    dataframe_strumento = dataframe_strumento.reset_index(drop=True)

                    index_row_time_start = dataframe_strumento[dataframe_strumento['timestamp'] > start_time].index[0]

                    for i in range(index_row_time_start, number_of_rows):

                        row = dataframe_strumento.iloc[i]

                        timestamp_row = row['timestamp']
                        timestamp_start = timestamp_row - timedelta(days=int(time_win))
                        dataframe_wma = copy.deepcopy(dataframe_strumento)

                        dataframe_wma = dataframe_wma[dataframe_wma['timestamp'] >= timestamp_start]
                        dataframe_wma = dataframe_wma[dataframe_wma['timestamp'] <= timestamp_row]

                        values = dataframe_wma['value'].tolist()
                        weights = dataframe_wma['h_since'].tolist()

                        moving_average = np.average(values, weights=weights)

                        copy_row = copy.deepcopy(row)
                        copy_row['moving_average'] = moving_average
                        wma = wma.append(copy_row)

                    frames.append(wma)

                elif tipo_media_mobile == "ema":

                    # window 2* time_win perchè time_win è in day mentre la granuralità del dataset è in 12h
                    dataframe_strumento['moving_average'] = dataframe_strumento['value'].ewm(ignore_na=False,
                                                                                             adjust=True, min_periods=0,
                                                                                             span=time_win).mean()

                frames.append(dataframe_strumento)

    if len(frames) == 0:
        print("not enough values, change time_win in the config table")
        return

    result = pd.concat(frames)
    result = result[result['moving_average'].notnull()]
    insert_moving_average_into_db(result, progressivo)


def regressione_lineare(dataframe, start_time, progressivo):

    impianti = dataframe.impianto.unique()

    dataframe['days_since'] = (dataframe.timestamp - start_time).astype('timedelta64[D]')

    frames = []

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
                slope, intercept, r_value, p_value, std_err = stats.linregress(dataframe_strumento['days_since'],
                                                                               dataframe_strumento['value'])

                dataframe_strumento['trend'] = slope * dataframe_strumento['days_since'] + intercept
                dataframe_strumento['coef'] = slope
                dataframe_strumento['p_value'] = p_value

                frames.append(dataframe_strumento)

    if len(frames) == 0:
        print("not enough values, change time_win in the config table")
        return

    result = pd.concat(frames)
    insert_linear_regression_into_db(result, progressivo)


def insert_linear_regression_into_db(dataframe, progressivo):

    # print("inserisco "+str(len(dataframe.index))+" nel db, partendo dal giorno "+str(dataframe.iloc[0]['timestamp']))

    trend_coefficient_label = "trend_coef_" + str(progressivo)
    trend_pvalue_label = "trend_pvalue_" + str(progressivo)
    trend_value_label = "trend_value_" + str(progressivo)

    cursor = connection.cursor()

    sql_command = "IF EXISTS (SELECT 1 FROM RAM_ANACONDA_APP_VIBRA WHERE tipologia_controllo=? AND impianto=? AND " \
        "apparecchiatura=? AND strumento=? AND timestamp=?) " \
        "UPDATE RAM_ANACONDA_APP_VIBRA SET {}=?,{}=?,{}=? " \
        "WHERE tipologia_controllo=? AND impianto=? AND apparecchiatura=? AND strumento=? AND timestamp=?" \
        " ELSE " \
        "INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, " \
        "timestamp, value, {}, {}, {}) VALUES (?,?,?,?,?,?,?,?,?)".format(trend_coefficient_label,
                                                                          trend_pvalue_label, trend_value_label,
                                                                          trend_coefficient_label, trend_pvalue_label,
                                                                          trend_value_label)

    number_of_rows = len(dataframe.index)

    for i in range(0, number_of_rows):

        row = dataframe.iloc[i]

        tipologia_controllo = row['tipologia_controllo']
        impianto = row['impianto']
        apparecchiatura = row['apparecchiatura']
        strumento = row['strumento']
        timestamp = row['timestamp']
        trend_coef = row['coef']
        trend_pvalue = row['p_value']
        trend_value = row['trend']
        value = row['value']

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, trend_coef, trend_pvalue,
                  trend_value, tipologia_controllo, impianto, apparecchiatura, strumento, timestamp,
                  tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, value, trend_coef,
                  trend_pvalue, trend_value]

        cursor.execute(sql_command, values)
    connection.commit()


def insert_moving_average_into_db(dataframe, progressivo):

    # print("inserisco "+str(len(dataframe.index))+" nel db, partendo dal giorno "+str(dataframe.iloc[0]['timestamp']))

    moving_average_label = "mavg_" + str(progressivo)

    cursor = connection.cursor()
    sql_command = "IF EXISTS (select 1 from RAM_ANACONDA_APP_VIBRA where tipologia_controllo=? and impianto=? and " \
        "apparecchiatura=? and strumento=? and timestamp=?) " \
        "update RAM_ANACONDA_APP_VIBRA set {}=? " \
        "where tipologia_controllo=? and impianto=? and apparecchiatura=? and strumento=? and timestamp=?" \
        " else " \
        "INSERT INTO RAM_ANACONDA_APP_VIBRA (tipologia_controllo, impianto, apparecchiatura, strumento, " \
        "timestamp, value, {}) VALUES (?,?,?,?,?,?,?)".format(moving_average_label, moving_average_label)

    number_of_rows = len(dataframe.index)

    for i in range(0, number_of_rows):
        row = dataframe.iloc[i]

        tipologia_controllo = row['tipologia_controllo']
        impianto = row['impianto']
        apparecchiatura = row['apparecchiatura']
        strumento = row['strumento']
        timestamp = row['timestamp']
        moving_average = row['moving_average']
        value = row['value']

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, moving_average,
                  tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, tipologia_controllo, impianto,
                  apparecchiatura, strumento, timestamp, value, moving_average]

        cursor.execute(sql_command, values)
    connection.commit()

if __name__ == '__main__':
    main()
