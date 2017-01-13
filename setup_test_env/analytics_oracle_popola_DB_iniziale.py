import db_interface
import pandas as pd
import numpy as np
import copy
import time
from datetime import datetime
from datetime import timedelta
from scipy import stats


def main():

    time_inizio = time.time()

    conf_analytics = db_interface.get_conf_analytics()
    analytics(conf_analytics)

    print("Finito!")
    time_fine = time.time()
    time_elapsed = time_fine - time_inizio
    seconds = int(time_elapsed) % 60
    minutes = int((time_elapsed / 60) % 60)
    hours = int((time_elapsed / (60 * 60)) % 24)
    print("tempo totale: " + str(hours) + "h " + str(minutes) + "m " + str(seconds) + "s")

    db_interface.close()


def analytics(conf_analytics):

    number_of_rows = len(conf_analytics.index)
    for i in range(0, number_of_rows):

        time_inizio_operazione = time.time()

        row = conf_analytics.iloc[i]
        tipo_calcolo = row.TIPO_CALCOLO
        progressivo = row.PROGRESSIVO
        calcolo = row.CALCOLO
        time_win = row.TIME_WIN

        today = datetime.now()

        # TODO <parametro> popolare con quanti giorni
        quanti_giorni_andiamo_indietro = int(time_win)

        time_start_query = (today - timedelta(days=quanti_giorni_andiamo_indietro))
        time_start_analytics = time_start_query

        print("eseguo il calcolo " + tipo_calcolo + " (" + calcolo + ") dal giorno " + time_start_query.strftime(
            "%Y-%m-%d"))

        if tipo_calcolo == "mavg" and (calcolo == "sma" or calcolo == "wma"):
            time_start_query = (today - timedelta(days=2 * quanti_giorni_andiamo_indietro))

        risoluzione_temporale = "HOUR"
        values = [time_start_query.strftime("%Y-%m-%d"), risoluzione_temporale]

        print("querydb")
        datasource = db_interface.get_vw_app_vibra_data(values)
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
        hours = int((time_elapsed_operazione / (60 * 60)) % 24)
        print("time: " + str(hours) + "h " + str(minutes) + "m " + str(seconds) + "s\n")


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

                    window = int(len(dataframe_strumento.index)/2)
                    dataframe_strumento['moving_average'] = dataframe_strumento.VALUE.rolling(window=window,
                                                                                              center=False).mean()
                    dataframe_strumento = dataframe_strumento[dataframe_strumento['moving_average'].notnull()]
                    dataframe_strumento = dataframe_strumento[dataframe_strumento.TIMESTAMP > start_time.date()]

                elif tipo_media_mobile == "wma":

                    dataframe_strumento = weighted_moving_average(dataframe_strumento, time_win, start_time.date())

                elif tipo_media_mobile == "ema":

                    # TODO <parametro> decadimento della media mobile
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


def weighted_moving_average(dataframe_strumento, time_win, start_time):
    first_time = dataframe_strumento.iloc[0].TIMESTAMP

    # TODO <parametro> come calcolare pesi

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


def regressione_lineare(dataframe, start_time, progressivo):

    impianti = dataframe.IMPIANTO.unique()
    dataframe['hours_since'] = (dataframe.TIMESTAMP - start_time.date()).astype('timedelta64[h]')

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
                slope, intercept, r_value, p_value, std_err = stats.linregress(dataframe_strumento['hours_since'],
                                                                               dataframe_strumento.VALUE)

                dataframe_strumento['trend'] = slope * dataframe_strumento['hours_since'] + intercept
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
               trend_value, trend_coef, trend_pvalue, trend_value, tipologia_controllo, impianto, apparecchiatura,
               strumento, timestamp]
        rows.append(row)

    print("execute upsert!")
    db_interface.insert_linear_regression(rows, trend_coefficient_label, trend_pvalue_label, trend_value_label)
    print("fatto")


def insert_moving_average_into_db(dataframe, progressivo):

    moving_average_label = "mavg_" + str(progressivo)

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

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, timestamp, value, moving_average,
               moving_average, tipologia_controllo, impianto, apparecchiatura, strumento, timestamp]
        rows.append(row)

    print("execute upsert!")
    db_interface.insert_moving_average(rows, moving_average_label)
    print("fatto")


if __name__ == '__main__':
    main()
