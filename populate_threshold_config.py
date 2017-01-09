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
    connection = pypyodbc.connect('Driver={SQL Server};Server=' + server + ';Database=' +
                                  database + ';uid=' + username + ';pwd=' + password)

    select = "SELECT tipologia_controllo, impianto, apparecchiatura, strumento from VW_RAM_APP_VIBRA"
    dataset = pd.read_sql_query(select, connection)

    impianti = dataset.impianto.unique()
    for impianto in impianti:

        i_dataset = dataset[dataset['impianto']==impianto]
        apparecchiature = i_dataset.apparecchiatura.unique()

        for apparecchiatura in apparecchiature:

            a_dataset = i_dataset[i_dataset['apparecchiatura'] == apparecchiatura]
            strumenti = a_dataset.strumento.unique()

            for strumento in strumenti:

                s_dataset = a_dataset[a_dataset['strumento'] == strumento]

                insert = "INSERT INTO RAM_ANACONDA_APP_VIBRA_CONF_THRESHOLD (tipologia_controllo, impianto, apparecchiatura, strumento, "\
        "risoluzione_temporale, from_date, to_date, delta) "\
        "VALUES (?,?,?,?,?,?,?,?)"

                tc = s_dataset.iloc[0].tipologia_controllo
                risoluzione_temporale = "HOUR"
                from_date = '2015-01-01'
                to_date = '2016-12-31'
                delta = 0.2

                values = [tc, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, to_date, delta]
                cursor = connection.cursor()
                cursor.execute(insert, values)

    connection.commit()

if __name__ == '__main__':
    main()