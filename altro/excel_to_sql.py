import pandas as pd
import numpy as np
from scipy import stats
import locale
import pypyodbc
locale.setlocale(locale.LC_ALL, '')


def main():

    connection = pypyodbc.connect('Driver={SQL Server};'
                                    'Server=localhost;'
                                    'Database=marco_db;'
                                    'uid=marco;pwd=CiaoCiao91')
    cursor = connection.cursor()
    SQLCommand = ("INSERT INTO VW_RAM_APP_VIBRA "
                     "(tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, timestamp, value, "
                  "value_min, allerta, allerta_blocco) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?)")


    tipologia_controllo = "vib"
    impianto = "imp1"
    apparecchiatura = "app1"
    risoluzione_temporale = "day"
    value_min = 0
    allerta = 0
    allerta_blocco = 0

    monitoraggio_vibrazioni = pd.read_excel('../datasource/monitoraggio_vibrazioni_2.xlsx')
    monitoraggio_vibrazioni.Time = pd.to_datetime(monitoraggio_vibrazioni.Time, format="%d-%b-%y %H:%M")

    headers = list(monitoraggio_vibrazioni.columns.values)
    number_of_rows = len(monitoraggio_vibrazioni.index)
    number_of_columns = len(headers)

    #scorri righe
    for i in range(0, number_of_rows):
        row = monitoraggio_vibrazioni.iloc[i]
        timestamp = row[0]
        for j in range(1, number_of_columns):
            strumento = headers[j]
            value = float(row[j].replace(',','.'))
            Values = [tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale,
              timestamp, value, value_min, allerta, allerta_blocco]
            print(SQLCommand)
            print(Values)
            return
            cursor.execute(SQLCommand,Values)

    connection.commit()
    connection.close()

if __name__ == '__main__':
    main()