import pandas as pd
import numpy as np
from scipy import stats
import locale
import cx_Oracle
import math
locale.setlocale(locale.LC_ALL, '')

filepath = '../DatiVibrazioni/Dati_Lcf.xlsx'
#filepath = '../DatiVibrazioni/test.xlsx'

def main():
    connection = cx_Oracle.connect("MARCO/CiaoCiao91@192.168.4.124/xe")

    cursor = connection.cursor()
    cursor.prepare("INSERT INTO VW_RAM_APP_VIBRA "
                     "(tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, timestamp, value, "
                  "value_min, allerta, allerta_blocco) "
                     "VALUES (:tipologia_controllo,:impianto,:apparecchiatura,:strumento,:risoluzione_temporale,:timestamp,:value,:value_min,:allerta,:allerta_blocco)")

    print("leggo il file "+filepath)

    monitoraggio_vibrazioni = pd.read_excel(filepath)
    monitoraggio_vibrazioni['TIMESTAMP'] = pd.to_datetime(monitoraggio_vibrazioni['TIMESTAMP'], format="%d-%b-%y %H:%M")

    number_of_rows = len(monitoraggio_vibrazioni.index)
    rows = []
    #scorri righe
    for i in range(0, number_of_rows):
        if i%1000 == 0:
            print(str(i)+"/"+str(number_of_rows))

        row = monitoraggio_vibrazioni.iloc[i]

        tipologia_controllo = row['TIPOLOGIA_CONTROLLO']
        impianto = row['IMPIANTO']
        apparecchiatura = row['APPARECCHIATURA']
        strumento = row['STRUMENTO']
        risoluzione_temporale = row['RISOLUZIONE_TEMPORALE']
        timestamp = row['TIMESTAMP']
        value = row['VALUE']
        if math.isnan(value):
            value = None
        value_min = row['VARIANZA']
        if math.isnan(value_min):
            value_min = None
        allerta = row['ALLERTA']
        if str(allerta) == 'nan':
            allerta = None

        allerta_blocco = row['ALLARME_BLOCCO']
        if math.isnan(allerta_blocco):
            allerta_blocco = None

        row = [tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale,
                  timestamp, value, value_min, allerta, allerta_blocco]
        #print(SQLCommand)
        #print(values)

        rows.append(row)

    cursor.executemany(None, rows)
    connection.commit()
    connection.close()

if __name__ == '__main__':
    main()