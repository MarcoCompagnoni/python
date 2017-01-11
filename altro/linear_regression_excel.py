import pandas as pd
import numpy as np
from scipy import stats
import locale
locale.setlocale(locale.LC_ALL, '')
frames=[]


def main():

    monitoraggio_vibrazioni = pd.read_excel('monitoraggio_vibrazioni_2.xlsx')
    headers = list(monitoraggio_vibrazioni.columns.values)

    for i in range(1, len(headers)):
        machine = headers[i]

        monitoraggio_vibrazioni.Time = pd.to_datetime(monitoraggio_vibrazioni.Time, format="%d-%b-%y %H:%M")
        monitoraggio_vibrazioni['days_since'] = (monitoraggio_vibrazioni.Time - pd.to_datetime('2006-07-31 00:00') ).astype('timedelta64[D]')

        monitoraggio_vibrazioni[machine] = pd.to_numeric(monitoraggio_vibrazioni[machine].str.replace(',','.'))
        monitoraggio_vibrazioni['soglia'] = np.mean(monitoraggio_vibrazioni[machine])
        slope, intercept, r_value, p_value, std_err = stats.linregress(monitoraggio_vibrazioni['days_since'],
                                                                       monitoraggio_vibrazioni[machine])

        monitoraggio_vibrazioni['trend'] = slope * monitoraggio_vibrazioni['days_since'] + intercept

        data_frame = pd.DataFrame({'time': monitoraggio_vibrazioni['Time'],
                                   'machine':machine,
                                   'value': monitoraggio_vibrazioni[machine],
                                   'trend':  monitoraggio_vibrazioni['trend'],
                                   'soglia':  monitoraggio_vibrazioni['soglia'],
                                   'coef':  slope,
                                   'p_value': p_value
                                   })

        frames.append(data_frame)

    result = pd.concat(frames)
    result.index = np.arange(0,len(result))
    writer = pd.ExcelWriter('C:/users/marco.compagnoni/Desktop/Monitoraggio Apparecchiature/output.xlsx')
    result.to_excel(writer,'Sheet1')
    writer.save()

if __name__ == '__main__':
    main()

