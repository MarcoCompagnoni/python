import db_interface
from datetime import datetime


def main():

    conf_threshold = db_interface.get_conf_threshold()
    conf_threshold_number_of_rows = len(conf_threshold.index)

    timestamp = datetime.now()

    for i in range(0, conf_threshold_number_of_rows):

        print("calcolo soglia: "+str(i+1)+"/"+str(conf_threshold_number_of_rows))

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

        values = [tipologia_controllo, impianto, apparecchiatura, strumento, risoluzione_temporale, from_date, to_date]
        soglia_dataset = db_interface.get_data_and_calculate_threshold(values)
        db_interface.insert_threshold_into_db(soglia_dataset, delta, timestamp)

    db_interface.close()


if __name__ == '__main__':
    main()
