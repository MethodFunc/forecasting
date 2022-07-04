import datetime

import pandas as pd

from modules.databases import define_pipeline, active_database, connect_maria, insert_data
from modules.prediction import predict_step
from modules.kma import Asos
from modules.preprocessing import read_data, asos_preprocessing
from modules.utils import datetime_return
from setting import define_args

if __name__ == '__main__':
    st = datetime.datetime.now()
    args = define_args()

    start, end = datetime_return(args)
    pipeline = define_pipeline(start, end)

    if args.old:
        generate_dict = active_database(pipeline, args)
        dataframe = read_data(generate_dict, args)

    if args.use_asos:
        asos = Asos(start, end, args).call()
        asos = asos_preprocessing(asos)
        print(f'ASOS Loaded')
        dataframe = pd.concat([asos, dataframe], axis=1)

    predict, ws = predict_step(dataframe, args)

    with connect_maria(args) as conn:
        insert_data(args, conn, start, pred=predict, ws=ws)
    print(f'insert data done.')

    ed = datetime.datetime.now()

    print(f'Elapsed time: {ed - st}')
