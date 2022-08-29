import argparse


def define_args():
    parser = argparse.ArgumentParser()
    args = parser.parse_args('')

    # Database Setting
    args.old = True
    args.host = '*'
    args.port = '*'

    args.mport = 3367
    args.user = 'dev'
    args.paswd = '1234'

    args.location = 'jeju'
    args.mdb_name = 'forecast'

    # Setting Date
    args.current = False
    args.year = 2022
    args.month = 4
    args.days = 30
    args.range_ = 2

    # Old Database Setting
    args.database_name = 'jeju'
    args.gen_name = 'DB.HJ'

    # preprocessing setting
    args.resample = True
    args.freq = '1H'
    args.univariate = True  # 단변량 시 AP만 합치기

    # kma api
    args.use_asos = False
    args.asos_key = '*'
    args.sticks = 184  # jeju 184
    args.data_type = 'JSON'  # Support XML, JSON
    if args.range_ > 1:
        args.one_day = False
    else:
        args.one_day = True

    # Path
    args.model_path = './models/model.h5'
    args.scale_path = './models/scaler.pkl'

    return args
