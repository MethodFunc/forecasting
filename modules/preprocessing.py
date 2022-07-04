from collections import OrderedDict

import numpy as np
import pandas as pd


def read_data(data_list: list, args, processing=True):
    dataframe = OrderedDict()
    for data in data_list:
        key = list(data.keys())[0]
        dataframe[key] = pd.DataFrame(data[key]).T

        if processing:
            dataframe[key] = preprocessing_(dataframe[key], key, args)

    if args.univariate:
        dataframe = univariate_process(dataframe_dict=dataframe)

    return dataframe


def preprocessing_(dataframe_, key, args):
    dataframe = dataframe_.copy()
    dataframe.rename(columns={
        'INFO.SPEED.WIND': 'WS',
        'INFO.DIRECTION.WIND': 'WD',
        'INFO.PITCHANGLE.BLADE1': 'PB1',
        'INFO.PITCHANGLE.BLADE2': 'PB2',
        'INFO.PITCHANGLE.BLADE3': 'PB3',
        'INFO.SPEED.ROTOR': 'RP',
        'INFO.SPEED.GENERATOR': 'GTR',
        'INFO.POWER.ACTIVE': f'{key}_AP'
    }, inplace=True)

    dataframe = dataframe[['WD', 'WS', 'RP', 'PB1', 'PB2', 'PB3', 'GTR', f'{key}_AP']]

    dataframe.index = pd.to_datetime(dataframe.index)
    dataframe['PB'] = dataframe.apply(lambda x: np.mean([x['PB1'], x['PB2'], x['PB3']]), axis=1)
    dataframe[f'{key}_AP'] = dataframe[f'{key}_AP'].apply(lambda x: 0 if x < 0 else x)
    dataframe = dataframe[['WD', 'WS', 'RP', 'PB', 'GTR', f'{key}_AP']]

    if args.resample:
        dataframe = dataframe.resample(args.freq).mean()
        dataframe.fillna(0, inplace=True)

    return dataframe


def univariate_process(dataframe_dict):
    dataframe = pd.DataFrame()
    for key, values in dataframe_dict.items():
        dataframe = pd.concat([dataframe, values[f'{key}_AP']], axis=1)

    dataframe = dataframe.apply(lambda x: np.sum(x), axis=1)

    return dataframe


def asos_preprocessing(dataframe):
    dataframe['DT'] = pd.to_datetime(dataframe['DT'])
    dataframe.set_index('DT', inplace=True)

    return dataframe


def blade_pitch_process(x, y, z):
    if x == y == z:
        return x, y, z

    if (x != y) and (y == z) and (x != z):
        under = y - (y * 0.05)
        upper = y + (y * 0.05)

        if not (under < x < upper):
            x = y

        return x, y, z

    if (x != y) and (y != z) and (x == z):
        under = x - (x * 0.05)
        upper = x + (x * 0.05)

        if not (under < y < upper):
            y = x

        return x, y, z

    if (x == y) and (y != z) and (x != z):
        under = x - (x * 0.05)
        upper = x + (x * 0.05)

        if not (under < z < upper):
            z = x

        return x, y, z
