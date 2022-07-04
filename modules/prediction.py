import pickle

import numpy as np
import pandas as pd
import tensorflow as tf


def load_modules(args):
    model = tf.keras.models.load_model(args.model_path)
    with open(args.scale_path, 'rb') as f:
        scale = pickle.load(f)

    return model, scale


def predict_step(dataframe, args):
    model, scale = load_modules(args)

    try:
        if isinstance(dataframe, pd.core.frame.DataFrame) or isinstance(dataframe, pd.core.series.Series):
            dataframe = dataframe.values
        else:
            dataframe = np.array(dataframe)
    except Exception as err:
        repr(f'Data before scale error: {err}')

    if args.univariate:
        dataframe = dataframe.reshape(-1, 1)

    data = scale.transform(dataframe)

    data = np.expand_dims(data, axis=0)

    predict = model.predict(data)
    predict = scale.inverse_transform(predict)

    return predict.ravel()
