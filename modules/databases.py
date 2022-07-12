import re
from collections import OrderedDict

import numpy as np
import pymongo
import pymysql
from joblib import Parallel, delayed


def database_list(client, name=None) -> list:
    """
        client: Connected Mongo database
        name: Generator Name (support recurrent database schema)
        return: list
    """
    if name is not None:
        list_database = client.list_database_names()
        return [x for x in list_database if name in x]
    else:
        return client.list_database_names()


def define_pipeline(start: str, end: str) -> list:
    """
        start: string datetime gte -> "2020-12-01"
        end: string datetime lt -> "2020-12-02"
        --> find 2020-12-01 datas
    """
    pipelines = [
        {"$match": {
            "date": {
                "$gte": f"{end}",
                "$lt": f"{start}",
            }
        }
        },

        {"$group": {
            "_id": "$date",
            "tag_values": {
                "$push": {
                    "$cond": [
                        {
                            "$eq": ["$tagValue", "$ifNull"]
                        },
                        np.nan, "$tagValue"
                    ]
                }
            },
            "_cols": {"$push": "$tagNm"},
        }
        },
    ]

    return pipelines


def db_collections(cursor, gen_name=None):
    if gen_name is not None:
        gen_list = [x for x in cursor.list_collection_names() if gen_name in x]
        gen_list.sort()
    else:
        gen_list = cursor.list_collection_names()
        gen_list.sort()

    return gen_list


def export_data(pipelines, cursor, gen):
    gen_dict = OrderedDict()
    pattern = r'[A-Z]{2}\.[A-Z]{2}[0-9]{2}\.*'
    search_col = ['INFO.SPEED.WIND', 'INFO.DIRECTION.WIND', 'PITCHANGLE.BLADE1', 'PITCHANGLE.BLADE2',
                  'PITCHANGLE.BLADE3', 'INFO.SPEED.ROTOR', 'INFO.SPEED.GENERATOR', 'INFO.POWER.ACTIVE']

    name = gen.replace('.', '_')
    result = cursor.get_collection(gen).aggregate(pipelines, allowDiskUse=True)
    data = OrderedDict()
    for post in result:
        new_cols = [re.sub(pattern, '', x) for x in post['_cols']]
        index_col = [new_cols.index(x) for x in new_cols for y in search_col if y in x]
        define_col = [new_cols[index] for index in index_col]
        define_values = [float(post['tag_values'][x]) for x in index_col]
        data[post['_id']] = {key: value for key, value in zip(define_col, define_values)}

    gen_dict[name] = data

    return gen_dict


def active_database(pipelines, args):
    with pymongo.MongoClient(args.host, args.port) as client:
        cursor = client[args.database_name]

        gen_list = db_collections(cursor=cursor, gen_name=args.gen_name)
        gen_list.sort()

        with Parallel(n_jobs=-1, backend='threading') as parallel:
            result = parallel(delayed(export_data)(pipelines=pipelines, cursor=cursor, gen=gen) for gen in gen_list)

    return result


def connect_maria(args):
    conn = pymysql.Connect(host=args.host, port=args.mport, user=args.user, password=args.paswd,
                           database=args.database_name, autocommit=True)

    return conn


def insert_data(args, conn, date, pred=None, ws=None, wh=None):
    cursor = conn.cursor()
    if args.location == 'jeju':
        ltype = 'sea'
    else:
        ltype = 'ground'

    sql = f"INSERT INTO date_table VALUES (NULL, {date}, {ltype});"
    cursor.execute(sql)

    if pred is not None:
        pred_sql = f"INSERT INTO power_table VALUES (NULL, {date}, {ltype}, {args.gen_name}, {pred[0]}, {pred[1]}, {pred[2]}," \
                   f"{pred[3]}, {pred[4]}, {pred[5]}, {pred[6]}, {pred[7]}, {pred[8]}, {pred[9]}, {pred[10]}, {pred[11]}," \
                   f"{pred[12]}, {pred[13]}, {pred[14]}, {pred[15]}, {pred[16]}, {pred[17]}, {pred[18]}, {pred[19]}, " \
                   f"{pred[20]}, {pred[21]}, {pred[22]}, {pred[23]})"
        cursor.execute(pred_sql)

    if ws is not None:
        ws_sql = f"INSERT INTO wind_table VALUES (NULL, {date}, {ltype}, {args.gen_name}, {ws[0]}, {ws[1]}, {ws[2]}," \
                 f"{ws[3]}, {ws[4]}, {ws[5]}, {ws[6]}, {ws[7]}, {ws[8]}, {ws[9]}, {ws[10]}, {ws[11]}," \
                 f"{ws[12]}, {ws[13]}, {ws[14]}, {ws[15]}, {ws[16]}, {ws[17]}, {ws[18]}, {ws[19]}, " \
                 f"{ws[20]}, {ws[21]}, {ws[22]}, {ws[23]})"
        cursor.execute(ws_sql)

    if wh is not None:
        wh_sql = f"INSERT INTO height_table VALUES (NULL, {date}, {ltype}, {args.gen_name}, {wh[0]}, {wh[1]}, {wh[2]}," \
                 f"{wh[3]}, {wh[4]}, {wh[5]}, {wh[6]}, {wh[7]}, {wh[8]}, {wh[9]}, {wh[10]}, {wh[11]}," \
                 f"{wh[12]}, {wh[13]}, {wh[14]}, {wh[15]}, {wh[16]}, {wh[17]}, {wh[18]}, {wh[19]}, " \
                 f"{wh[20]}, {wh[21]}, {wh[22]}, {wh[23]})"
        cursor.execute(wh_sql)
