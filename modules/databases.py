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
    default = f'INSERT INTO {args.mdb_name} (date, location, generator'
    try:
        if (pred is not None) and (ws is not None) and (wh is not None):
            sql = f'''{default}, ap, ws, wh) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(pred)}", "{list(ws)}", "{list(wh)}");'''
        elif (pred is not None) and (ws is not None) and (wh is None):
            sql = f'''{default}, ap, ws) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(pred)}", "{list(ws)}");'''
        elif (pred is not None) and (ws is None) and (wh is not None):
            sql = f'''{default}, ap, wh) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(pred)}", "{list(wh)}");'''
        elif (pred is None) and (ws is not None) and (wh is not None):
            sql = f'''{default}, ws, wh) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(ws)}", "{list(wh)}");'''
        elif (pred is not None) and (ws is None) and (wh is None):
            sql = f'''{default}, ap) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(pred)}");'''
        elif (pred is None) and (ws is not None) and (wh is None):
            sql = f'''{default}, ws) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(ws)}");'''
        elif (pred is None) and (ws is None) and (wh is not None):
            sql = f'''{default}, wh) VALUES  ("{date}", "{args.location}", "{args.gen_name}", "{list(wh)}");'''
        else:
            raise f'check your data'
        cursor.execute(sql)

    except Exception as err:
        print(err)
