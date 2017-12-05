from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from datetime import datetime

import pandas as pd
from cassandra.query import tuple_factory, BatchStatement
from pytz import timezone

cluster = Cluster(['database'])
keyspace = 'cpsc531'


def get_session(ks=keyspace):
    return cluster.connect(keyspace=ks)
    # if keyspace:
    #     if not hasattr(get_session, 'session_dict'):
    #         get_session.session_dict = dict()
    #     if keyspace not in get_session.session_dict:
    #         s = cluster.connect(keyspace)
    #         get_session.session_dict[keyspace] = s
    #         return s
    #     else:
    #         return get_session.session_dict.get(keyspace)
    # else:
    #     if not hasattr(get_session, 'session'):
    #         get_session.session = cluster.connect()
    #     return get_session.session


def init_database():
    session = get_session(None)
    session.execute("drop KEYSPACE if exists %s;" % keyspace)

    session.execute(
        """CREATE KEYSPACE %s WITH REPLICATION = {
                'class' : 'SimpleStrategy',
                'replication_factor' : %%s
        };""" % keyspace,
        (1,)
    )
    session.set_keyspace('cpsc531')
    session.execute(
        '''CREATE TABLE DATASETS(orig_table text, target_table text, create_time timestamp,
                                 PRIMARY KEY (orig_table));''',
    )
    session.execute(
        '''CREATE TABLE DATASETINFO(table_name text, col_name text, col_type text,
                                     min_val double, max_val double, mean_val double,
                                     PRIMARY KEY(table_name, col_name))'''
    )
    session.execute(
        '''CREATE TABLE IMAGES(fig_name text, table_name text, col_name text,
                               fig_title text, create_time timestamp,
                               PRIMARY KEY(fig_name, table_name, col_name))'''
    )


def add_dataset(name, csv_file):
    name = name.upper()
    session = get_session()
    info = pd.read_csv(csv_file, index_col=False)
    session.execute('CREATE TABLE {} (idx int, {}, PRIMARY KEY (idx))'.format(name, ', '.join(
        [col_name + (' double' if col_type == float else ' text')
         for col_name, col_type in zip(info.columns, info.dtypes)])))
    session.execute('CREATE TABLE {} (idx int, PRIMARY KEY(idx))'.format(name + '_TARGET'))
    session.execute(
        "INSERT INTO DATASETS (orig_table, target_table, create_time) VALUES (%s, %s, %s)",
        [name, name + "_TARGET", datetime.now()]
    )
    insert_data = session.prepare(
        'INSERT INTO {} (idx,{}) VALUES (?,{})'.format(name, ','.join(info.columns),
                                                       ','.join(['?'] * len(info.columns))))
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for idx, row in info.iterrows():
        values = list(row.values)
        values.insert(0, idx)
        batch.add(insert_data, tuple(values))
        if (idx + 1) % 20 == 0:
            session.execute(batch)
            batch.clear()
    session.execute(batch)
    insert_col_info = session.prepare(
        '''INSERT INTO DATASETINFO (table_name, col_name, col_type, min_val, max_val, mean_val)
                            VALUES (?,          ?,        ?,        ?,       ?,       ?)'''
    )
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for col_name, col_type in zip(info.columns, info.dtypes):
        col_data = info[col_name]
        if col_type == float:
            batch.add(insert_col_info,
                      (name, col_name, 'DOUBLE', col_data.min(), col_data.max(), col_data.mean()))
        else:
            batch.add(insert_col_info, (name, col_name, 'STRING', None, None, None))
    session.execute(batch)
    return info


def exist_data_set(name):
    sess = get_session()
    sess.row_factory = tuple_factory
    rows = sess.execute("SELECT orig_table FROM DATASETS where orig_table = %s", [name])
    return len(list(rows))


def get_data_sets():
    sess = get_session()
    rows = sess.execute("SELECT orig_table, target_table, create_time FROM DATASETS")
    return list(rows)


def delete_data_set(name):
    sess = get_session()
    sess.execute("DELETE from DATASETS where orig_table = %s", [name])
    sess.execute('DROP TABLE IF EXISTS {}'.format(name))
    sess.execute('DROP TABLE IF EXISTS {}_TARGET'.format(name))
    return None


def get_col_infos(table_name):
    sess = get_session()
    rows = sess.execute(
        '''SELECT col_name, col_type, min_val, max_val, mean_val
              FROM DATASETINFO
           WHERE TABLE_NAME = %s
        ''', [table_name])
    return list(rows)


def get_col_info(table_name, col_name):
    sess = get_session()
    rows = sess.execute(
        '''SELECT col_type, min_val, max_val, mean_val
              FROM DATASETINFO
           WHERE TABLE_NAME = %s and COL_NAME = %s''',
        [table_name, col_name])
    return rows[0]


def get_col_values(table_name, col_name):
    sess = get_session()
    rows = sess.execute('SELECT {} FROM {}'.format(col_name, table_name))
    return [row[0] for row in rows]


def insert_image(fig_name, table_name, col_name, fig_title):
    sess = get_session()
    sess.execute('''INSERT INTO IMAGES(fig_name, table_name, col_name, fig_title, create_time)
                                VALUES (%s, %s, %s, %s, %s)''',
                 [fig_name, table_name, col_name, fig_title, datetime.now()])
    return None


def get_all_images(table_name, col_name):
    sess = get_session()
    rows = sess.execute('''SELECT fig_title, fig_name, create_time FROM IMAGES
                             WHERE table_name = %s and col_name = %s ALLOW FILTERING''',
                        [table_name, col_name])
    return list(rows)
