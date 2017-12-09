from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from datetime import datetime

import pandas as pd
from cassandra.query import tuple_factory, BatchStatement, dict_factory
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
                                    nan_count int, str_vals set<text>,
                                     PRIMARY KEY(table_name, col_name))'''
    )
    session.execute(
        '''CREATE TABLE IMAGES(fig_name text, table_name text, col_name text,
                               fig_title text, create_time timestamp,
                               PRIMARY KEY(fig_name, table_name, col_name))'''
    )
    session.execute('''CREATE TABLE RULES(table_name text, col_name text, operation list<text>,
                                          PRIMARY KEY(table_name, col_name))''')


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
    insert_target = session.prepare('INSERT INTO {}(idx) VALUES (?)'.format(name + '_TARGET'))
    data_batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    target_batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for idx, row in info.iterrows():
        values = list(row.values)
        values.insert(0, idx)
        data_batch.add(insert_data, tuple(values))
        target_batch.add(insert_target, (idx,))
        if (idx + 1) % 20 == 0:
            session.execute(data_batch)
            data_batch.clear()
    session.execute(data_batch)
    session.execute(target_batch)
    insert_col_info = session.prepare(
        '''INSERT INTO DATASETINFO (table_name, col_name, col_type,
                                    min_val, max_val, mean_val, nan_count, str_vals)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
    )
    data_batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for col_name, col_type in zip(info.columns, info.dtypes):
        col_data = info[col_name]
        if col_type == float:
            data_batch.add(insert_col_info,
                           (name, col_name, 'DOUBLE',
                            col_data.min(), col_data.max(), col_data.mean(), col_data.isna().sum(),
                            None))
        else:
            data_batch.add(insert_col_info, (name, col_name, 'STRING',
                                             None, None, None, None,
                                             set(col_data)))
    session.execute(data_batch)
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
    sess.execute('DROP TABLE IF EXISTS {}'.format(name))
    sess.execute('DROP TABLE IF EXISTS {}_TARGET'.format(name))
    sess.execute("DELETE from DATASETS where orig_table = %s", [name])
    sess.execute('DELETE FROM DATASETINFO WHERE table_name = %s', (name,))
    sess.execute('DELETE FROM DATASETINFO WHERE table_name = %s', (name + '_TARGET',))
    return None


def get_col_infos(table_name):
    sess = get_session()
    rows = sess.execute(
        '''SELECT col_name, col_type, min_val, max_val, mean_val, nan_count
              FROM DATASETINFO
           WHERE TABLE_NAME = %s
        ''', [table_name])
    return list(rows)


def get_col_info(table_name, col_name):
    sess = get_session()
    rows = sess.execute(
        '''SELECT col_type, min_val, max_val, mean_val, str_vals, nan_count
              FROM DATASETINFO
           WHERE TABLE_NAME = %s and COL_NAME = %s''',
        [table_name, col_name])
    rows = list(rows)
    return rows[0] if rows else None


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


def delete_image(name):
    sess = get_session()
    sess.execute('DELETE FROM IMAGES WHERE fig_name=%s', [name])
    return None


def get_operation(table_name, col_name):
    sess = get_session()
    sess.row_factory = tuple_factory
    rows = sess.execute("SELECT operation FROM RULES where table_name = %s and col_name = %s",
                        [table_name, col_name])
    return list(rows)


def add_target_column(table_name, col_name, fill_na_method):
    sess = get_session()
    idx_list, value_list = [], []
    for idx, value in sess.execute('SELECT idx,{} FROM {}'.format(col_name, table_name)):
        idx_list.append(idx)
        value_list.append(value)
    df = pd.DataFrame({'idx': idx_list, col_name: value_list})
    if fill_na_method == 'zero':
        df[col_name] = df[col_name].fillna(0)
    elif fill_na_method == 'mean':
        df[col_name] = df[col_name].fillna(df[col_name].mean())
    elif fill_na_method == 'median':
        df[col_name] = df[col_name].fillna(df[col_name].median())
    elif fill_na_method == 'min':
        df[col_name] = df[col_name].fillna(df[col_name].min())
    elif fill_na_method == 'max':
        df[col_name] = df[col_name].fillna(df[col_name].max())
    sess.execute("ALTER TABLE {} ADD ({} double)".format(table_name + "_target", col_name))
    update_col = sess.prepare(
        '''UPDATE {} SET {}=? WHERE idx=?'''.format(table_name + '_target', col_name)
    )
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    counter = 0
    for _, row in df.iterrows():
        batch.add(update_col, (row[col_name], row['idx']))
        counter += 1
        if counter % 1000 == 0:
            sess.execute(batch)
            batch.clear()
    sess.execute(batch)
    sess.execute('''INSERT INTO DATASETINFO (table_name, col_name, col_type,
                                min_val, max_val, mean_val, nan_count)
                        VALUES (%s, %s, 'DOUBLE', %s, %s, %s, %s)''',
                 [(table_name + '_target').upper(), col_name,
                  df[col_name].min(), df[col_name].max(), df[col_name].mean(),
                  df[col_name].isna().sum()])
    return None


def del_target_column(table_name, col_name):
    sess = get_session()
    sess.execute("ALTER TABLE {} DROP {}".format(table_name, col_name))
    sess.execute('DELETE FROM DATASETINFO WHERE table_name = %s and col_name = %s',
                 (table_name, col_name))
    return None
