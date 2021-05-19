# -*- coding: utf-8 -*-
import re
import sqlite3
from objects import divide, stamper, time_now
sql_patterns = ['database is locked', 'no such table']


class SQL:
    def __init__(self, database):
        def dict_factory(cursor, row):
            dictionary = {}
            for idx, col in enumerate(cursor.description):
                dictionary[col[0]] = row[idx]
            return dictionary
        self.connection = sqlite3.connect(database, timeout=100, check_same_thread=False)
        self.connection.execute('PRAGMA journal_mode = WAL;')
        self.connection.execute('PRAGMA synchronous = OFF;')
        self.connection.row_factory = dict_factory
        self.cursor = self.connection.cursor()

    # ------------------------------------------------------------------------------------------ UTILITY BEGIN
    def request(self, sql, fetchone=False):
        lock = True
        while lock is True:
            lock = False
            try:
                with self.connection:
                    self.cursor.execute(sql)
            except IndexError and Exception as error:
                for pattern in sql_patterns:
                    if pattern in str(error):
                        lock = True
                if lock is False:
                    raise error

        if fetchone:
            return self.cursor.fetchone()
        else:
            return self.cursor.fetchall()

    def close(self):
        self.connection.close()

    def update(self, table, item_id, dictionary, google_update=None):
        if table == 'users' and google_update is None:
            dictionary.update({'last_update': time_now(), 'updates': 1})
        self.request(f"UPDATE {table} SET {self.upd_kv(dictionary)} WHERE id = '{item_id}'")
    # ------------------------------------------------------------------------------------------ UTILITY END

    # ------------------------------------------------------------------------------------------ TRANSFORM BEGIN
    @staticmethod
    def ins_dict_items(dictionary):
        """Преобразование dict в строки с keys и values (только для INSERT или REPLACE)"""
        values = []
        for key in dictionary:
            value = dictionary.get(key)
            if value is None:
                values.append('NULL')
            elif type(value) == dict:
                values.append(f'"{value}"')
            else:
                values.append(f"'{value}'")
        return ', '.join(dictionary.keys()), ', '.join(values)

    @staticmethod
    def upd_kv(dictionary):
        """Преобразование dict в строку key=value, key=value ... (только для UPDATE)"""
        items = []
        for key in dictionary:
            value = dictionary.get(key)
            if value is None:
                value = 'NULL'
            elif type(value) == dict:
                value = f'"{value}"'
            elif type(value) == list and len(value) == 1 and type(value[0]) == str:
                value = value[0]
            else:
                value = f"'{value}'"
            items.append(f'{key}={value}')
        return ', '.join(items)

    def ins_kv(self, dictionary):
        """Готовая строка значений для запроса (только для INSERT или REPLACE)"""
        keys, values = self.ins_dict_items(dictionary)
        return f'({keys}) VALUES ({values})'
    # ------------------------------------------------------------------------------------------ TRANSFORM END

    # ------------------------------------------------------------------------------------------ CREATION BEGIN
    @staticmethod
    def google_columns(raw_columns, additional=None):
        keys = []
        columns = []
        combined = []
        additional = ['updates <INTEGER>'] if additional else []
        for raw in [*raw_columns, *additional]:
            value = 'TEXT'
            search = re.search('<(.*?)>', raw)
            key = re.sub('<.*?>', '', raw).strip()
            if search:
                value = 'INTEGER' if search.group(1) == 'DATE' else search.group(1)
                value += ' DEFAULT 0' if value == 'INTEGER' else ''
                value += ' UNIQUE' if key == 'id' else ''
            combined.append(f'{raw} {value}')
            columns.append(f'{key} {value}')
            keys.append(key)
        return ', '.join(keys), columns, combined

    def create_table(self, table, raw_columns, additional=None):
        _, columns, _ = self.google_columns(raw_columns, additional=additional)
        self.request(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})")
        return raw_columns

    def upload(self, table, raw_columns, array):
        all_values = []
        collected_ids = []
        keys, _, columns = self.google_columns(raw_columns)
        columns_range = range(0, len(columns))
        for key in array:
            collected_ids.append(key[0])
            if len(key) == len(columns):
                values = []
                for i in columns_range:
                    if 'TEXT' in columns[i] and key[i] == 'None':
                        values.append('NULL')
                    elif 'DATE' in columns[i]:
                        values.append(f'{stamper(key[i])}')
                    else:
                        values.append(f"'{key[i]}'")
                all_values.append(f"({', '.join(values)})")
        for values in divide(all_values):
            self.request(f"INSERT OR REPLACE INTO {table} ({keys}) VALUES {', '.join(values)}")
        return collected_ids, columns

    # ------------------------------------------------------------------------------------------ CREATION END

    # ------------------------------------------------------------------------------------------ USERS BEGIN
    def get_updates(self):
        return self.request('SELECT * FROM users WHERE updates = 1')

    def get_user(self, user_id):
        return self.request(f"SELECT * FROM users WHERE id = '{user_id}'", fetchone=True)

    def create_user(self, user):
        user.update({'last_update': time_now(), 'updates': 1})
        self.request(f'REPLACE INTO users {self.ins_kv(user)}')
    # ------------------------------------------------------------------------------------------ USERS END

    # ------------------------------------------------------------------------------------------ IMAGES BEGIN
    def create_image(self, image):
        self.request(f'REPLACE INTO images {self.ins_kv(image)}')

    def get_images(self):
        return self.request('SELECT * FROM images ORDER BY last_update DESC')

    def get_image_by_id(self, file_id):
        return self.request(f"SELECT * FROM images WHERE id = '{file_id}'", fetchone=True)

    def get_image(self, frame, name):
        return self.request(f"SELECT * FROM images WHERE name = '{name}' AND frame = '{frame}'", fetchone=True)
    # ------------------------------------------------------------------------------------------ IMAGES END
