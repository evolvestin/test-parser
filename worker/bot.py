# -*- coding: utf-8 -*-
import os
import re
import asyncio
import gspread
import objects
import _thread
from SQL import SQL
from time import sleep
from aiogram import types
from copy import copy, deepcopy
from string import ascii_uppercase
from aiogram.utils import executor
from objects import bold, code, time_now
from objects import GoogleDrive as Drive
from aiogram.dispatcher import Dispatcher
# =================================================================================================================
stamp1 = time_now()


def users_db_creation():
    db = SQL(db_path)
    spreadsheet = gspread.service_account('google.json').open('UNITED USERS')
    users = spreadsheet.worksheet(os.environ['folder']).get('A1:Z50000', major_dimension='ROWS')
    raw_columns = db.create_table('users', users.pop(0), additional=True)
    users_ids, columns = db.upload('users', raw_columns, users)
    _zero_user = db.get_user(0)
    db.close()
    return _zero_user, ['id', *users_ids], columns


def images_db_creation():
    names = []
    buttons = []
    raw_names = []
    folder_id = None
    db = SQL(db_path)
    client = Drive('google.json')
    keyboard = types.ReplyKeyboardMarkup(row_width=4, resize_keyboard=True)
    db.create_table('images', ['id <TEXT>', 'name', 'path', 'last_update <DATE>'])
    allowed = os.environ['allowed'].split('/') if os.environ.get('allowed') else []

    for folder in client.files(only_folders=True):
        if folder['name'] == os.environ.get('folder'):
            folder_id = folder['id']

    for file in client.files(parents=folder_id):
        name = re.sub(r'\.jpg', '', file['name'])
        if name in allowed or len(allowed) == 0:
            raw_names.append(name)
            name = re.sub('_', ' ', name)
            name = re.sub('5', '5 мин', name)
            name = re.sub('60', '1 час', name)
            path = f"images/{file['name']}"
            db.create_image({
                'name': name,
                'path': path,
                'id': file['id'],
                'last_update': file['modifiedTime']})
            client.download_file(file['id'], path)

    names_db = {}
    for name in raw_names:
        prefix, postfix = name.split('_')
        name = re.sub('_', ' ', name)
        name = re.sub('5', '5 мин', name)
        name = re.sub('60', '1 час', name)
        if names_db.get(prefix) is None:
            names_db[prefix] = {}
        names_db[prefix][name] = int(re.sub(r'\D', '', postfix))

    for key in names_db:
        names_db[key] = sorted(names_db[key], reverse=True)

    for key in sorted(names_db, key=lambda x: x):
        for name in names_db[key]:
            names.append(name)
            buttons.append(types.KeyboardButton(name))

    keyboard.add(*buttons)
    db.close()
    return names, client, keyboard, folder_id


logging = []
idMe = 396978030
db_path = 'db/database.db'
objects.environmental_files()
os.makedirs('db', exist_ok=True)
os.makedirs('images', exist_ok=True)
Auth = objects.AuthCentre(LOG_DELAY=15,
                          ID_DEV=-1001312302092,
                          ID_DUMP=-1001486338288,
                          ID_LOGS=-1001275893652,
                          ID_MEDIA=-1001423966952,
                          ID_FORWARD=-1001254536149,
                          TOKEN=os.environ.get('TOKEN'),
                          DEV_TOKEN=os.environ['DEV_TOKEN'])

bot = Auth.async_bot
dispatcher = Dispatcher(bot)
zero_user, google_users_ids, users_columns = users_db_creation()
keys_names, drive_client, static_keys, main_folder = images_db_creation()
# =================================================================================================================


def first_start(message):
    db = SQL(db_path)
    user = deepcopy(zero_user)
    _, name, username = Auth.logs.header(message['chat'].to_python())
    user.update({
        'name': name,
        'username': username,
        'id': message['chat']['id']})
    db.create_user(user)
    db.close()
    return 'Добро пожаловать', static_keys


async def sender(message, user, text=None, keyboard=None, log_text=None):
    global logging
    kwargs = {'log': log_text, 'text': text, 'user': user, 'message': message, 'keyboard': keyboard}
    response, log_text, update = await Auth.async_message(bot.send_message, **kwargs)
    if log_text is not None:
        logging.append(log_text)
    if update:
        db = SQL(db_path)
        db.update('users', user['id'], update)
        db.close()
    return response


@dispatcher.chat_member_handler()
@dispatcher.my_chat_member_handler()
async def member_handler(message: types.ChatMember):
    global logging
    try:
        db = SQL(db_path)
        text, keyboard = None, None
        user = db.get_user(message['chat']['id'])
        log_text, update, greeting = Auth.logs.chat_member(message, db.get_user(message['chat']['id']))
        if greeting:
            text = 'Добро пожаловать, снова'
            if user is None:
                await asyncio.sleep(1)
                text, keyboard = first_start(message)
                if message['chat']['type'] == 'channel':
                    text = None
        logging.append(log_text)
        db.update('users', message['chat']['id'], update) if update else None
        keyboard = keyboard if message['chat']['type'] != 'channel' else None
        await Auth.async_message(bot.send_message, id=message['chat']['id'], text=text, keyboard=keyboard)
        db.close()
    except IndexError and Exception:
        await Auth.dev.async_except(message)


@dispatcher.message_handler(content_types=objects.red_contents)
async def red_messages(message: types.Message):
    try:
        db = SQL(db_path)
        text, keyboard = None, None
        user = db.get_user(message['chat']['id'])
        if user and message['migrate_to_chat_id']:
            db.update('users', user['id'], {'username': 'DISABLED_GROUP', 'reaction': '🅾️'})
        await sender(message, user, text, keyboard, log_text=True)
        db.close()
    except IndexError and Exception:
        await Auth.dev.async_except(message)


@dispatcher.message_handler()
async def repeat_all_messages(message: types.Message):
    try:
        db = SQL(db_path)
        user = db.get_user(message['chat']['id'])
        if user:
            keyboard = static_keys
            text, response, log_text = None, None, True

            if message['text'].startswith('/'):
                if message['text'].lower().startswith('/st'):
                    text = 'Добро пожаловать, снова'

                if message['chat']['id'] == idMe:
                    if message['text'].lower().startswith('/info'):
                        text = ''
                        images = db.get_images()
                        now = Auth.logs.time(form='iso', tag=bold)
                        for image in images:
                            name = re.sub('images/', '', image['path'])
                            last_update = Auth.logs.time(image['last_update'], tag=bold, form='iso')
                            text += f'{name}: {last_update}\n'
                        text += f"{code('-' * 30)}\nСейчас: {now}"

                    elif message['text'].lower().startswith('/logs'):
                        text = Auth.logs.text()

                    elif message['text'].lower().startswith('/reboot'):
                        text, log_text = objects.heroku_reboot()

            elif message['text'] in keys_names:
                image = db.get_image(message['text'])
                if image:
                    caption = None
                    response = True
                    if message['chat']['id'] == idMe:
                        last_update = Auth.logs.time(image['last_update'], tag=bold, form='iso')
                        caption = f"Изображение: {bold(image['path'])}\n" \
                                  f"Обновлено: {last_update}"
                    await Auth.async_message(Auth.async_bot.send_photo, id=message['chat']['id'],
                                             path=image['path'], caption=caption)

            await sender(message, user, text, keyboard, log_text=log_text)
            if text is None and response is None:
                task = Auth.async_bot.forward_message
                await Auth.logs.async_message(task, id=Auth.logs.dump_chat_id, message=message)
        else:
            text, keyboard = first_start(message)
            await sender(message, user, text, keyboard, log_text=' [#Впервые]')
        db.close()
    except IndexError and Exception:
        await Auth.dev.async_except(message)


def google_update():
    global google_users_ids
    while True:
        try:
            sleep(2)
            db = SQL(db_path)
            users = db.get_updates()
            if len(users) > 0:
                client = gspread.service_account('google.json')
                worksheet = client.open('UNITED USERS').worksheet(os.environ['folder'])
                for user in users:
                    del user['updates']
                    if str(user['id']) in google_users_ids:
                        text = 'обновлен'
                        row = google_users_ids.index(str(user['id'])) + 1
                    else:
                        text = 'добавлен'
                        row = len(google_users_ids) + 1
                        google_users_ids.append(str(user['id']))
                    google_row = f'A{row}:{ascii_uppercase[len(user)-1]}{row}'

                    try:
                        user_range = worksheet.range(google_row)
                    except IndexError and Exception as error:
                        if 'exceeds grid limits' in str(error):
                            worksheet.add_rows(1000)
                            user_range = worksheet.range(google_row)
                            sleep(5)
                        else:
                            raise error

                    for index, value, col_type in zip(range(len(user)), user.values(), users_columns):
                        value = Auth.time(value, form='iso', sep='_') if '<DATE>' in col_type else value
                        value = 'None' if value is None else value
                        user_range[index].value = value
                    worksheet.update_cells(user_range)
                    db.update('users', user['id'], {'updates': 0}, True)
                    Auth.dev.printer(f"Пользователь {text} {user['id']}")
                    sleep(1)
        except IndexError and Exception:
            Auth.dev.thread_except()


def google_files():
    while True:
        try:
            db = SQL(db_path)
            files = drive_client.files(parents=main_folder)
            for file in files:
                image = db.get_image_by_id(file['id'])
                if image:
                    if image['last_update'] < file['modifiedTime']:
                        drive_client.download_file(file_id=file['id'], file_path=image['path'])
                        db.update('images', file['id'], {'last_update': file['modifiedTime']})
            sleep(5)
        except IndexError and Exception:
            Auth.dev.thread_except()


def logger():
    global logging
    while True:
        try:
            log = copy(logging)
            logging = []
            Auth.logs.send(log)
        except IndexError and Exception:
            Auth.dev.thread_except()


def start(stamp):
    if os.environ.get('local'):
        threads = [logger, google_update]
        Auth.dev.printer(f'Запуск бота локально за {time_now() - stamp} сек.')
    else:
        Auth.dev.start(stamp)
        threads = [logger, google_update, google_files]
        Auth.dev.printer(f'Бот запущен за {time_now() - stamp} сек.')

    for thread_element in threads:
        _thread.start_new_thread(thread_element, ())
    executor.start_polling(dispatcher, allowed_updates=objects.allowed_updates)


if os.environ.get('local'):
    start(stamp1)
