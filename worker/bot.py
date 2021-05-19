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
from datetime import datetime, timezone, timedelta
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


class Keys:
    def __init__(self):
        self.k = types.KeyboardButton
        self.b = types.InlineKeyboardButton

    def folders(self):
        keyboard = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        keyboard.add(*[self.k(button) for button in [os.environ['folder'].capitalize(), f"{'Fear'}&{'Greed'}"]])
        return keyboard

    def frames(self):
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.add(*[self.b(text, callback_data=f'{frame}') for frame, text in frames])
        return keyboard

    def currencies(self, frame):
        keyboard = types.InlineKeyboardMarkup(row_width=4)
        keyboard.add(*[self.b(name, callback_data=f'{name}_{frame}') for name in names])
        return keyboard


def images_db_creation():
    _names = []
    _frames = []
    raw_frames = []
    folder_id = None
    db = SQL(db_path)
    client = Drive('google.json')

    db.create_table('images', ['id <TEXT>', 'name', 'frame', 'path', 'last_update <DATE>'])
    allowed = os.environ['allowed'].split('/') if os.environ.get('allowed') else []

    for folder in client.files(only_folders=True):
        if folder['name'] == os.environ.get('folder'):
            folder_id = folder['id']

    for file in client.files(parents=folder_id):
        name = re.sub(r'\.jpg', '', file['name'])
        if name in allowed or len(allowed) == 0:
            frame = int(re.sub('[^0-9]', '', name))
            name = re.sub('[^A-Z]', '', name)
            path = f"images/{file['name']}"
            db.create_image({
                'name': name,
                'path': path,
                'frame': frame,
                'id': file['id'],
                'last_update': file['modifiedTime']})
            _names.append(name) if name not in _names else None
            raw_frames.append(frame) if frame not in raw_frames else None
            client.download_file(file['id'], path)
    for frame in sorted(raw_frames):
        text = '5 мин' if frame == 5 else '1 час'
        _frames.append((frame, text))
    db.close()
    return sorted(_names), _frames, client, folder_id


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
names, frames, drive_client, main_folder = images_db_creation()
zero_user, google_users_ids, users_columns = users_db_creation()
# =================================================================================================================
keys = Keys()


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
    return 'Добро пожаловать', keys.folders()


async def sender(message, user, text=None, keyboard=None, log_text=None, **a_kwargs):
    global logging
    dump = True if 'Впервые' in str(log_text) else None
    task = a_kwargs['func'] if a_kwargs.get('func') else bot.send_message
    kwargs = {'log': log_text, 'text': text, 'user': user, 'message': message, 'keyboard': keyboard, **a_kwargs}
    response, log_text, update = await Auth.async_message(task, **kwargs)
    if log_text is not None:
        logging.append(log_text)
        if dump:
            await Auth.async_message(bot.send_message, id=Auth.logs.dump_chat_id, text=log_text)
    if update:
        db = SQL(db_path)
        db.update('users', user['id'], update)
        db.close()
    return response


async def editor(call, user, text, keyboard, log_text=None):
    global logging
    await bot.answer_callback_query(call['id'])
    kwargs = {'log': log_text, 'call': call, 'text': text, 'user': user, 'keyboard': keyboard}
    response, log_text, update = await Auth.async_message(bot.edit_message_text, **kwargs)
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


@dispatcher.callback_query_handler()
async def callbacks(call):
    try:
        db = SQL(db_path)
        keyboard = call['message']['reply_markup']
        user = db.get_user(call['message']['chat']['id'])
        if user:
            text, log_text = None, None
            split = call['data'].split('_')
            if len(split) == 2:
                if split[0] in names:
                    image = db.get_image(name=split[0], frame=split[1])
                    await editor(call, user, text=text, keyboard=keyboard, log_text=log_text)
                    if image:
                        caption = None
                        if call['message']['chat']['id'] == idMe:
                            last_update = Auth.logs.time(image['last_update'], tag=bold, form='iso')
                            caption = f"Изображение: {bold(image['path'])}\nОбновлено: {last_update}"
                        await sender(call['message'], user, id=call['message']['chat']['id'],
                                     func=bot.send_photo, path=image['path'], caption=caption)
            else:
                for frame, f_text in frames:
                    if str(frame) in call['data']:
                        keyboard = keys.currencies(frame)
                        text = f'Выбран фрейм {f_text}, выберите валюту'
                await editor(call, user, text=text, keyboard=keyboard, log_text=log_text)
        db.close()
    except IndexError and Exception:
        await Auth.dev.async_except(call)


@dispatcher.message_handler()
async def repeat_all_messages(message: types.Message):
    try:
        db = SQL(db_path)
        user = db.get_user(message['chat']['id'])
        if user:
            keyboard = keys.folders()
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
                        text, log_text = Auth.logs.reboot(dispatcher)

                    elif message['text'].lower().startswith('/reload'):
                        query = "SELECT id FROM users WHERE reaction = '✅' AND NOT id = 0"
                        users = db.request(query)
                        for target_user in users:
                            await Auth.async_message(bot.send_message, id=target_user['id'],
                                                     text=bold('Бот обновлен'), keyboard=keys.folders())

                if message['text'].lower().startswith('/remove'):
                    await bot.send_message(message['chat']['id'], bold('Окей'),
                                           reply_markup=types.ReplyKeyboardRemove(True), parse_mode='HTML')

            elif message['text'].lower().startswith('f'):
                text = bold('Пример сообщения')

            elif message['text'].lower().startswith('h'):
                text = 'Выбор таймфрейма'
                keyboard = keys.frames()

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


def auto_reboot():
    global logging
    reboot = None
    while True:
        try:
            date = datetime.now(timezone(timedelta(hours=3)))
            if date.strftime('%M') == '01' and date.strftime('%M') == '00':
                reboot = True
            sleep(60)
            if reboot:
                reboot = None
                Auth.logs.reboot(dispatcher)
        except IndexError and Exception:
            Auth.dev.thread_except()


def start(stamp):
    if os.environ.get('local'):
        threads = [logger, google_update, auto_reboot]
        Auth.dev.printer(f'Запуск бота локально за {time_now() - stamp} сек.')
    else:
        Auth.dev.start(stamp)
        threads = [logger, google_update, google_files, auto_reboot]
        Auth.dev.printer(f'Бот запущен за {time_now() - stamp} сек.')

    for thread_element in threads:
        _thread.start_new_thread(thread_element, ())
    executor.start_polling(dispatcher, allowed_updates=objects.allowed_updates)


if os.environ.get('local'):
    start(stamp1)
