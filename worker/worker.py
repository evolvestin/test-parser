import os
import re
import objects
import heroku3
import _thread
from PIL import Image
from time import sleep
from chrome import chrome
from datetime import datetime
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
# ==================================================================================================================
last_update = int(datetime.now().timestamp())


def db_creation():
    data = {}
    folder_id = None
    client = objects.GoogleDrive(json_path)
    allowed = os.environ['allowed'].split('/') if os.environ.get('allowed') else []

    for folder in client.files(only_folders=True):
        if folder['name'] == os.environ.get('folder'):
            folder_id = folder['id']

    for file in client.files(parents=folder_id):
        name = re.sub(r'\.jpg', '', file['name'])
        if name in allowed:
            data[name] = file['id']
    return data, client


origin_files = os.listdir()
json_path = 'geocoding2.json'
objects.environmental_files()
db, drive_client = db_creation()
# ==================================================================================================================


def new_file():
    filename = max([f if os.path.isdir(f) is False else 0 for f in os.listdir()], key=os.path.getctime)
    if filename not in origin_files:
        return filename
    else:
        return None


def drive_updater(file_id, file_path):
    global drive_client
    try:
        drive_client.update_file(file_id, file_path)
    except IndexError and Exception:
        drive_client = objects.GoogleDrive(json_path)
        drive_client.update_file(file_id, file_path)


def checking_updated():
    while True:
        try:
            if int(datetime.now().timestamp()) - last_update > 600 and os.environ.get('api'):
                connection = heroku3.from_key(os.environ['api'])
                for app in connection.apps():
                    for dyno in app.dynos():
                        dyno.restart()
        except IndexError and Exception:
            pass


def updater(driver, name):
    global last_update
    currency = name.split('_')[0]
    period = int(name.split('_')[1])
    driver.get(f"{os.environ.get('link')}={currency}")
    WebDriverWait(driver, 20).until(ec.presence_of_element_located((By.CLASS_NAME, 'swap-long-short-trend-chart')))
    elements = driver.find_elements(By.CLASS_NAME, 'swap-long-short-trend-chart  ')
    if len(elements) == 2:
        ActionChains(driver).move_to_element(elements[1].find_element(By.TAG_NAME, 'canvas')).perform()
        if period == 60:
            div = elements[1].find_element(By.CLASS_NAME, 'select-white')
            div.click()
            sleep(2)

            for li in div.find_elements(By.TAG_NAME, 'li'):
                if li.text == '1H':
                    ActionChains(driver).move_to_element(li).click().perform()
                    break
        else:
            sleep(3)

        element = elements[1].find_element(By.TAG_NAME, 'canvas')
        ActionChains(driver).move_to_element_with_offset(element, 315, 35).click().perform()
        WebDriverWait(driver, 20).until(ec.presence_of_element_located(
            (By.CLASS_NAME, 'chart-share-modal-content-footer-download')))
        sleep(3)
        driver.find_element(By.CLASS_NAME, 'chart-share-modal-content-footer-download').click()
        sleep(5)
        downloaded = new_file()
        if downloaded:
            new_path = re.sub(r'\.png', '.jpg', downloaded)
            image = Image.open(downloaded)
            image = image.convert('RGB')
            os.remove(downloaded)
            image.save(new_path)
            drive_updater(db[name], new_path)
            os.remove(new_path)
            last_update = int(datetime.now().timestamp())
    driver.get('https://google.com')


def start(stamp):
    if os.environ.get('local') is None:
        print(f'Запуск на сервере за {objects.time_now() - stamp}')
    while True:
        chrome_client = None
        try:
            stamp = datetime.now().timestamp()
            chrome_client = chrome(os.environ.get('local'))
            for key in db:
                updater(chrome_client, key)
            chrome_client.close()
            _thread.start_new_thread(checking_updated, ())
            print(f"Проход {', '.join(db.keys())} за {datetime.now().timestamp() - stamp}")
        except IndexError and Exception as error:
            print(error)
            reboot = True
            if chrome_client:
                try:
                    chrome_client.close()
                    reboot = False
                except IndexError and Exception:
                    pass
            if reboot and os.environ.get('api'):
                connection = heroku3.from_key(os.environ['api'])
                for app in connection.apps():
                    for dyno in app.dynos():
                        dyno.restart()


if os.environ.get('local'):
    start(objects.time_now())
