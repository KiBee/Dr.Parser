import datetime
import json
import os
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from network_config import WEB_HEADERS

months_dict = {
    'января': 'Jan',
    'февраля': 'Feb',
    'марта': 'Mar',
    'апреля': 'Apr',
    'мая': 'May',
    'июня': 'June',
    'июля': 'Jul',
    'августа': 'Aug',
    'сентября': 'Sep',
    'октября': 'Oct',
    'ноября': 'Nov',
    'декабря': 'Dec'
}

wd_list = ['4WD', 'передний', 'задний']
transmission_list = ['механика', 'АКПП', 'вариатор', 'робот']
fuel_list = ['бензин', 'дизель', 'гибрид', 'электро']


def html_response(url, headers):
    for _ in range(3):
        try:
            response = requests.get(url, headers=headers)
            return response.text
        except ConnectionError or TimeoutError or ConnectionResetError:
            print("\n*****ConnectionError, TimeoutError or ConnectionResetError*"
                  "****\n\nI will retry again after 7 seconds...")
            sleep(7)
            print('Making another request...')


def get_date(date, today):
    """Получение даты и времени публикации"""

    if 'сегод' in date:
        date = today.date()

    elif 'минут' in date:
        try:
            delta_minutes = int(date.split(' ')[0])
        except:
            delta_minutes = 1
        date = (today - datetime.timedelta(minutes=delta_minutes)).date()

    elif 'час' in date:
        delta_hours = int(date.split(' ')[0])
        date = (today - datetime.timedelta(hours=delta_hours)).date()

    else:
        day, month = date.split(' ')
        month = months_dict.get(month)

        if today.month == 1 and month == 'Dec':
            year = today.year - 1
        else:
            year = today.year

        date = datetime.datetime.strptime(
            f'{day} {month} {year}', '%d %b %Y').date()

    return date


def get_car_info(about):
    description = about.get('description')
    if description:
        description = description.replace(
            '\n', ' ').replace('\r', '').replace('  ', ' ')

    brand = about.get('brand').get('name')
    brand_len = len(brand) or 0

    name = about.get('name').split(', ')[0][brand_len + 1:]

    new = {
        'brand': brand,
        'name': name,
        'bodyType': about.get('bodyType'),
        'color': about.get('color'),
        'fuelType': about.get('fuelType'),
        'year': about.get('modelDate'),
        'mileage': None,
        'transmission': about.get('vehicleTransmission'),
        'power': None,
        'price': None,
        'vehicleConfiguration': about.get('vehicleConfiguration'),
        'engineName': None,
        'engineDisplacement': None,
        'date': None,
        'location': None,
        'link': None,
        'description': description,
    }

    vehicle = about.get('vehicleEngine')
    if vehicle:
        new.update({
            'engineName': vehicle.get('name'),
            'engineDisplacement': vehicle.get('engineDisplacement')
        })
    return new


def parse_page_response(response):
    today = datetime.datetime.today()
    soup = BeautifulSoup(response, 'html.parser')
    js_data = soup.find_all('script', type='application/ld+json')[1:21]

    links = soup.find('div', class_='css-0 e1m0rp605')
    if not links:
        print('Bad links!')

    main_page_data = []

    for js_item, full_header, price, date_loc, link, mileage in zip(
            js_data,
            soup.find_all('a', attrs={"data-ftid": "bulls-list_bull"}),
            soup.find_all('span', class_="css-46itwz e162wx9x0"),
            soup.find_all('div', class_="css-1x4jcds eotelyr0"),
            links.find_all('a', attrs={"data-ftid": "bulls-list_bull"}),
            soup.find_all('span', class_='css-1l9tp44 e162wx9x0')
    ):
        js_info = json.loads(js_item.contents[0])
        js_info = get_car_info(js_info)

        mileage = None
        power = None

        buf = full_header.find_all('span', class_='css-1l9tp44 e162wx9x0')
        for item in buf:
            if 'тыс' in item.text:
                mileage = int(item.text.replace(' ', '').replace(
                    'тыс.км', '000').replace(',', '').replace('<', ''))
                break

        split_header = full_header.text.split(', ')
        for item in split_header:
            if 'л.с.' in item:
                power = item.rsplit('(')[-1][:-6]
                try:
                    power = int(power)
                    if power > 2000:
                        power = None
                except:
                    power = None
                break

        price = price.text[:-2].replace(u'\xa0', u'')
        date = get_date(date_loc.div.text, today)
        location = date_loc.span.text
        link = link.get('href')

        js_info.update({
            'mileage': mileage,
            'power': power,
            'price': price,
            'date': date,
            'location': location,
            'link': link,
        })
        main_page_data.append(js_info)

    return main_page_data


def start(region, pages_count):
    """
    Основная функция по парсингу страниц дрома по регионам и сохранением результатов

    :param region: Номер региона
    :param pages_count: Количество страниц, которые будут обработаны
    """

    print(f'Start {region} region!')
    main_folder = 'drom/'

    region = f'region{region}'
    folder = f'{main_folder}{region}/'
    MANE_PAGE_YFA = f'https://auto.drom.ru/{region}/all/page'

    # Создаем главную папку с регионами
    if not os.path.exists(main_folder):
        print(f'Create {main_folder[:-1]} folder...')
        os.mkdir(main_folder)

    now_datetime = datetime.datetime.now()
    today_date = now_datetime.date()
    today_hour = now_datetime.hour

    responses = []  # Необработанные страницы
    result = []  # Обработанные страницы
    errors = []

    # Парсим страницы для дальнейшей обработки
    for page in tqdm(range(pages_count), desc='Collect Pages'):
        page_response = html_response(MANE_PAGE_YFA + str(page + 1), WEB_HEADERS)
        responses.append(page_response)

    # Обрабатываем собранные страницы
    for idx in tqdm(range(pages_count), desc='Parsing Pages'):
        try:
            page_records = parse_page_response(responses[idx])

            for i in page_records:
                result.append(i)
        except:
            errors.append(responses[idx])

    if errors:
        print(f'Errors: {len(errors)}')

    if len(result) == 0:
        print('PARSING ERROR: EMPTY CSV FILE!')
        print('PARSING ERROR: EMPTY CSV FILE!')
        print('PARSING ERROR: EMPTY CSV FILE!')

        return

    # Создаем папку региона
    if result and not os.path.exists(folder):
        print(f'Create {folder[:-1]} folder...')
        os.mkdir(folder)

    result_ = pd.DataFrame(result)

    if len(errors) > 0:
        csv_name = f'{folder}drom_{region}_{today_date}_{today_hour:02d}_{len(errors):02d}errors.csv'
    else:
        csv_name = f'{folder}drom_{region}_{today_date}_{today_hour:02d}.csv'

    result_.to_csv(csv_name)
    print(f'Saved to {csv_name}')


def main():
    pages_count = 100  # Номер страницы, до которой будет производиться парсинг (макс 100)
    # regions = [41, 25]  # Номера регионов

    regions = [i for i in range(1, 103)]
    bad_regions = [i for i in range(80, 86)] + [i for i in range(87, 89)] + [i for i in range(90, 101)]

    for bad_region in bad_regions:
        regions.remove(bad_region)

    for region in regions:
        try:
            start(region, pages_count)
        except:
            print(f'{region} is Bad!')


main()
