import asyncio
import json
import os
import random
import urllib.parse
from multiprocessing import Process

import requests
from bs4 import BeautifulSoup
from proxybroker import Broker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from robobrowser import RoboBrowser
from antigate import AntiGate

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
BASE_URL = 'https://www.reformagkh.ru'


def get_html(url, user_agent=None, proxy=None):
    r = requests.get(url, headers=user_agent, proxies=proxy)
    if r.status_code == 404:
        return None
    return r.text


def get_user_agents():
    with open('user_agents.txt', 'r') as f:
        result = f.read().splitlines()
    return result


def get_ready_html(url, wait_for: tuple, use_proxy=True):
    if use_proxy:
        capabilities = DesiredCapabilities.PHANTOMJS

        # prox = Proxy()
        # prox.proxy_type = ProxyType.MANUAL
        # prox.http_proxy = random.choice(proxies)
        # prox.add_to_capabilities(capabilities)
        #
        capabilities["phantomjs.page.settings.user_agent"] = (random.choice(user_agents))

        driver = webdriver.PhantomJS('webdrivers/phantomjs', desired_capabilities=capabilities)
    else:
        driver = webdriver.PhantomJS('webdrivers/phantomjs')
    driver.get(url)
    try:
        element = WebDriverWait(driver, 20).until(EC.presence_of_element_located(wait_for))
        result = driver.page_source
    except:
        print('Too long: %s' % url)
        result = None
    finally:
        driver.quit()
    return result


def get_house_link(address):
    url = '%s/search/houses?query=%s' % (BASE_URL, urllib.parse.quote_plus(address))
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    tbl = soup.find('div', class_='grid')
    if tbl is None:
        return '', ''
    links = tbl.find('table').find_all('a')
    result = [('%s%s' % (BASE_URL, link.get('href')), link.text) for link in links]
    return result[0]


def save_districts():
    with open('myhouse.html', 'r') as f:
        page = f.read()
    soup = BeautifulSoup(page, 'lxml')
    cols = soup.find_all('div', class_='col2')
    districts = []
    for col in cols:
        districts.extend(col.find_all('a'))
    districts = [{'name': link.text, 'url': '%s/%s' % (BASE_URL, link.get('href'))} for link in districts]
    districts = [link_dict for link_dict in districts if link_dict['url'].endswith('item=tp')]
    with open('districts.json', 'w') as f:
        json.dump(districts, f, ensure_ascii=False)


def handle_district(district):
    district_id = district['url'].split('tid=')[1].split('&')[0]
    json_file = 'houses/%s.json' % district_id
    if os.path.isfile(json_file):
        return
    page_id = 0
    houses = []
    while True:
        page_id += 1
        url = '%s&page=%d&limit=100' % (district['url'], page_id)
        print(url)
        page = get_ready_html(url, (By.CLASS_NAME, 'grid'), False)
        if page is None:
            return
        soup = BeautifulSoup(page, 'lxml')
        links = soup.find('div', class_='grid').find('tbody').find_all('a')
        if len(links) == 0:
            break
        links = [{'name': link.text, 'url': '%s%s' % (BASE_URL, link.get('href'))} for link in links]
        houses.extend(links)
    with open(json_file, 'w') as f:
        json.dump(houses, f, ensure_ascii=False)


def merge_house_links():
    json_names = os.listdir('houses')
    result = []
    for json_name in json_names:
        with open('houses/%s' % json_name, 'r') as f:
            result.extend(json.load(f))
    with open('houses.json', 'w') as f:
        json.dump(result, f, ensure_ascii=False)


def save_houses():
    with open('districts.json', 'r') as f:
        districts = json.load(f)
    for i, district in enumerate(districts):
        print('[%d/%d] %s' % (i, len(districts), district['name']))
        handle_district(district)


def get_ip():
    print('get_ip')
    print('New Proxy & User-Agent:')

    url = 'http://sitespy.ru/my-ip'
    user_agent = {'User-Agent': random.choice(user_agents)}
    proxy = {'http': 'http://' + random.choice(proxies)}
    page = get_html(url, user_agent, proxy)

    soup = BeautifulSoup(page, 'lxml')
    ip = soup.find('span', class_='ip').text.strip()
    ua = soup.find('span', class_='ip').find_next_sibling('span').text.strip()
    print(ip)
    print(ua)
    print('---------------------------')


def get_proxies(limit=10):
    result = []

    async def show(proxies):
        while True:
            proxy = await proxies.get()
            if proxy is None:
                break
            print('[%d/%d] Found proxy: %s' % (len(result) + 1, limit, proxy))
            result.append('%s:%s' % (proxy.host, proxy.port))

    proxies = asyncio.Queue()
    broker = Broker(proxies)
    tasks = asyncio.gather(
        broker.find(types=['HTTP', 'HTTPS'], limit=limit),
        show(proxies))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    return result


def save_district_houses_info(json_file, user_agent, proxy):
    with open(json_file, 'r') as f:
        houses = json.load(f)
    for i, house in enumerate(houses):
        print('[%d/%d] %s (%s)' % (i + 1, len(houses), house['name'], house['url']))
        page = get_html(house['url'], user_agent, proxy)


def dd():
    processes = []

    json_names = os.listdir('houses')
    for i, json_name in enumerate(json_names):
        args = ('houses/%s' % json_name, user_agents[i % len(user_agents)], proxies[i % len(proxies)],)
        p = Process(target=save_district_houses_info, args=args)
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print('yolo')


def get_house_area(page):
    soup = BeautifulSoup(page, 'lxml')
    tbl = soup.find('table', class_='col_list_group').find('table', class_='col_list')
    key_trs = tbl.find_all('tr', class_='left')
    value_trs = tbl.find_all('tr', class_=None)
    for key_tr, value_tr in zip(key_trs, value_trs):
        if 'Общая площадь дома, кв.м' in key_tr.text:
            try:
                result = float(value_tr.find('span').text.strip().replace(' ', ''))
                return result
            except:
                return None
    return None


def get_house_management(page):
    translator = {
        'Газоснабжение': 'gaz',
        'Отопление': 'heat',
        'Электроснабжение': 'power',
        'Водоотведение': 'drainage',
        'Горячее водоснабжение': 'water_hot',
        'Холодное водоснабжение': 'water_cold'
    }
    result = {
        'gaz': None,
        'heat': None,
        'power': None,
        'drainage': None,
        'water_hot': None,
        'water_cold': None
    }

    soup = BeautifulSoup(page, 'lxml')
    ul_subtabs = soup.find('ul', class_='subtab_labels')
    if ul_subtabs is None:
        print('Страница не загружена')
        return result
    trs = soup.find('div', id='tab1-subtab3').find_all('tr', class_='middle')
    headers = [th.text for th in trs[0].find_all('th')]
    trs = trs[1:]
    props = []
    for tr in trs:
        tds = tr.find_all('td')
        if len(tds) != len(headers):
            continue
        try:
            key = translator[tds[0].text.strip()]
            value = float(tds[2].text.strip().replace(' ', ''))
            result[key] = value
        except:
            continue

    return result


def get_house_heat_total(house_id):
    url = 'https://www.reformagkh.ru/myhouse/profile/finance/%d/' % house_id
    user_agent = {'User-Agent': random.choice(user_agents)}
    proxy = {'http': 'http://' + random.choice(proxies)}
    page = get_html(url, user_agent, proxy)
    while not solved_captcha(page):
        page = get_html(url, user_agent, proxy)
    if solved_captcha(page):
        page = get_html(url, user_agent, proxy)
    if page is None:
        return None
    soup = BeautifulSoup(page, 'lxml')
    print(page)

    result = None

    return result


def get_house_performed_work(page):
    soup = BeautifulSoup(page, 'lxml')
    ul_subtabs = soup.find('ul', class_='subtab_labels')
    if ul_subtabs is None:
        # print('%s: страница не загружена' % url)
        return None
    trs = soup.find('div', id='tab1-subtab2').find_all('tr', class_='middle')[1:]
    result = 0.0
    for tr in trs:
        tds = tr.find_all('td')
        try:
            value = float(tds[1].text.strip().split(' ')[0].replace(' ', ''))
            result += value
        except:
            continue
    return result


def get_captcha_answer(captcha_url):
    captcha_name = 'captcha.jpg'

    img_data = requests.get(captcha_url).content
    with open(captcha_name, 'wb') as handler:
        handler.write(img_data)

    gate = AntiGate('90f2971a20beee5a508bad796a8790fa')
    captcha_id = gate.send(captcha_name)
    print('Расшифровываем капчу')
    result = gate.get(captcha_id)
    print('Расшифровали капчу: %s' % result)
    return result


def solved_captcha(page):
    soup = BeautifulSoup(page, 'lxml')
    if soup.find('form', action='/captcha-form') is None:
        return True
    browser = RoboBrowser()
    browser.open(BASE_URL)
    form = browser.get_form(action='/captcha-form')
    captcha_url = '%s%s' % (BASE_URL, browser.find('img').get('src'))
    answer = get_captcha_answer(captcha_url)
    form['captcha[input]'].value = answer
    browser.submit_form(form)
    return False


def test():
    json_names = os.listdir('houses')
    json_props_names = os.listdir('houses_props')
    for json_name in json_names:
        if json_name in json_props_names:
            continue
        with open('houses/%s' % json_name, 'r') as f:
            houses = json.load(f)
        for i in range(len(houses)):
            url = 'https://www.reformagkh.ru/myhouse/profile/management/%d/' % houses[i]['id']
            print('[%d/%d] %s (%s)' % (i + 1, len(houses), houses[i]['name'], url))
            user_agent = {'User-Agent': random.choice(user_agents)}
            proxy = {'http': 'http://' + random.choice(proxies)}
            page = get_html(url, user_agent, proxy)
            while not solved_captcha(page):
                page = get_html(url, user_agent, proxy)

            houses[i]['props'] = {}
            houses[i]['props']['area'] = get_house_area(page)
            houses[i]['props'].update(get_house_management(page))
            houses[i]['props']['performed_work'] = get_house_performed_work(page)
            # houses[i]['props']['heat_total'] = get_house_heat_total(houses[i]['id'])
            pass
        with open(json_name.replace('houses/', 'houses_props/'), 'w') as f:
            json.dump(houses, f, ensure_ascii=False)
            # [print(tr) for tr in trs]


proxies = get_proxies(50)
user_agents = get_user_agents()
