import requests
import time
import csv
import pandas as pd
import numpy as np
from lxml import etree
import concurrent.futures
import random

class Reptile:
    def __init__(self, city_code):
        self.city_code = city_code.upper()
        self.url = f"https://flights.ctrip.com/schedule/{self.city_code}..html"
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
        }
        self.session = requests.Session()  # 复用Session

    def city_flight_list(self):
        try:
            page_text = self.session.get(url=self.url, headers=self.headers, timeout=10).text
            tree = etree.HTML(page_text)
            name = tree.xpath("//div[@class='m']/a/text()")
            link = tree.xpath("//div[@class='m']/a/@href")
            print(f"共获取到 {len(name)} 个机场往返记录")
        except Exception as e:
            print(f"获取城市列表失败: {e}")
        return name, link

    def specific_flight(self, flight_name, flight_link):
        def parse_city_code(link):
            return link.split('.')[1]

        def process_schedule(flight):
            WEEK_MAP = {'1':'周一', '2':'周二', '3':'周三', '4':'周四', '5':'周五', '6':'周六', '7':'周日'}
            schedule = flight["currentWeekSchedule"]
            active_days = [WEEK_MAP[day] for day, active in schedule.items() if active]
            return ", ".join(active_days)

        api_url = "https://flights.ctrip.com/schedule/getScheduleByCityPair"
        dep_city_code = self.city_code
        data = []

        def process_link(link, name):
            """ 处理单个航班链接 """
            time.sleep(random.uniform(0.2, 0.8))  # 随机延迟降低封禁风险
            arr_city_code = parse_city_code(link)
            try:
                payload = {"arriveCityCode": arr_city_code, "departureCityCode": dep_city_code}
                response = self.session.post(api_url, json=payload, headers=self.headers, timeout=10).json()
                rows = []
                for flight in response.get('scheduleVOList', []):
                    date = process_schedule(flight)
                    row = {
                        '出发-到达': name,
                        '航班号': flight['flightNo'],
                        '飞机型号': flight['aircraftType'],
                        '航班日期': date,
                        '起飞时间': flight['departTime'],
                        '到达时间': flight['arriveTime'],
                        '起飞机场': flight['departPortName'],
                        '落地机场': flight['arrivePortName'],
                        '航空公司': flight['airlineCompanyName']
                    }
                    rows.append(row)
                print(f"{name} 处理完成，共 {len(rows)} 条记录")
                return rows
            except Exception as e:
                print(f"处理 {name} 时出错: {e}")
                return []

        # 使用线程池并发处理每个航班链接
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:  # 控制并发数
            futures = [executor.submit(process_link, link, name) for link, name in zip(flight_link, flight_name)]
            for future in concurrent.futures.as_completed(futures):
                data.extend(future.result())

        return pd.DataFrame(data)

def process_city(city_code):
    """ 处理单个城市并返回数据 """
    print(f"开始处理城市: {city_code}")
    reptile = Reptile(city_code)
    try:
        names, links = reptile.city_flight_list()
        if names and links:
            return reptile.specific_flight(names, links)
    except Exception as e:
        print(f"城市 {city_code} 处理异常: {e}")
    return pd.DataFrame()

if __name__ == "__main__":
    city_codes = {'bjs', 'sha', 'can', 'ctu', 'urc', 'hrb'}
    all_data = pd.DataFrame()

    start=time.perf_counter()
    # 并发处理各城市
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:  # 控制城市并发数
        future_to_city = {executor.submit(process_city, code): code for code in city_codes}
        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                city_df = future.result()
                all_data = pd.concat([all_data, city_df], ignore_index=True)
            except Exception as e:
                print(f"城市 {city} 数据合并失败: {e}")

    end=time.perf_counter
    Stime=end-start

    print(Stime)
    all_data.to_csv("optimized_data.csv", index=False)
   