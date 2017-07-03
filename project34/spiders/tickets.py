# -*- coding: utf-8 -*-
import time
import datetime
import json
import urllib

import pymysql.cursors

import scrapy
from scrapy.http.request import Request
from project34.items import BriefDeltaItem
from project34.items import CodeItem
from project34.items import TicketItem
from project34.items import CommitItem

class TicketsSpider(scrapy.Spider):
    name = 'TicketsSpider'

    custom_settings = {
        'ITEM_PIPELINES': {
            'project34.pipelines.TicketSQLPipeline': 300,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'project34.middlewares.DownloaderMiddleware': 500,
        },
        'DUPEFILTER_CLASS': "project34.filter.URLTurnFilter",
        'JOBDIR': "s/tickets",
    }

    def __init__(self, *a, **kw):
        super(TicketsSpider, self).__init__(self.name, **kw)
        self.turn = a[0]
        self.logger.info("%s. this turn %d" % (self.name, self.turn))

    def start_requests(self):
        yield Request("https://kyfw.12306.cn/otn/resources/js/framework/station_name.js?station_version=1.8936", callback = self.parse, meta = {"turn":self.turn})

    @staticmethod
    def fetch_routes():
        conn = pymysql.connect(host = 'localhost',
                                    port = 3306,
                                    user = '12306',
                                    passwd = '12306',
                                    db = '12306-train',
                                    charset = 'utf8')


        select = "select * from train_infos"

        schedules = {}
        with conn.cursor() as cursor:
            cursor.execute(select)
            count = 0
            for results in cursor.fetchall():
                if results[0] not in schedules:
                    schedules[results[0]] = {results[1]:results[2]}
                else:
                    schedules[results[0]][results[1]] = results[2]


        routes = {}
        for key in schedules:
            route = schedules[key]

            seq = sorted(route)
            len1 = len(seq)
            for i in range(0, len1):
                if route[seq[i]] not in routes:
                    tmp = set()
                    routes[route[seq[i]]] = tmp
                else:
                    tmp = routes[route[seq[i]]]
                for j in range(i + 1, len1):
                    tmp.add(route[seq[j]])
        return routes


    def parse(self, response):
        station_str = response.body.decode("utf-8")
        stations = station_str.split(u"@")
        results = {}

        for i in range(1, len(stations)):
            station = stations[i].split(u"|")
            results[station[1]] = station[2]
            item = CodeItem()
            item["name"] = station[1]
            item["code"] = station[2]
            item["turn"] = response.meta["turn"]
            yield item

        yield CommitItem()

        routes = TicketsSpider.fetch_routes()

        url = "https://kyfw.12306.cn/otn/leftTicket/query"
        t = (datetime.datetime.now() + datetime.timedelta(days = 3)).strftime("%Y-%m-%d")
        for s in routes:
            if s in results:
                code_s = results[s]
            else:
                self.logger.warning("code miss " + s)
                continue
            for e in routes[s]:
                if e in results:
                    code_e = results[e]
                else:
                    self.logger.warning("code miss " + e)
                    continue

            params = u"leftTicketDTO.train_date=" + t + u"&leftTicketDTO.from_station=" + code_s + \
                     u"&leftTicketDTO.to_station=" + code_e + u"&purpose_codes=ADULT"

            yield Request(url + params, callback = self.parse_ticket, meta = {"s":s, "e":e, "turn":response.meta["turn"]}, verify=False)

    def parse_ticket(self, response):
        datas = json.loads(response.body)

        if "result" not in datas["data"]:
            self.logger.info("there is no data " + response.meta["s"] + " " + response.meta["e"])
            return

        for da in datas["data"]["result"]:
            data = {}
            data["from_station_name"] = da.split('|')[6]
            data["end_station_name"] = da.split('|')[7]
            data["station_train_code"] = da.split('|')[3]
            data["train_no"] = da.split('|')[2]
            data["seat_type"] = da.split('|')[-1]
            # 余票数量信息
            data["swz_num"] = da.split('|')[-3]
            data["tz_num"] = da.split('|')[-10]
            data["zy_num"] = da.split('|')[-4]
            data["ze_num"] = da.split('|')[-5]
            data["gr_num"] = da.split('|')[-13]
            data["rw_num"] = da.split('|')[-12]
            data["yw_num"] = da.split('|')[-7]
            data["rz_num"] = da.split('|')[-8]
            data["yz_num"] = da.split('|')[-6]
            data["wz_num"] = da.split('|')[-9]
            data["qt_num"] = da.split('|')[-14]

            out = u"--------------------------------------\n"
            out += data["from_station_name"]
            out += u" " + data["end_station_name"]
            s = out.encode("GBK")
            print s

            deltaItem = BriefDeltaItem()
            deltaItem["code"] = data["station_train_code"]
            deltaItem["seat_type"] = data["seat_types"]
            yield deltaItem

            item = TicketItem()
            item["train_no"] = data["train_no"]
            item["start"] = data["from_station_name"]
            item["end"] = data["end_station_name"]
            item["swz"] = data["swz_num"]
            if item["swz"] == '':
                item["swz"] = -1
            item["tz"] = data["tz_num"]
            if item["tz"] == '':
                item["tz"] = -1
            item["zy"] = data["zy_num"]
            if item["zy"] == '':
                item["zy"] = -1
            item["ze"] = data["ze_num"]
            if item["ze"] == '':
                item["ze"] = -1
            item["gr"] = data["gr_num"]
            if item["gr"] == '':
                item["gr"] = -1
            item["rw"] = data["rw_num"]
            if item["rw"] == '':
                item["rw"] = -1
            item["yw"] = data["yw_num"]
            if item["yw"] == '':
                item["yw"] = -1
            item["rz"] = data["rz_num"]
            if item["rz"] == '':
                item["rz"] = -1
            item["yz"] = data["yz_num"]
            if item["yz"] == '':
                item["yz"] = -1
            item["wz"] = data["wz_num"]
            if item["wz"] == '':
                item["wz"] = -1
            item["qt"] = data["qt_num"]
            if item["qt"] == '':
                item["qt"] = -1
            yield item
        yield CommitItem()
