# -*- coding: utf-8 -*-
import time
import datetime
import json
import urllib

import scrapy
from scrapy.http.request import Request
from project34.items import BriefItem
from project34.items import InfoItem
from project34.items import TurnItem
from project34.items import CommitItem

class ScheduleSpider(scrapy.Spider):
    name = 'ScheduleSpider'
    #start_urls = ['https://kyfw.12306.cn/otn/queryTrainInfo/getTrainName']

    custom_settings = {
        'ITEM_PIPELINES': {
            'project34.pipelines.TrainSQLPipeline': 300,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'project34.middlewares.DownloaderMiddleware': 500,
        },
        'DUPEFILTER_CLASS': "project34.filter.URLTurnFilter",
        'JOBDIR': "s/schedule",
    }

    def __init__(self, *a, **kw):
        super(ScheduleSpider, self).__init__(self.name, **kw)
        self.turn = a[0]
        self.logger.info("%s. this turn %d" % (self.name, self.turn))

    def start_requests(self):

        url = "https://kyfw.12306.cn/otn/queryTrainInfo/getTrainName?"

        n = datetime.datetime.now()
        t = (n + datetime.timedelta(days = 3)).strftime("%Y-%m-%d")
        params = {"date":t}

        s_url = url + urllib.urlencode(params)
        yield Request(s_url, callback = self.parse, meta = {"t":t, "turn":self.turn})

    def parse(self, response):
        datas = json.loads(response.body)
        url = "https://kyfw.12306.cn/otn/czxx/queryByTrainNo?"
        for data in datas["data"]:
            item = BriefItem()
            briefs = data["station_train_code"].split("(")
            item["train_no"] = data["train_no"]
            item["code"] = briefs[0]
            briefs = briefs[1].split("-")
            item["start"] = briefs[0]
            item["end"] = briefs[1][:-1]
            item["turn"] = response.meta["turn"]
            yield item

            params = u"train_no=" + data["train_no"] + \
                     u"&from_station_telecode=BBB&to_station_telecode=BBB&depart_date=" + response.meta["t"]

            yield Request(url + params, callback = self.parse_train_schedule,
                                            meta = {"train_no":data["train_no"],
                                            "turn":response.meta["turn"]}, verify=False)

    def parse_train_schedule(self, response):
        stations = json.loads(response.body)

        datas = stations["data"]["data"]
        size = len(datas)
        for i in range(0, size):
            data = datas[i]

            info = InfoItem()
            info["train_no"] = response.meta["train_no"];
            info["no"] = int(data["station_no"])
            info["station"] = data["station_name"]
            info["turn"] = response.meta["turn"]

            if data["start_time"] != u"----":
                info["start_time"] = data["start_time"] + u":00";
            else:
                info["start_time"] = None

            if data["arrive_time"] != u"----":
                info["arrive_time"] = data["arrive_time"] + u":00";
            else:
                info["arrive_time"] = None

            stop = data["stopover_time"]
            if stop != u"----":
                if stop.endswith(u"分钟"):
                    info["stopover_time"] = u"00:" + stop[:stop.find(u"分钟")] + u":00";
                else:
                    info["stopover_time"] = stop + u":00";
            else:
                info["stopover_time"] = None

            yield info
        yield CommitItem()