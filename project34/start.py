#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import time
import datetime

import pymysql.cursors

project_path = os.path.dirname(os.path.abspath(__file__ + "/.."))
sys.path.insert(0, project_path)


# import the spiders you want to run
from spiders.agencys import AgencysSpider
from spiders.stations import StationsSpider
from spiders.trains import ScheduleSpider
from spiders.tickets import TicketsSpider

# scrapy api imports
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

settings = get_project_settings()

crawler = CrawlerProcess(settings)

def sleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d

@defer.inlineCallbacks
def crawl():

    conn = pymysql.connect(host='localhost', port=3306,
                           user='12306',
                           password='12306',
                           db='12306-train',
                           charset='utf8')

    agency_count = 30
    station_count = 30
    train_count = 5
    ticket_count = 1
    first = True

    last_turn = -1
    while True:
        n = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s = time.time()
        turn = int(s / 86400)

        if turn == last_turn:
            sleep(5)
            continue

        print "new turn", turn, n
        last_turn = turn

        with conn.cursor() as cursor:
            cursor.execute("INSERT IGNORE INTO `turns` VALUES (%s, %s)", (turn, n))
        conn.commit()

        if first or turn % agency_count == 0:
            yield crawler.crawl(AgencysSpider, turn)
        if first or turn % station_count == 0:
            yield crawler.crawl(StationsSpider, turn)
        if first or turn % train_count == 0:
            yield crawler.crawl(ScheduleSpider, turn)
        if first or turn % ticket_count == 0:
            yield crawler.crawl(TicketsSpider, turn)

        first = False
        e = time.time()
        left = int(86400 - e + s)
        if left > 0:
            print "sleep", left
            sleep(left)
    print "crawler over"

crawl()
crawler.start()
