#! /usr/local/python3/bin/python3.6
#coding=utf-8

'''
抓取IT新闻（博客园）基本内容存储到数据库中
数据库版本：mysql  Ver 14.14 Distrib 5.7.23
爬虫线路： requests - bs4
Python版本： 3.6
OS： ubuntu 16.04
'''

import sys, os, threading, multiprocessing
import MySQLdb
import re, time, random
import itnews_spider_multi

class Storage(itnews_spider_multi.Spider):
    #多进程执行
    def multi_process_execute(self, start, end):
        offset = (end - start) // 4
        single_num = offset // 5
        p = multiprocessing.Pool()
        for i in range(4):
            p.apply_async(self.multi_threading_execute, args=(start, offset*i, single_num,))
        p.close()
        p.join()
        print('All processes done.')
        print('Successful.')

    #多线程执行
    def multi_threading_execute(self, start, offset, single_num):
        threads = [threading.Thread(target=self.insert_datas1, args=(start+offset+x*single_num, start+offset+(x+1)*single_num)) for x in range(5)]
        for i in range(5):
            threads[i].start()
        for i in range(5):
            threads[i].join()

    #爬取n页新闻信息，存入mysql （只能抓取100页）
    def insert_datas(self, start_page, end_page):
        db = MySQLdb.connect("localhost", "test", "123456", "spider_datas", charset='utf8')
        cursor = db.cursor()
        url_lists = []
        for i in range(start_page, end_page):
            url_lists.append(self.url + '/n/page/' + str(i+1))
        for url in url_lists:
            infos = self.get_content(url)
            for info in infos:
                info['view'] = re.findall('[0-9]+', info['view'])[0]
                info['comment'] = re.findall('[0-9]+', info['comment'])[0]
                try:
                    mysql_insert = "insert into itnews(title, link, posttime, tag, view, comment) values('%s', '%s', '%s', '%s', '%s', '%s')" % (info['title'], info['link'], info['posttime'], info['tag'], info['view'], info['comment'])
                    cursor.execute(mysql_insert)
                except:
                    print(info['title'])
        db.commit()
        db.close()

    #爬取n条新闻信息，存入mysql
    def insert_datas1(self, start_no, end_no):
        db = MySQLdb.connect("localhost", "test", "123456", "spider_datas", charset='utf8')
        cursor = db.cursor()
        url_lists = []
        for i in range(start_no, end_no):
            url_lists.append(self.url + '/n/' + str(i))
        for url in url_lists:
            try:
                info = self.get_news(url)
                cursor.execute("insert into itnews_test(title, link, posttime, tag, view, comment) values('%s', '%s', '%s', '%s', '%s', '%s')" % (info['title'], info['link'], info['posttime'], info['tag'], info['view'], info['comment']))
            except:
                pass
        db.commit()
        db.close()

    #打印指定范围的新闻的信息
    def print_urls(self, start_no, end_no):
        url_lists = []
        for i in range(start_no, end_no):
            url_lists.append(self.url + '/n/' + str(i))
        for url in url_lists:
            try:
                info = self.get_news(url)
                print(info['link'], info['title'], info['view'], info['comment'])
            except:
                pass

if __name__ == '__main__':
    url = itnews_spider_multi.base_url
    #表不存在时，创建表
#    db = MySQLdb.connect("localhost", "root", "123456", "spider_datas")
#    cursor = db.cursor()
##    cursor.execute("create table if not exists `itnews`(`id` int not null auto_increment primary key, `title` char(64), `link` char(64), `posttime` datetime, `tag` char(32), `view` int, `comment` int) auto_increment=1")
#    cursor.execute("truncate table itnews_test")
#    db.commit()
#    db.close()
    time_start = time.time()
    st = Storage(url)
    #st.multi_process_execute(100000, 150000)
    st.multi_process_execute(600000, 605720)
    #st.multi_process_execute(500000, 550000)

    #st.print_urls(60900,60910)

    time_end = time.time()
    print("time consuming: %2.fs" %(time_end-time_start))
