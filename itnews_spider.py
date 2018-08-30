#! /usr/local/python3/bin/python3.6
#coding=utf-8

'''
抓取IT新闻（博客园）基本内容
爬虫线路： requests - bs4
Python版本： 3.6
OS： ubuntu 16.04
'''

import sys, os, threading, multiprocessing
import requests
import re, time, random
from bs4 import BeautifulSoup
import useragent_pool, ip_pool

class Spider():
    def __init__(self, url):
        self.url = url
        self.info_url = url + '/NewsAjax/GetAjaxNewsInfo?contentId=' #获取评论数，浏览数

    #请求页面信息
    def get_html_text(self, url):
        try:
            header = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "Referer": "http://zkeeer.space/",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            header['User-Agent'] = random.choice(useragent_pool.user_agents)
            proxy = random.choice(ip_pool.ip_pool)
            r = requests.get(url = url, proxies = proxy, headers = header, timeout=300)
            r.raise_for_status()
            r.encoding = 'utf-8'
            return r.text
        except:
            raise BaseException
#            print('get html error!')

    #抓取单个新闻信息
    def get_news(self, url):
        info = {}
        soup = BeautifulSoup(self.get_html_text(url), 'lxml')
        news_title = soup.find('div', attrs={'id': 'news_title'})
        info['news_info_url'] = self.info_url + re.findall('[0-9]+', url)[0]
        news_infos = self.get_html_text(info['news_info_url'])
        info['link'] = 'https:' + news_title.find('a')['href']
        info['title'] = news_title.text.strip()
        news_info = soup.find('div', attrs={'id': 'news_info'})
        info['posttime'] = re.findall('[^0-9]([0-9-]+\s[0-9:]+)$', news_info.find('span', class_='time').text.strip())[0]
        info['view'] = re.findall('"TotalView":[^0-9]*([0-9]+)', news_infos)[0]
        info['comment'] = re.findall('"CommentCount":[^0-9]*([0-9]+)', news_infos)[0]
        try:
            info['tag'] = soup.find('div', class_='news_tags').text.strip()
        except:
            info['tag'] = '--'
        return info

    #抓取一页新闻信息
    def get_content(self, url):
        infos = []
        soup = BeautifulSoup(self.get_html_text(url), 'lxml')
        news_tags = soup.find_all('div', class_='news_block')
        for news in news_tags:
            info = {}
            news_entry = news.find('h2', class_='news_entry')
            info['title'] = news_entry.find('a').text.strip()
            info['link'] = 'https://news.cnblogs.com' + news_entry.find('a')['href']
            info['posttime'] = news.find_all('span', class_='gray')[-1].text.strip()
            info['view'] = news.find('span', class_='view').text.strip()
            info['comment'] = news.find('span', class_='comment').text.strip()
            #info['content'] = self.get_news_content(info['link'])
            try:
                info['tag'] = news.find('span', class_='tag').text.strip()
            except:
                info['tag'] = '--'
            infos.append(info)
        return infos

    #抓取新闻内容
    def get_news_content(self, url):
        news_content_soup = BeautifulSoup(self.get_html_text(url), 'lxml')
        news_content = news_content_soup.find('div', attrs={'id': 'news_body'}).text
        return news_content

    #计算字符串中中文个数, 使格式化打印对齐
    def count_ch_length(self, string):
        length = 0
        for ch in string:
            if u'\u2999' < ch < u'\uffcf':
                length += 1
        return length

    #把新闻信息保存到文件
    def write_to_file(self, output, infos):
        with open(output, 'a+') as f:
            f.write('#grab time: %s\n\n'%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            for info in infos:
                f.write('title: {:-<{title_length}} link: {} \ntag: {:<{tag_length}} posttime: {:<20} view: {:<10} comment: {}\n'.format(info['title'], info['link'], info['tag'], info['posttime'], info['view'], info['comment'], title_length=70-self.count_ch_length(info['title']), tag_length=50-self.count_ch_length(info['tag'])))

    #多进程执行
    def multi_process_execute(self, output):
        p = multiprocessing.Pool()
        for i in range(4):
            p.apply_async(self.multi_threading_execute, args=(output, 25*i,))
        p.close()
        p.join()
        print('All processes done.')
        print('Successful.')

    #多线程执行
    def multi_threading_execute(self, output, page_offset):
        threads = [threading.Thread(target=self.main, args=(page_offset+x*5+5, output, page_offset+x*5)) for x in range(5)]
        for i in range(5):
            threads[i].start()
        for i in range(5):
            threads[i].join()

    #抓取n页新闻信息并打印
    def main(self, end_page, output, start_page=0):
        try:
            url_lists = []
            for i in range(start_page, end_page):
                url_lists.append(self.url + '/n/page/' + str(i+1))
            for url in url_lists:
                infos = self.get_content(url)
                page = re.findall('/([0-9]+)', url)[-1]
                print('page %s done.'%page)
                self.write_to_file(output+'_page%s'%page, infos)
        except:
            print('main error.')

    #合并生成的文件
    def merge_files(self, output):
        for i in range(1, 101):
            try:
                with open(output+'_page%d'%i, 'r') as source_file:
                    content = source_file.read()
                os.remove(output+'_page%d'%i)
                with open(output, 'a+') as dest_file:
                    dest_file.write(content)
            except:
                print('page%d read error.' % i)
        print('*****merge done.*****')

base_url = 'https://news.cnblogs.com' #博客园IT新闻
news_info_url = 'https://news.cnblogs.com/NewsAjax/GetAjaxNewsInfo?contentId=' #评论数，浏览数等信息

if __name__=='__main__':
    time_start = time.time()
    output = sys.argv[1]
    s = Spider(base_url)
    s.multi_process_execute(output)
    s.merge_files(output)
    time_end = time.time()
    print('time consuming: %.2fs.' % (time_end-time_start))
