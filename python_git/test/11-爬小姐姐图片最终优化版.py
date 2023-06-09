import os
import queue
import re
import threading
import time

import requests
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
}

# 写真集队列
page_ul = queue.Queue()
# 图片队列
img_url = queue.Queue()


# 获得图片的方法
def get_picture():
    while True:
        # 判断队列是否为空，如果为空就返回整个方法，如果不为空则进入写真集获取图片地址
        if page_ul.empty():
            return
        url, i = page_ul.get()
        try:
            response = requests.get(url, headers=headers)
            html = response.text
            # 解析网页
            dir_name = re.findall('<h1 class="post-title h1">(.*?)</h1>', html)[-1]
            # 判断目录是否存在，如果不存在则创建
            if not os.path.exists(f'./小姐姐图片/第{i}页/{dir_name}'):
                os.makedirs(f'./小姐姐图片/第{i}页/{dir_name}')
            urls = re.findall('<a href="(.*?)" alt=".*?" title=".*?">', html)
            if len(urls) == 0:
                soup = BeautifulSoup(html, 'lxml')
                div = soup.select('div[class="nc-light-gallery"]')
                img_url = div[0].find_all('img')
                urls = [obj.get('src') for obj in img_url]
                get_picture_href(urls, dir_name, i)
            else:
                get_picture_href(urls, dir_name, i)
        except Exception as e:
            print(e, type(e))


# 获取图片连接：
def get_picture_href(urls, dir_name, i):
    # 保存图片
    for url in urls:
        time.sleep(0.5)
        if url.startswith('//static'):
            url1 = 'https:' + url
            # 将图片连接放入到队列中
            img_url.put((url1, dir_name, i))
        elif url.startswith('/image'):
            url1 = 'https://static.vmgirls.com' + url
            img_url.put((url1, dir_name, i))


# 下载图片
def download_picture():
    while True:
        if img_url.empty() and page_ul.empty():
            return
        # 图片的名字
        url1, dir_name, i = img_url.get()
        file_name = url1.split('/')[-1]
        try:
            response = requests.get(url1, headers=headers)
            with open(f'./小姐姐图片/第{i}页/{dir_name}' + '/' + file_name, 'wb') as f:
                f.write(response.content)
                print(f'第{i}页-{file_name}下载完成。。。。')
        except Exception as e:
            print(e, type(e))


# 获取写真集的连接
def get_hrefs(i):
    data = {
        'append': 'list-home',
        'paged': i,
        'action': 'ajax_load_posts',
        'query': '',
        'page': 'home'
    }
    url = 'https://www.vmgirls.com/wp-admin/admin-ajax.php'
    response = requests.post(url=url, headers=headers, data=data)
    response.encoding = 'utf-8'
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    divs = soup.select('div[ class= "col-6 col-md-3 d-flex py-2 py-md-3"]')
    for div in divs:
        a = div.select('a[class="media-content"]')
        hrefs = [obj.get('href') for obj in a]
        # 将写真集放到队列中
        page_ul.put((hrefs[0], i))


if __name__ == '__main__':
    join_list = []
    page_start = int(input('请输入想要从那页下载：'))
    page_during = int(input('请输入想要下载的页数：'))
    for i in range(page_start, (page_start + page_during)):
        get_hrefs(i)
    # 创建30个线程同时爬取
    for h in range(30):
        # 创建线程
        t1 = threading.Thread(target=get_picture)
        t2 = threading.Thread(target=download_picture)
        # 启动线程
        t1.start()
        t2.start()
        join_list.append(t1)
        join_list.append(t2)

    for j in join_list:
        j.join()
    input('程序执行成功，按任意键退出：')





