import requests
from lxml import etree
import os

def request_data(url):
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
    }
    response = requests.get(url=url,headers=headers)
    # with open('tmall.html','w',encoding='utf-8') as fp:
    #     fp.write(response.text)
    return response.text

def parse_date(tmall):
    e = etree.HTML(tmall)
    image = []
    img = e.xpath('//div[@class="tb-gallery"]/ul/li/div/a/img')
    for i in img:
        image.append(e.xpath('/@src'))
    return image

#存储图片
def save_img(items):
    # save_dir = 'C:/Users/Lenovo/Desktop/图片转换'
    # if not os.path.exists(save_dir):
    #     os.mkdir(save_dir)
    print(items)
    image_content = requests.get(items).content
    # file_path = os.path.join(save_dir, '图片转换')
    # with open(file_path ,'wb') as f:
    #     f.write(image_content)

if __name__ == '__main__':
    tmall = request_data(url = 'https://detail.tmall.com/item.htm?ali_refid=a3_430585_1006:1123105682:N:Ta48tArib4vJBuOUkbUOVMjrKlkz/J/Q:e6a75858241a9a9fe0556e049eb60afb&ali_trackid=1_e6a75858241a9a9fe0556e049eb60afb&id=563510207800&sku_properties=147956252:48712&spm=a230r.1.14.1')
    print(tmall)
    image = parse_date(tmall)
    save_img(image)
    print('保存成功')