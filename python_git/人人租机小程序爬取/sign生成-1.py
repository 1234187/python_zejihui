import hashlib
from urllib.parse import urlencode, quote

def queryToArray(query):
    result = []
    for key, value in query.items():
        if isinstance(value, (list, tuple)):
            for v in value:
                result.append({
                    'key': key,
                    'value': v
                })
        else:
            result.append({
                'key': key,
                'value': value
            })
    return result

def queryToStr(query, method='GET'):
    if method == 'POST':
        return urlencode(query, '')
    else:
        return '&'.join(['{}={}'.format(k, v) for k, v in query.items()])

def queryToStr_1(query, method='GET'):
    if method == 'POST':
        return urlencode(query, '')
    else:
        return ''.join(['{}{}'.format(k, v) for k, v in query.items()])



# def queryToStr(query,sep='&', eq='='):
#     query_str = []
#     for item in query:
#         key = item['key']
#         print(key)
#         value = item['value']
#         print(value)
#         type = item.get('type', 'GET')
#         if type == 'POST':
#             query_str.append(f'{key}{eq}{value}')
#         else:
#             query_str.append(f'{key}{eq}{urllib.parse.urlencode(str(value))}')
#     return sep.join(query_str)


def sign(t, n, o={}, c={}):
    if not t:
        return ''
    s = {**o, **c}
    u = queryToArray(s) or []
    if len(u) == 0:
        return t
    u.sort(key=lambda x: x['key'])
    print(u)
    u_dict = {i['key']: i['value'] for i in u}
    print(queryToStr(u_dict))
    sign_str = quote(urlencode(u_dict, '', ''), safe='~()*!.=&\'')
    print(sign_str)
    return '{}?{}&sign={}'.format(t, queryToStr(c if n == 'POST' else s, n), hashlib.md5('{}{}'.format(queryToStr_1(u_dict), 'L8aU4erNfY2xbIHy').encode()).hexdigest())


if __name__ == '__main__':
    url = "https://api.rrzuji.com/individual/search/product"
    method = "GET"
    public_params = {
        "page": 2,
        "short": "all",
        "city": "全国",
        "cate": "phone",
        "mini_version": "1.13.33",
        "terminal": "wx.mini",
    }
    sign = sign(url, method, public_params)
    print(sign)
    