import hashlib
from urllib.parse import urlencode, quote

def sign(t, n, o=None, c=None):
    if not t:
        return ""
    s = {} if o is None else o.copy()
    if c is not None:
        s.update(c)
    u = []
    for key, value in s.items():
        u.append({'key': key, 'value': value})
    if len(u) == 0:
        return t
    u.sort(key=lambda x: x['key'])
    u_dict={i['key']:i['value'] for i in u}
    sign_str = quote(urlencode(u_dict, '', ''), safe='~()*!.\'')
    return f"{t}?{queryToStr('POST' if n == 'POST' else 'GET', c)}&sign={hashlib.md5((sign_str + 'L8aU4erNfY2xbIHy').encode()).hexdigest()}"

def queryToArray(s):
    result = []
    for key, value in s.items():
        result.append({'key': key, 'value': value})
    return result

def queryToStr(method, params=None):
    if params is None:
        params = {}
    p = []
    for key, value in params.items():
        p.append({'key': key, 'value': value})
    p.sort(key=lambda x: x['key'])
    p_str = urlencode(p, '', '')
    if method == 'GET':
        return p_str
    return p_str

if __name__ == '__main__':
    url = "https://api.rrzuji.com/individual/search/product"
    method = "GET"
    public_params = {
        "page": 1,
        "short": "all",
        "city": "全国",
        "cate": "phone",
        "mini_version": "1.13.33",
        "terminal": "wx.mini",
    }
    sign = sign(url, method, public_params)
    print(sign)
