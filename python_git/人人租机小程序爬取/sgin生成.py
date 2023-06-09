import hashlib

r='catephonecity全国mini_version1.13.33page1shortallterminalwx.mini'
sign=hashlib.md5((r+'L8aU4erNfY2xbIHy').encode()).hexdigest()
print(sign)