import pandas as pd
import pymysql

class qfsql:
    def data_fetcher_ji(self,stmt,chunksize=''):
        try:
            con = pymysql.connect(host='zubei88.mysql.polardb.rds.aliyuncs.com',
                                  port=3306,
                                  user='psc_read',
                                  password='1234qweR',
                                  database='ji_ke_zu', charset='utf8')
            if chunksize!='':
                render = pd.read_sql(stmt, con,chunksize=chunksize)
                data=pd.concat(render,axis=0,ignore_index=True)
            else:
                data = pd.read_sql(stmt, con)
            con.close()
            return data
        except Exception as e:
            print('Error: {}'.format(e))

