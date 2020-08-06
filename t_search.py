import pandas as pd
import numpy as np
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.types import CHAR,INT
import datetime


def time():

    db_data = 'mysql+pymysql://root:123456@localhost:3306/twitter_1?charset=utf8mb4'
    engine = create_engine(db_data)

    sql="SELECT * FROM twitter_1 ORDER BY create_time DESC LIMIT 10";  #以降冪查詢前10筆日期

    df_select = pd.read_sql(sql=sql,con=engine)

    s_select=df_select['create_time']
    search_time=[]
    for i in s_select:
        search_time.append(i)
    return search_time


    ## 另一連線方式
    # db_data = pymysql.connect(host="localhost",
    #                           user="root",
    #                           password="123456",
    #                           db="twitter_1" )

    # cursor = db_data.cursor()  #獲得操作mysql的方式
    # sql="SELECT * FROM twitter_1 ORDER BY create_time DESC LIMIT 10";  #以降冪查詢前10筆日期
    # cursor.execute(sql)
    # results = cursor.fetchall()
    # db_data.close()

    # if results == ():
    #     return results
    # else:
    #     search_time=[]
    #     for result in results:
    #         search_time.append(result[0])
    #     return search_time


if __name__ == '__main__':
    time()
    if time()==():
        print('no data')
    else:
        for i in time():
            print('time:',i)