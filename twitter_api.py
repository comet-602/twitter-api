import pandas as pd
import numpy as np
from twitter import *
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.types import CHAR,INT
import datetime
import t_search
import t_tok
import line_notify

def t_api():
    #twitter token 需申請，token放別處
    tok=t_tok.t_tok()
    twitter = Twitter(auth = OAuth(tok[0],tok[1],tok[2],tok[3]))

    #設定爬取發文數量
    tweets_count=3

    #設定爬取條件，人名、評論數、喜好數、內文
    statuses = twitter.statuses.user_timeline(screen_name = "@realDonaldTrump",count=tweets_count,tweet_mode='extended')

    created_at=[]
    for data in statuses:
        dt_time=datetime.datetime.strptime(data['created_at'].replace('+0000',''),'%a %b %d %H:%M:%S %Y')
        created_at.append(dt_time)
    #print(created_at)
    retweet=[data['retweet_count'] for data in statuses]
    favorite_count=[data['favorite_count'] for data in statuses]
    text=[data["full_text"].replace("“","_").replace("’","_") for data in statuses]


    #將資料存成DataFrame
    df = pd.DataFrame({"create_time": created_at,
                        'retweet_count': retweet,
                        'favorite_count': favorite_count,
                        'text': text})


    #連接mysql資料庫
    try:
        db_data = 'mysql+pymysql://root:123456@localhost:3306/twitter_1?charset=utf8mb4'
        engine = create_engine(db_data)
        print("成功連到mysql")
    except Exception as lex:
        print('連線發生問題:',lex)


    #套入t_search，得到資料庫前幾筆資料
    #判斷是否有重複，若無重複則insert至資料庫
    try:
        for i in range(tweets_count):
            print('count:',i+1)
            if created_at[i] in t_search.time():
                print('I have it')
            else:
                db_data = pymysql.connect("localhost","root","123456","twitter_1" )
                cursor = db_data.cursor()
                
                sql="INSERT INTO twitter_1(create_time, retweet_count, favorite_count, text) VALUES ('%s', '%d', %d, '%s')" %(df.iloc[i,0], df.iloc[i,1], df.iloc[i,2], df.iloc[i,3]);
                print(df.iloc[i,0], df.iloc[i,1], df.iloc[i,2], df.iloc[i,3])
                n_text='\n發文人:realDonaldTrump\n發文時間:'+str(df.iloc[i,0])+'\n推撥數:'+str(df.iloc[i,1])+'\n喜愛數:'+str(df.iloc[i,2])+'\n內文:\n'+str(df.iloc[i,3])
                line_notify.lineNotifyMessage(n_text)  # line notify 會推撥訊息
                print(n_text)
                
                cursor.execute(sql)          
                db_data.commit()
                db_data.close()
        return 


    except Exception as ex:
        print("問題發生:",ex)
        print(df.iloc[i,3])

if __name__ == '__main__':
    t_api()
