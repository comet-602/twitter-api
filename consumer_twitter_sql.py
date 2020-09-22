from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import time

import pandas as pd
import numpy as np
from twitter import *
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.types import CHAR,INT
import datetime
import t_search
import t_tok

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)


if __name__ == '__main__':
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': 'localhost:9092',       # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'STUDENT_ID',                     # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'earliest',             # Offset從最前面開始
        'session.timeout.ms': 6000,                  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb                         # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = "twitter"
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName], on_assign=my_assign)
    # 步驟5. 持續的拉取Kafka有進來的訊息
    count = 0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}\n'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata

                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    df = pd.DataFrame({"create_time": eval(msgValue)[0],
                                        'retweet_count': eval(msgValue)[1],
                                        'favorite_count': eval(msgValue)[2],
                                        'text':eval(msgValue)[3]})

                    #連接mysql資料庫
                    try:
                        db_data = 'mysql+pymysql://root:123456@localhost:3306/kafka_twitter?charset=utf8mb4'
                        engine = create_engine(db_data)
                        print("成功連到mysql")
                    except Exception as lex:
                        print('連線發生問題:',lex)

                    try:
                        for i in range(len(eval(msgValue)[0])):
                            print('count:',i+1)
                            if eval(msgValue)[0][i] in t_search.time():
                                print('I have it')
                            else:
                                db_data = pymysql.connect("localhost","root","123456","kafka_twitter" )
                                cursor = db_data.cursor()
                                
                                sql="INSERT INTO k_twitter(create_time, retweet_count, favorite_count, text) VALUES ('%s', '%d', %d, '%s')" %(df.iloc[i,0], df.iloc[i,1], df.iloc[i,2], df.iloc[i,3]);
                                
                                cursor.execute(sql)          
                                db_data.commit()
                                db_data.close()

                    except Exception as ex:
                        print("問題發生:",ex)


                    # 秀出metadata與msgKey & msgValue訊息
                    count += 1
                    print('{}-{}-{}'.format(topic, partition, offset))
                    #print('{}-{}-{} : ({} , {})'.format(topic, partition, offset, msgKey, msgValue))
                    time.sleep(1)
    
    
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(e)

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()
