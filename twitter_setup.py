#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul  7 12:52:06 2019

@author: astana
"""

import os
import tweepy as tw
import time
from kafka import KafkaConsumer, KafkaProducer

# twitter access
consumer_key = "ed3HSrzkRbR5lmf410MCHUK47"
consumer_secret = "vp8Qx6YDcPQAK4BJ6900pN72JHFnAcDwJlmGpzm885qqZxySU0" 
access_token = "1115080180316430337-Noi1RpF8zw8fBNgB64zms3kDjoRj6A"
access_token_secret = "ntyp0sOGhDJFEciYoxcqEuhaKneq8cFkrcxzMUsPqhNmH"
# Creating the authentication object
auth = tw.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tw.API(auth) 

# get today's date and time 
from datetime import datetime, timedelta

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=-4)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

# Define Kafka Producer
    
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'tweets-lambda1'

# Producing and sending records to the Kafka Broker
date_since = "2019-07-06"
search_words = "trump -filter:retweets"
# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items()

# Iterate on tweets
#x = 0 
#for tweet in tweets:
#    if x < 10:
#        print(tweet.text)
#    x += 1

def get_twitter_data():
    tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en"
#              ,since=date_since
              ).items()
    for tweet in tweets:
        producer.send(topic_name, str.encode(tweet.text))
       # 1004910016271298560;2019-07-07 19:06:01;122;Malaysia;0;0; 
get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()  
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)
        
periodic_work(60*1)





















# Producing and sending records to the Kafka Broker
    def get_twitter_data():
        res = api.search(search_words, lang="en", )
        for i in res:
            record = ''
            record += str(i.user.id_str)
#            record += ';'
#            record += str(normalize_timestamp(str(i.created_at)))
#            record += ';'
#            record += str(i.user.followers_count)
#            record += ';'
#            record += str(i.user.location)
#            record += ';'
#            record += str(i.favorite_count)
#            record += ';'
            record += str(i.text) #str(i.retweet_count)
            record += ';'
            producer.send(topic_name, str.encode(record))
           # 1004910016271298560;2019-07-07 19:06:01;122;Malaysia;0;0; 
    get_twitter_data()
    
    def periodic_work(interval):
        while True:
            get_twitter_data()  
            #interval should be an integer, the number of seconds to wait
            time.sleep(interval)
            
    periodic_work(60*1)


