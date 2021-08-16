import numpy as np
import mysql.connector
import sys
import boto3
import json

class AWS_Comprehend:
    
    def __init__(self, sentence_id):
        #データベースへの接続
        self.dbh = mysql.connector.connect(
            host='localhost',
            port='3306',
            db='naisyo',
            user='naisyo',
            password='Oshienaiyo',
            charset='utf8'
        )
        self.cur = self.dbh.cursor()
        self.sentence_id = sentence_id
        
        #AWSのインスタンス化
        self.comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        
        #(1)解析対象文章をDBから取り出す
        self.sentence = self.getSentence()
        
        #(2)KeyPhraseのDBへの格納
        self.getKeyPhrase()
        
        #(3)EntityのDBへの格納
        self.getEntity()
        
        #(4)SentimentのDBへの格納
        self.getSentiment()
        
        #DBのCommit
        self.dbh.commit()
        
    def getSentence(self):
        self.cur.execute("SELECT sentence FROM `sentences` WHERE `id` = " + str(self.sentence_id) +";")
        sentences = self.cur.fetchall()
        return sentences[0]
    
    def getKeyPhrase(self):
        phrases = self.comprehend.detect_key_phrases(Text=str(self.sentence), LanguageCode='ja')
        #KeyPhraseのDBへの格納
        for i in range(0, len(phrases['KeyPhrases'])):
            self.cur.execute('INSERT INTO `mzncomprehends`.`keyphrases` (`sentence_id`, `phrase`, `score`) VALUES (%s, %s, %s)', (self.sentence_id, phrases['KeyPhrases'][i]['Text'], phrases['KeyPhrases'][i]['Score']))
   
    def getEntity(self):
        entities = self.comprehend.detect_entities(Text=str(self.sentence), LanguageCode='ja')
        for i in range(0, len(entities['Entities'])):
            self.cur.execute('INSERT INTO `mzncomprehends`.`entities` (`sentence_id`, `entities`, `category`, `score`) VALUES (%s, %s, %s, %s)', (self.sentence_id, entities['Entities'][i]['Text'], entities['Entities'][i]['Type'], entities['Entities'][i]['Score']))

    def getSentiment(self):
        sentiments = self.comprehend.detect_sentiment(Text=str(self.sentence), LanguageCode='ja')
        sentiment_list = ['Positive', 'Negative', 'Neutral', 'Mixed']
        for i in sentiment_list:
            self.cur.execute('INSERT INTO `mzncomprehends`.`sentiments` (`sentence_id`, `sentiment`, `score`) VALUES (%s, %s, %s)', (self.sentence_id, i, sentiments['SentimentScore'][i]))

    
    
if __name__ == '__main__':
    sentence_id = sys.argv[1]
    aws = AWS_Comprehend(sentence_id)
    
    #呼び出し python3 AWS_Comprehend.py 1(sentencesテーブルのID:解析したい文章番号)
    #プログラム実行前に .aws/credentials のAPIキーの更新をしておく