import requests
import pandas as pd
from dingtalkchatbot.chatbot import DingtalkChatbot
from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime
from config import config

class Bigdata():
    def __init__(self):
        self.path_coin = 'C:\\Users\\Administrator\\Desktop\\research\\10\\rank\\data\\'
        self.path_defi = 'C:\\Users\\Administrator\\Desktop\\research\\10\\rank\\defi\\'
        self.path_concept = 'C:\\Users\\Administrator\\Desktop\\research\\10\\rank\\concept\\'

        self.scheduler = BackgroundScheduler()
        """
        self.header = {'User-Agent' :"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
                  "Referer": "https: // www.feixiaohao.cc/",
                  "Sec - Fetch - Site": "cross - site",
                  "Host": "dncapi.bqrank.net",
                  "Origin": "https: // www.feixiaohao.cc",
                  "Referer": "https: // www.feixiaohao.cc/",

                  }
        """
        self.header = {'User-Agent' :"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"


                  }
        self.dingding = DingtalkChatbot(config.webhook)

    def get_rank_list(self):
        print("get_rank_list")

        #signal = 0
        try:
            res = requests.get(url = 'https://dncapi.soulbab.com/api/coin/web-coinrank?page=1&type=-1&pagesize=100&webp=1', headers = self.header, timeout=10)
            #print(res)
            signal = 1

        except:
            signal = 0

        print("signal: ", signal)
        if signal == 1:
            i = 1
            rank_df = pd.DataFrame(columns=['rank', 'name', 'current_price_usd', 'market_value_usd', 'supply', 'turnoverrate'])
            for coin in res.json()['data']:

                new = pd.DataFrame({'rank': coin['rank'], 'name': coin['name'], 'current_price_usd': coin['current_price_usd'], 'market_value_usd': coin['market_value_usd'], 'supply': coin['supply'], 'turnoverrate': coin['turnoverrate']}, index = [i])
                i+=1
                rank_df = rank_df.append(new, ignore_index=True)
            #print(rank_df)
            rank_df.to_csv(self.path_coin + time.strftime('%Y%m%d') + '.csv')
            #self.dingding.send_text(msg=f"成功下载市值排名数据, {time.strftime('%Y%m%d %H:%M:%S')}", is_at_all=False)

        else:
            print("error in getting rank")
            #self.dingding.send_text(msg=f"下载市值排名数据出现错误, {time.strftime('%Y%m%d %H:%M:%S')}", is_at_all=False)

            #rank_df = 0



    def get_defi_rank_list(self):


        rank_df = pd.DataFrame(columns=['rank', 'code', 'volume', 'changerate', 'percentage', 'typename'])

        try:
            res = requests.get(url = 'https://dncapi.bqrank.net/api/v2/Defi/newlockup/list/page?per_page=100&typeid=1&webp=1', headers = self.header, timeout=10)

            signal = 1
        except:

            signal = 0

        if signal == 1:
            data = res.json()['data']['list']
            rank = 0
            for coin in data[0]['lockupitem']:
        # print(coin)
                new = pd.DataFrame({'rank': rank+1, 'code': coin['code'], 'volume': coin['volume'],
                            'changerate': coin['changerate'], 'percentage': coin['percentage'],
                            'typename': coin['typename']}, index=[rank])
                rank += 1
                rank_df = rank_df.append(new, ignore_index=True)
            rank_df.to_csv(self.path_defi + time.strftime('%Y%m%d')+'.csv')
            self.dingding.send_text(msg=f"成功下载DEFI锁仓量排名数据, {time.strftime('%Y%m%d %H:%M:%S')}", is_at_all=False)

        else:
            print("error in getting defi data")
            #self.dingding.send_text(msg=f"下载DEFI锁仓量排名数据出现错误, {time.strftime('%Y%m%d %H:%M:%S')}", is_at_all=False)

    def get_hot_races(self):


        rank_df = pd.DataFrame(columns=['rank', 'name', 'change'])

        try:
            res = requests.get(url = 'https://dncapi.bqrank.net/api/concept/conceptapplies?pagesize=10&type=1&webp=1', headers = self.header, timeout=10)
            #print(res)
            signal = 1
        except:

            signal = 0
        #print(res.json()['data'])
        if signal == 1:
            print(res.json()['data'])
            message = []
            data = res.json()['data']
            rank = 0
            for concept in data:
        # print(coin)
                new = pd.DataFrame({'rank': rank+1, 'concpet': concept['name'], 'change': concept['change']}, index=[rank])
                message.append((concept['name'], concept['change']))
                rank += 1
                rank_df = rank_df.append(new, ignore_index=True)
            rank_df.to_csv(self.path_concept + time.strftime('%Y%m%d')+'.csv')
            self.dingding.send_text(msg=f"概念板块排名:{message}, {time.strftime('%Y%m%d')}", is_at_all=False)

        else:
            print("error in getting concept data")
        #    self.dingding.send_text(msg=f"下载概念板块排名数据出现错误, {time.strftime('%Y%m%d %H:%M:%S')}", is_at_all=False)

    def get_defi_tvl_rank(self):
        hearder = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",}
        try:
            res = requests.get(url='https://defi-tracker.dappradar.com/api/ethereum/defi/all?currency=USD&limit=25&page=1&sort=tvlInFiat&order=desc',
                           headers=hearder, timeout=10)
            print(res)
        except:
            print("no response")
        #print(res.json())




    def monitor(self):
        self.scheduler.add_job(self.get_rank_list, 'cron', hour='17', minute='19', second='01')
        self.scheduler.add_job(self.get_defi_rank_list, 'cron', hour='17', minute='10', second='01')
        self.scheduler.start()
        while True:
            time.sleep(5)

def func():
    print("yes")


if __name__ == '__main__':
    # 创建后台执行的 schedulers
    bigdata = Bigdata()
    #bigdata.monitor()
    #bigdata.get_hot_races()
    #bigdata.get_defi_chain_rank()
    #bigdata.get_defi_rank_list()
    bigdata.get_rank_list()
    #bigdata.get_defi_tvl_rank()

    #bigdata.get_rank_list()
    #bigdata.get_defi_rank_list()
    #scheduler = BackgroundScheduler()
    #scheduler.add_job(bigdata.get_rank_list, 'cron', hour='17', minute='07', second='01')
    #scheduler.add_job(bigdata.get_rank_list, 'cron', hour='17', minute='11', second='01')
    #scheduler.add_job(bigdata.get_defi_rank_list, 'cron', hour='17', minute='30', second='00')
    #scheduler.start()
    #while True:
    #    time.sleep(5)
#get_rank_list()





#https://dncapi.bqrank.net/api/concept/conceptapplies?pagesize=10&type=1&webp=1