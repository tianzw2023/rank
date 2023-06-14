import requests
import pandas as pd
import time


path = 'C:\\Users\\Administrator\\Desktop\\research\\rank\\defi\\'


def get_defi_rank_list():
    header = {
        'User-Agent' :"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36"
    }

    rank_df = pd.DataFrame(columns=['rank', 'code', 'volume', 'changerate', 'percentage', 'typename'])

    try:
        res = requests.get(url = 'https://dncapi.bqrank.net/api/v2/Defi/newlockup/list/page?per_page=100&typeid=1&webp=1', headers = header, timeout=10)
        #['lockupitem']
        signal = 1
    except:
        #print("error")
        signal = 0
    #data_detail = data[0]['lockupitem']
    #for coin in data_detail:
    #    print(coin)
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
        rank_df.to_csv(path + time.strftime('%Y%m%d')+'.csv')
    else:
        print("error in getting defi data")

if __name__ == '__main__':
    print("yes")
    get_defi_rank_list()
