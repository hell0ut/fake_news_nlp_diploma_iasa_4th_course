    import pandas as pd
    import numpy as np
    import json
    from bs4 import BeautifulSoup as bs
    import requests as r
    from tqdm import tqdm
    from csv import writer
    from pathlib import Path
    from threading import Thread,Lock
    import schedule
    import os
    from time import time

    # mutexes for not breaking everything using threads
    error_file_mutex = Lock()
    news_file_mutex = Lock()
    mutex_log = Lock()
    mutex_log_max = Lock()


    #function to parse one tweet by its id and write to csv file
    def get_tweet_text_by_id(filepath,news_id,tweet_id,write_row_func):
        res = r.get(f'https://publish.twitter.com/oembed?dnt=true&omit_script=true&url=https://mobile.twitter.com/i/status/{tweet_id}')
        if res.status_code == 200:
            try:
                obj =json.loads(res.text)
                soup = bs(obj['html'],features='lxml')
                return soup.find('p').text
            except Exception as e:
                write_row_func(filepath,news_id,tweet_id,str(e))
                return ''
        else:
            write_row_func(filepath,news_id,tweet_id,res.status_code)
            return ''
        

    df_with_parsed_tweets_columns = ['news_id','news_url','title','tweet_id','text','label']

    required_files_with_cols = [('parsed\\news.csv',['id','title','news_url']),
                                ('parsed\\errors.csv',['filepath','news_id','tweet_id','error_text']),
                                ]

    FakeNewsDataset_files = ['datasets/FakeNewsNet/gossipcop_fake.csv',
                            'datasets/FakeNewsNet/gossipcop_real.csv',
                            'datasets/FakeNewsNet/politifact_fake.csv',
                            'datasets/FakeNewsNet/politifact_real.csv']

    labels_mapping = {'real':1,'fake':0}


    for ds_file_path in FakeNewsDataset_files:
        _,__,dataset = ds_file_path.split('/')
        ds_name = (dataset.split('.'))[0]
        new_file_path = f'parsed\\parsed_tweets_{ds_name}.csv'
        required_files_with_cols.append((new_file_path,df_with_parsed_tweets_columns))


    # def write_head_to_csv(path,columns_list):
    #     with open(path,'w+',newline='') as f:
    #         w = writer(f)
    #         w.writerow(columns_list)

    # script is reran so this part is not used for now to not wipe all the files
    # for filepath,columns in required_files_with_cols:
    #     write_head_to_csv(filepath,columns)


    # for decided to get rid of news file cuz of bugs and really needlessness of it
    #news_file_path = required_files_with_cols[0][0]
    #NEWS_FILE_DESC = open(news_file_path,'a',encoding='utf-8',newline='')
    #NEWS_WRITER = writer(NEWS_FILE_DESC)

    error_file_path = required_files_with_cols[1][0]
    ERROR_FILE_DESC = open(error_file_path,'a',encoding='utf-8',newline='')
    ERROR_WRITER = writer(ERROR_FILE_DESC)



    #writing row to csv error
    def write_error_row(filepath,news_id,tweet_id,error_text):
        error_file_mutex.acquire()
        try:
            ERROR_WRITER.writerow([filepath,news_id,tweet_id,error_text])
        finally:
            error_file_mutex.release()



    # def write_news_row(news_id,title,url):
    #     news_file_mutex.acquire()
    #     try:
    #         NEWS_WRITER.writerow([news_id,title,url])
    #     finally:
    #         news_file_mutex.release()

    def save_files():
        ERROR_FILE_DESC.close()
        # NEWS_FILE_DESC.close()



    logfile = open('parsed\\log.txt','a+',encoding='utf-8')
    logfile.close()


    news_parsed = 3842
    news_max = 0

    # def print_log_dictionary():
    #     global ask_iteration
    #     ask_iteration+=1
    #     with open('parsed\\log.txt','a',encoding='utf-8') as f:
    #         f.write(f'Time passed: {ask_iteration*ask_interval} minutes\n')    
    #         for ds_file_path in FakeNewsDataset_files:
    #             _,__,dataset = ds_file_path.split('/')
    #             ds_name = (dataset.split('.'))[0]
    #             f.write(f'{dataset} parsed {log_cur.get(ds_name,0)} of {log_max.get(ds_name,0)}\n')


    START_TIME =time()


    starting_points = {'parsed\\parsed_tweets_gossipcop_fake.csv': 'gossipcop-699303448',
                        'parsed\\parsed_tweets_gossipcop_real.csv': 'gossipcop-881670',
                        'parsed\\parsed_tweets_politifact_fake.csv': 'politifact15135',
                        'parsed\\parsed_tweets_politifact_real.csv': 'politifact954'}


    def run_parsing(ds_file_path,use_starting_ids=False):
        global news_max
        global news_parsed
        _,__,dataset = ds_file_path.split('/')
        ds_name,ds_label = (dataset.split('.'))[0].split('_')
        new_file_path = new_file_path = f'parsed\\parsed_tweets_{ds_name+"_"+ds_label}.csv'
        label = labels_mapping[ds_label]
        df = pd.read_csv(ds_file_path)
        f = open(new_file_path,'a+',encoding='utf-8',newline='') # w+ to wipe +a to continue filling files
        f_writer = writer(f)

        starting_id_reached = True
        if use_starting_ids:
            starting_id = starting_points[new_file_path]
            starting_id_reached = False

        try:
            mutex_log_max.acquire()
            news_max += df.shape[0]
            mutex_log_max.release()

            for index,row in df.iterrows():
                # news_url = row['news_url']
                # title = row['title']
                news_id = row['id']
                if news_id == starting_id:
                    starting_id_reached = True

                if starting_id_reached:
                #write_news_row(news_id,title,news_url)
                    try:
                        tweet_ids = row['tweet_ids'].split('\t')
                    except:
                        pass
                    for tweet_id in tweet_ids:
                        tweet_content = get_tweet_text_by_id(ds_name+" "+ds_label,news_id,tweet_id,write_error_row)
                        if len(tweet_content)>0:
                            f_writer.writerow([news_id,tweet_id,tweet_content,label])
                    mutex_log.acquire()
                    news_parsed+=1
                    with open('parsed\\log.txt','a',encoding='utf-8') as flog:
                        flog.write(f'Time passed: {time()-START_TIME} seconds. News parsed: {news_parsed} of {news_max}\n')
                    mutex_log.release()


        finally:
            f.close()



    threads = list()


    for filepath in FakeNewsDataset_files:
        thread = Thread(target=run_parsing,args=(filepath,True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    save_files()


