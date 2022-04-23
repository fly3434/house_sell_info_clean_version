import pandas as pd
import sqlite3
import os
import datetime
import sys

sources = ['rakuya','h591','sinyi'] 

def open_file(source, before_day):
    main_dir = os.path.join('function', 'data', 'crawling')
    if not os.path.isdir(main_dir):
        os.makedirs(main_dir)
    
    # get today data
    if before_day == 0: 
        today = datetime.datetime.now()  
        try:
            conn_today = sqlite3.connect(main_dir + '\\' + today.strftime('%Y%m%d') + '.sqlite3')
            df_today = pd.read_sql('select * from '+ str(source) +'_daily_house', conn_today)    
            print("open file: "+ main_dir + '\\' + today.strftime('%Y%m%d') + '.sqlite3', 'successfully')
            return df_today
        except pd.io.sql.DatabaseError as error:
            print(error)
            print('Please excute "crawl_and_save()" to crawl today data')
            sys.exit()      
    
    # get yesterday data
    else:
        for n_day in range(before_day, before_day+7):
            print('Try to get data from ' + str(n_day) + ' days ago')
            try:
                yesterday = datetime.datetime.now() - datetime.timedelta(n_day)
                conn_yesterday = sqlite3.connect(main_dir + '\\' + yesterday.strftime('%Y%m%d') + '.sqlite3')
                df_yesterday = pd.read_sql('select * from '+ str(source) +'_daily_house', conn_yesterday)
                print("open file: "+ main_dir + '\\' + yesterday.strftime('%Y%m%d') + '.sqlite3', 'successfully')
                return df_yesterday
            except pd.io.sql.DatabaseError as error:
                # print(error)
                print('No data existed yesterday, or file is not on the directory: ' + str(main_dir)) 
        print('No data can be compared on the past 7 days, please try again tomorrow')
        sys.exit() 

def compare(df_today, df_yesterday):
    df_today_new = df_today_delete = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])

    #compare new house
    for house in df_today.iloc:
        if house['連結'] not in df_yesterday['連結'].tolist():
            df_today_new = df_today_new.append(house, ignore_index = True)
    print('Objects are Compared successfully...New objects total: ' + str(len(df_today_new)) + ' counts')

    # #compare deleted house
    # for house in df_yesterday.iloc:
    #     if house['連結'] not in df_today['連結'].tolist():
    #         df_today_delete = df_today_delete.append(house, ignore_index = True)
    # print('Objects are Compared successfully...Deleted objects total: ' + str(len(df_today_delete)) + ' counts')

    return df_today_new  #, df_today_delete

def save(source, df_today_new):
    main_dir = os.path.join('function', 'data', 'crawling')
    if not os.path.isdir(main_dir):
        os.makedirs(main_dir)
    
    today = datetime.datetime.now() 
    conn_today = sqlite3.connect(main_dir + '\\' + today.strftime('%Y%m%d') + '.sqlite3')
    df_today_new.to_sql( str(source) +'_today_new', conn_today, if_exists = 'replace')
    print('Save new house successfully')
    # df_today_delete.to_sql( str(source) +'_today_delete', conn_today, if_exists = 'replace')
    # print('Save deleted house successfully')
    return None


def compare_to_save():
    for source in sources:
        df_today = open_file(source, 0)      # open today data
        df_yesterday = open_file(source, 1)  # open yesterday data
        df_today_new = compare(df_today, df_yesterday)
        save(source, df_today_new)

    return None


if __name__ == '__main__':
    compare_to_save()




