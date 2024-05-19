# -*- coding: UTF-8 -*- 
import pandas as pd
import sqlite3
import os
import datetime
import sys
from function import global_api
import openpyxl
# sources = ['rakuya','h591','sinyi']
# key_words = ['寓上長安','寓上里安','裕森林','北歐','勇建光翼','水悅','FUN','昇揚','inn','四季文華','天空之城', '品藏', 'Green1','和聚原砌','MY']
# far_community = ['總太青境','總太聚作','敦富花園','鉅虹森美館','大城八月小確幸','日安花園','美樂地','文華硯','松築瓚','風格講義','九月采掬','勝麗方程式','勝麗方程市','CASA','登陽穗悅','七月沐樂','華太怡居','登陽仰峰','家在e起','東方悦','東方悅','鼎泰鑫鴻','櫻花沐然','幸福森林','大城四月泊樂','聚佳捷作','達麗居山','佳泰新麗馳','鵬程NEW1','登陽城之華','惠宇朗庭','鑫時代','大毅人人幸福','裕國豐展','總太2020','通豪高邑','富宇富好','翠堤清境','勝美樹','鴻邑璞麗','興願景','大城樂好事','惠宇一方庭','我愛龍邦','佳福青樂','富宇峰景','情定水蓮','佳福謙邑','一月春語','六月微風','宏台松築','惠宇千曦','自在柳陽']
# far_address = ['軍福','太原路三段','環太東路','松竹','旱溪','東山','軍功','敦富路','太原','好事多']
# bad_community = ['巴黎第六區','赫里翁臻愛','赫里翁傳奇','世界之心','市政愛悅',     # 社區複雜
#                 '洲際W']                                                       # 太吵             

# ignore_words = far_community + far_address + bad_community

# delete the house which is not demanded
def ignore_house(tableName, ignore_words):
    # open database
    try:
        sqlitePath = os.path.join(global_api.SQLiteDIR, global_api.SQLiteFILENAME) + '.sqlite3'
        conn = sqlite3.connect(sqlitePath)
        df_new_ignore = pd.read_sql('select * from '+ tableName, conn)
        print('Get ' + tableName + ' house data successfully')
    except pd.io.sql.DatabaseError as error:
        print(error)
        print('Please excute "compare_to_save()" to get today new house data')
        sys.exit()    
    
    # ignore houses which contain words in "ignore_words"
    def ignore_far_bad(df_new_ignore):  
        house_counts  = len(df_new_ignore)             # to count how many house be ignored
        ignore_counts = 0
        for index in range(house_counts):
            ingore_this = False
            for ignore_word in ignore_words:
                if (ignore_word in df_new_ignore['標題'].iloc[index]) or (ignore_word in df_new_ignore['社區'].iloc[index]):
                    ingore_this = True

            if ingore_this:
                df_new_ignore.at[index, '忽略'] = '1'
                ignore_counts += 1
            else:
                df_new_ignore.at[index, '忽略'] = '0'

        #     ignore_title     = ~ df_new_ignore['標題'].str.contains(ignore_word)
        #     ignore_community = ~ df_new_ignore['社區'].str.contains(ignore_word)
        #     selected_house   = ignore_title & ignore_community
        #     df_new_ignore    = df_new_ignore[selected_house][['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結']]
        # remain_counts = len(df_new_ignore)      # to count how many house remained
        # ignore_counts = counts - remain_counts  # to count how many house be ignored
        # print('Total ignore ' + str(ignore_counts) + ' houses which is too far away')
        print('Total ignore ' + str(ignore_counts) + ' houses which is bad')
        return df_new_ignore

    # ignore houses which is on the top floor
    def ignore_top_floor(df_new_ignore):
        number = 0
        top_floor_counts = 0
        for house in df_new_ignore.iloc:
            if (house['樓層'] == None):
                continue
            floor = house['樓層'].split('/')
            if len(floor) == 2:               # 確認是否為[11樓/15樓]格式,若為[11樓]格式則忽略 
                if floor[1] == floor[0]:
                    df_new_ignore.at[number, '忽略'] = '1'
                    # df_new_ignore.drop(df_new_ignore.index[number], inplace = True)
                    top_floor_counts += 1
                else:
                    if (df_new_ignore.at[number, '忽略'] != '1'):
                        df_new_ignore.at[number, '忽略'] = '0'
            number += 1
        df_new_ignore.reset_index(drop = True, inplace = True)
        print('Total ignore ' + str(top_floor_counts) + ' houses on the top floor')
        return df_new_ignore

    df_new_ignore = ignore_far_bad(df_new_ignore)
    df_new_ignore = ignore_top_floor(df_new_ignore)

    return df_new_ignore

def get_good_houses(tableName, good_words):
    try:
        sqlitePath = os.path.join(global_api.SQLiteDIR, global_api.SQLiteFILENAME) + '.sqlite3'
        conn = sqlite3.connect(sqlitePath)        
        df_new = pd.read_sql('select * from '+ tableName, conn)
        print('Get ' + tableName + ' house data successfully')
    except pd.io.sql.DatabaseError as error:
        print(error)
        print('Please excute "compare_to_save()" to get today new house data')
        sys.exit()    

    pd.set_option('display.max_colwidth', None)  # show all url elements

    # compare key word one house by one house
    house_counts  = len(df_new)             # to count how many house be ignored
    key_counts = 0
    for index in range(house_counts):
        this_is_good = False
        for good_word in good_words:
            if (good_word in df_new['標題'].iloc[index]) or (good_word in df_new['社區'].iloc[index]):
                this_is_good = True
                
        if this_is_good:
            df_new.at[index, '標的'] = '1'
            key_counts += 1
        else:
            df_new.at[index, '標的'] = '0'
    print('Total get key_word houses: ' + str(key_counts))

    return df_new

# def save_to_DB(area, source, df_house):

#     tableName = str(area) + '_new'
#     global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, tableName, df_house)


    # # build main dir
    # main_dir = os.path.join('function', 'data', 'crawling')
    # if not os.path.isdir(main_dir):
    #     os.makedirs(main_dir)

    # # open splite3 and save
    # now = datetime.datetime.now()
    # conn = sqlite3.connect(main_dir + '\\' + now.strftime('%Y%m%d') + '.sqlite3')
    # df_house.to_sql( str(source) +'_today_new', conn, if_exists = 'replace')
    # df_from_DB = pd.read_sql('select * from ' + str(source) +'_today_new', conn)
    # print('Save ' + str(source) + ' house successfully')
    # return None

# print good houses which contain words in "key_words" list
def print_good_houses(tableName, key_words):
    try:
        sqlitePath = os.path.join(global_api.SQLiteDIR, global_api.SQLiteFILENAME) + '.sqlite3'
        conn = sqlite3.connect(sqlitePath)
        df_new = pd.read_sql('select * from '+ tableName, conn)
        print('Get ' + tableName + ' house data successfully')
    except pd.io.sql.DatabaseError as error:
        print(error)
        print('Please excute "compare_to_save()" to get today new house data')
        sys.exit()    

    pd.set_option('display.max_colwidth', None)  # show all url elements
    # df_new_key = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])

    # compare key word one house by one house
    for key_word in key_words:
        search1 = df_new['標題'].str.contains(key_word)
        search2 = df_new['社區'].str.contains(key_word)
        selected_house = search1 | search2
        if len(df_new[selected_house]) == 0:
            print('No house contains word: ' + key_word)
        else:
            print(str(len(df_new[selected_house])) + ' houses contain word: ' + key_word)
            print(df_new[selected_house][['標題','社區','連結']])
        # df_new_key = df_new_key.append(df_new[select_house], ignore_index = True)
    print('/**************************************************************************************/')
    return None
    # return df_new_key

def filter():
    enableAreas = global_api.getEnableAreas()
    for area in enableAreas:
        good_words   = global_api.getAreaIniInfo(area, "GOOD_COMMUNITIES", "").split(",")
        ignore_words = global_api.getAreaIniInfo(area, "BAD_COMMUNITIES", "").split(",") 

        # remove empty
        while("" in good_words): 
            good_words.remove("")

        while("" in ignore_words): 
            ignore_words.remove("")

        tableName = str(area)
        df_after_ignore = ignore_house(tableName, ignore_words)
        global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, tableName, df_after_ignore, 'replace')
        df_get_goods    = get_good_houses(tableName, good_words)
        global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, tableName, df_get_goods, 'replace')
        print_good_houses(tableName, good_words)   

    return True

if __name__ =='__main__':
    filter()
