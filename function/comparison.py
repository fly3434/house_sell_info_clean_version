import pandas as pd
import sqlite3
import os
import datetime
import sys
from function import global_api

def open_file(table_name, before_day):
    main_dir   = global_api.SQLiteDIR
    if not os.path.isdir(main_dir):
        os.makedirs(main_dir)
    
    # get today data
    if before_day == 0: 
        print('Try to get today data...')
        try:
            sqlitePath = os.path.join(global_api.SQLiteDIR, global_api.SQLiteFILENAME) + '.sqlite3'
            conn = sqlite3.connect(sqlitePath)            
            df_today = pd.read_sql('select * from '+ table_name, conn)    
            # print("open file: "+ sqlitePath, 'successfully')
            print(len(df_today))
            return True, df_today
        except pd.io.sql.DatabaseError as error:
            print(error)
            print('Please excute "crawl_and_save()" to crawl today data or no data today')
            return False, ""
    
    # get yesterday data
    else:
        for n_day in range(before_day, before_day+7):
            print('Try to get data from ' + str(n_day) + ' days ago...')
            try:
                yesterday      = datetime.datetime.now() - datetime.timedelta(n_day)
                sqlitePath     = os.path.join(global_api.SQLiteDIR, yesterday.strftime('%Y%m%d')) + '.sqlite3'
                conn_yesterday = sqlite3.connect(sqlitePath)
                df_yesterday   = pd.read_sql('select * from '+ table_name, conn_yesterday)
                # print("open file: "+ sqlitePath + '.sqlite3', 'successfully')
                return True, df_yesterday
            except pd.io.sql.DatabaseError as error:
                pass
                # print(error)
                # print('No data existed yesterday, or file is not in the directory: ' + str(main_dir)) 
        print('No data can be compared on the past 7 days, please try again tomorrow')
        return False, ""

def compare(df_today, df_yesterday):
    df_today_new = df_today_delete = global_api.TABLETITLE

    #compare new house
    for house_today in df_today.iloc:
        if house_today['連結'] not in df_yesterday['連結'].tolist():
            # append
            df_today_new = pd.concat([df_today_new, house_today.to_frame().T], ignore_index=True)
    print('New objects total: ' + str(len(df_today_new)) + ' counts')
    df_today_new['總價'] = df_today_new['總價'].astype(str) # to prevent "總價" column in binary format, reason is unknown
    return df_today_new  #, df_today_delete

def save_to_excel(dataFrame, excelDir, excelName, sheetName):
    if (dataFrame.empty):
        print("dataFrame: " + sheetName + " is empty, skip save to excel!")
        return None

    print('Start '+ sheetName + ' data saving to .xlsx...')
    if not os.path.isdir(excelDir):
        os.makedirs(excelDir)
    # excelPath      = exceldir + '\\' + today +'_daily_new.xlsx'
    excelPath      = os.path.join(excelDir, excelName)
    writerMode     = 'w'
    if_sheet_exists=None
    if os.path.exists(excelPath):
        writerMode = 'a'
        if_sheet_exists='replace'

    # write to excel
    with pd.ExcelWriter( 
        excelPath,
        mode = writerMode,                   # if existing Excel file -> append data
        if_sheet_exists = if_sheet_exists,   # if existing sheet      -> replace
        ) as writer:  
        # df_h591_new.to_excel(writer, sheet_name='591新資料') 
        dataFrame.to_excel(writer, sheet_name=sheetName) 
    print('Save ' + str(len(dataFrame.index)) + ' houses to excel successfully')
    return None

def compare_and_save():
    print('===== Start Filtering Today`s New House Data =====')
    enableAreas = global_api.getEnableAreas()
    for area in enableAreas:
        table_name     = str(area)
        new_table_name = str(area) + '_new'
        result, df_today = open_file(table_name, 0)  # open today data
        if (result == False):
            continue
        result, df_yesterday = open_file(table_name, 1)  # open yesterday data
        if (result == False):
            continue
        df_today_new   = compare(df_today, df_yesterday)
        global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, new_table_name, df_today_new)

        excelDir       = os.path.join('results', global_api.SQLiteFILENAME) 
        excelName      = global_api.SQLiteFILENAME + '_daily_new.xlsx'
        save_to_excel(df_today_new, excelDir, excelName, new_table_name)
    
    print('===== End of Filtering Today`s New House Data =====')
    return True

if __name__ == '__main__':
    compare_and_save()




