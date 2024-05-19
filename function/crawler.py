################################################
#########    rakuya crawler area    ############
################################################
import pandas as pd
import requests
from lxml import etree
import os
import datetime
import time
import random
import sqlite3
from function import global_api
DEFAULT_ROWS_IN_RAKUYA_PAGE = 19

def rakuya_url_trans(url):
    # from //www.rakuya.com.tw/xxx 
    # to   //www.rakuya.com.tw/xxx&page=

    if url.endswith("&page="):
        return url
    else:
        return url + "&page="

def get_rakuya(area, url_from_ini):
    print("===== Start Parsing " + area + " houses from Rakuya =====")
    df_rakuya_house = global_api.TABLETITLE
    source          = "Rakuya"
    page_count      = 1
    first_page      = True
    last_house      = False
    total_rows      = -1
    total_page      = -1

    while last_house == False: # check if this is last page
        headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        # url_from_ini = global_api.getAreaIniInfo(area, "SOURCE_URL", "rakuya")
        url_standard = rakuya_url_trans(url_from_ini)
        url       = url_standard + str(page_count)
        # url      = "https://www.rakuya.com.tw/sell/result?city=8&zipcode=407%2C406&price=500~1500&room=2%2C3&floor=7~11%2C12~&age=~15&other=P&sort=11&browsed=0&page=" + str(page_count)
        res       = requests.get(url, headers = headers)
        tree      = etree.HTML(res.text)   

        # print Total rows
        if (first_page):
            total_rows_tmp = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[1]/div[2]/div/span/span')
            total_rows = total_rows_tmp[0].text
            total_page = -(((-1) * int(total_rows)) // DEFAULT_ROWS_IN_RAKUYA_PAGE)
            print("Total " + str(total_rows) + " houses, " + str(total_page) + " pages, Start parsing...")
            first_page = False

        # parsing from first house to last house (20) on one page, get data by xpath
        for j in range(1, DEFAULT_ROWS_IN_RAKUYA_PAGE + 1):
            title          = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[1]/h2')
            if title == []:  # if title = empty, means there is no data behind it
                last_house = True
                break
            
            # if title[0].text == "新上架":  # if the title is "新上架", parsing the second title
            #     title = tree.xpath(' /html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[1]/span[2]')
            link           = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/@href')
            community      = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/h2/span[2]')
            size           = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/ul/li[1]/text()')
            room_count     = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/div/ul/li[2]')
            price          = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[2]/div/span/b')
            # if price == []:
            #     price = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[3]/span[2]/text()')

            price_per_unit_tmp = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/ul/li[3]')
            if(len(price_per_unit_tmp) == 0):
                price_per_unit = None
            else:
                price_per_unit = price_per_unit_tmp[0].text
            
            year           = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/div/ul/li[3]')
            if '樓' in year[0].text:   # sometimes there is no house age data, the 樓層 will shift to front
                house_year = None
                floor      = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/div/ul/li[3]')
            else:
                house_year = year[0].text
                floor      = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[4]/div[1]/section['+ str(j) +']/a/div[2]/div[2]/div[1]/div/div/ul/li[4]')        

            # save these data to dataframe
            df_rakuya_house = pd.concat([df_rakuya_house, pd.DataFrame([{'標的': '', '忽略': '', '標題': title[0].text, '社區': community[0].text, '坪數':size[1], '格局': room_count[0].text, '總價': price[0].text, '萬/坪': price_per_unit, '屋齡': house_year, '樓層': floor[0].text, '來源': source, '連結': link[0]}])], ignore_index = True)
        
        # to prevent total rows count can fully divided by dafault rows in a page 
        if page_count >= -(((-1) * int(total_rows)) // DEFAULT_ROWS_IN_RAKUYA_PAGE):
            last_house = True

        print('Parse page number: ' + str(page_count) + '/' + str(total_page) + ' from Rakuya successfully')
        page_count += 1
        time.sleep(random.randint(2,6))
    print("Parsing " + area + " data from Rakuya successfully, Saving...")
    return df_rakuya_house

################################################
###########    591 crawler area    #############
################################################
import pandas as pd
import requests
from lxml import etree
import os
import datetime
import time
import json
import random
from bs4 import BeautifulSoup
import warnings 
import sqlite3
DEFAULT_ROWS_IN_591_PAGE = 30

# get CSRF token, then I can parsing house data from sub_url
def get_591_token(rs, url): 
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cookie':'webp=1; PHPSESSID=ek8g1s3hdl4lddooptfrrq5cg1; T591_TOKEN=ek8g1s3hdl4lddooptfrrq5cg1; is_new_index=1; is_new_index_redirect=1; __auc=a840505817f796e92ca60ef1302; newUI=1; tw591__privacy_agree=1; __utma=82835026.1091730978.1647009760.1655302319.1655302319.1; __utmc=82835026; d161625dfbb5ac1ad88659a2e77c5b5a=1; statement-privacy=%7B%22userIds%22%3A%5B2847772%5D%2C%22isAgree%22%3Atrue%7D; _ga_ZDKGFY9EDM=GS1.1.1659841667.24.1.1659841669.0; user_index_role=2; _ga_8E3GTG4K5C=GS1.3.1693721113.3.0.1693721113.60.0.0; _ga_HCHGBJRJLT=GS1.3.1693721114.3.0.1693721114.0.0.0; _ga_RN6MJ068GP=GS1.3.1693721114.3.0.1693721114.0.0.0; urlJumpIp=7; _ga=GA1.4.1091730978.1647009760; __lt__cid=c6fecd93-a4fe-4bd2-9a03-eb094e6a5c06; _fbp=fb.2.1707442764740.512421703; last_search_type=2; _gid=GA1.3.352186247.1707621548; _gid=GA1.4.352186247.1707621548; __lt__sid=c41c57bf-9234deef; fcm_pc_token=eOP6xCIpSCH6t7cpihPLxR%3AAPA91bHhzP69uZzVGYRCcbgKTGo-TZuASrFr98Rj_zZDSbmIxKKUps9G0z6hZzvXdhFbgCPgQHwKM2uzC6CeQ2180wHID-whylG-MPfCQry1Sm_jXJqw6wyUtFY7xKxm75IIxRV2ZyHC; timeDifference=-1; XSRF-TOKEN=eyJpdiI6IlJPYnU2ck9mL0hUUFNyNzAzeWkxbEE9PSIsInZhbHVlIjoiWkhkL05GeGhZd1lCVFhhWFRGVG02UWhKTHN3QUQwdk10Mmdvb1loS0hablp4ejZJMkZVaGdHbytWdldlUUFwbE1xQUxidlRNNzV2bVc3NkFLdjhkUzBydWtkODU0UGJ1dzFWSEh4NElrS1M0MThnSTEwaEh6LzMzL296MHpmbWwiLCJtYWMiOiI3ZDMwOTUyOGRiMTJmODk1Zjg5MTI1ZDNiYjIyZTQ5OWQzOGZkY2FiYmViOTUxYjRhYTEwYTBjOGNmMTQzZDY2IiwidGFnIjoiIn0%3D; user_browse_recent=a%3A5%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14906927%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14901347%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A15015851%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14965859%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A13995175%3B%7D%7D; 591_new_session=eyJpdiI6ImFoTCttUUZuWlhScklxN2g2S1JWOVE9PSIsInZhbHVlIjoiNGVLWUpneUlBdGs5YmZpUkJvUGFUSVBpWkhZdFlWcjZkRVgrVzBsVnpLTWo0NVdkWkwvVmhRcDNaTEJqMFV5QkdQc2EzVmhCWUdXSFlMK2V3TDdFaUhuOUUrN3VIV3pFMzJiN284WXljQU95TWhLaThrSENOK2FnK3RMcGhHekciLCJtYWMiOiI0MDA2MWU5MjA0MmRiMzUxOWQyMzk4NTgxODQ1MGM3ZDI2ZGUyOWIwOWI1NDVkM2ZjYzcyMWEwNWNlMzYxZjM2IiwidGFnIjoiIn0%3D; _gat=1; _dc_gtm_UA-97423186-1=1; _ga_H07366Z19P=GS1.3.1707621548.13.1.1707626798.60.0.0; _ga=GA1.3.1091730978.1647009760; _gat_UA-97423186-1=1; _ga_HDSPSZ773Q=GS1.1.1707621548.43.1.1707626798.0.0.0',
    }
    home_res = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    # requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    soup = BeautifulSoup(home_res.text, 'lxml')
    csrf_token = soup.find("meta", dict(name="csrf-token"))['content']

    return csrf_token

def get_591_XHR_request_URL(url):
    # 591 house list is dynamic content
    # and it can be got by XHR json response
    # but we don`t know how to get XHR request URL
    # so we format origin URL to XHR-like URL, and it works
    # origin_url = 'https://sale.591.com.tw/?shType=list&regionid=7&section=80,81&kind=0&shape=2,1&pattern=2&houseage=0_5,5_10&floor=5$_100$&firstRow=30&totalRows=100'
    # XHR_url    = 'https://sale.591.com.tw/home/search/list?type=2&shType=list&regionid=8&section=105,103&kind=9&price=2000_3000&shape=2&pattern=3&houseage=5_10,0_5&totalRows=1824&firstRow=0&timestamp=1707625315133&recom_community=1'

    XHR_url = ""

    if ("/?" in url):
        XHR_url = url.replace("/?", "/home/search/list?type=2&")
        XHR_url += "&timestamp=1707625315133&recom_community=1"
    else:
        print("get_XHR_request_URL ERROR!! WRONG URL format!")

    return XHR_url

# get page response
def get_591_res(rs, token, url):
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cookie':'webp=1; PHPSESSID=ek8g1s3hdl4lddooptfrrq5cg1; T591_TOKEN=ek8g1s3hdl4lddooptfrrq5cg1; is_new_index=1; is_new_index_redirect=1; __auc=a840505817f796e92ca60ef1302; newUI=1; tw591__privacy_agree=1; __utma=82835026.1091730978.1647009760.1655302319.1655302319.1; __utmc=82835026; d161625dfbb5ac1ad88659a2e77c5b5a=1; statement-privacy=%7B%22userIds%22%3A%5B2847772%5D%2C%22isAgree%22%3Atrue%7D; _ga_ZDKGFY9EDM=GS1.1.1659841667.24.1.1659841669.0; user_index_role=2; _ga_8E3GTG4K5C=GS1.3.1693721113.3.0.1693721113.60.0.0; _ga_HCHGBJRJLT=GS1.3.1693721114.3.0.1693721114.0.0.0; _ga_RN6MJ068GP=GS1.3.1693721114.3.0.1693721114.0.0.0; urlJumpIp=7; _ga=GA1.4.1091730978.1647009760; __lt__cid=c6fecd93-a4fe-4bd2-9a03-eb094e6a5c06; _fbp=fb.2.1707442764740.512421703; last_search_type=2; _gid=GA1.3.352186247.1707621548; _gid=GA1.4.352186247.1707621548; __lt__sid=c41c57bf-9234deef; fcm_pc_token=eOP6xCIpSCH6t7cpihPLxR%3AAPA91bHhzP69uZzVGYRCcbgKTGo-TZuASrFr98Rj_zZDSbmIxKKUps9G0z6hZzvXdhFbgCPgQHwKM2uzC6CeQ2180wHID-whylG-MPfCQry1Sm_jXJqw6wyUtFY7xKxm75IIxRV2ZyHC; timeDifference=-1; XSRF-TOKEN=eyJpdiI6IlJPYnU2ck9mL0hUUFNyNzAzeWkxbEE9PSIsInZhbHVlIjoiWkhkL05GeGhZd1lCVFhhWFRGVG02UWhKTHN3QUQwdk10Mmdvb1loS0hablp4ejZJMkZVaGdHbytWdldlUUFwbE1xQUxidlRNNzV2bVc3NkFLdjhkUzBydWtkODU0UGJ1dzFWSEh4NElrS1M0MThnSTEwaEh6LzMzL296MHpmbWwiLCJtYWMiOiI3ZDMwOTUyOGRiMTJmODk1Zjg5MTI1ZDNiYjIyZTQ5OWQzOGZkY2FiYmViOTUxYjRhYTEwYTBjOGNmMTQzZDY2IiwidGFnIjoiIn0%3D; user_browse_recent=a%3A5%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14906927%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14901347%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A15015851%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14965859%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A13995175%3B%7D%7D; 591_new_session=eyJpdiI6ImFoTCttUUZuWlhScklxN2g2S1JWOVE9PSIsInZhbHVlIjoiNGVLWUpneUlBdGs5YmZpUkJvUGFUSVBpWkhZdFlWcjZkRVgrVzBsVnpLTWo0NVdkWkwvVmhRcDNaTEJqMFV5QkdQc2EzVmhCWUdXSFlMK2V3TDdFaUhuOUUrN3VIV3pFMzJiN284WXljQU95TWhLaThrSENOK2FnK3RMcGhHekciLCJtYWMiOiI0MDA2MWU5MjA0MmRiMzUxOWQyMzk4NTgxODQ1MGM3ZDI2ZGUyOWIwOWI1NDVkM2ZjYzcyMWEwNWNlMzYxZjM2IiwidGFnIjoiIn0%3D; _gat=1; _dc_gtm_UA-97423186-1=1; _ga_H07366Z19P=GS1.3.1707621548.13.1.1707626798.60.0.0; _ga=GA1.3.1091730978.1647009760; _gat_UA-97423186-1=1; _ga_HDSPSZ773Q=GS1.1.1707621548.43.1.1707626798.0.0.0',
    'X-Csrf-Token': token,
    'X-Requested-With': 'XMLHttpRequest'}
    response = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    return response.json()

# extract elements which we need
def get_591_page_data(js):
    # create emply dataframe
    df_house = global_api.TABLETITLE
    source = "591"

    # check the max row on this page
    max_row = len(js.get('data').get('house_list'))
    for i in range(0 , max_row):
        price     = js.get('data').get('house_list')[i].get('price') # get price (to check if it`s 預售屋)
        if price != 0:  # check if it`s 預售屋, if == 0, means it`s 預售屋, then ignore it
            title     = js.get('data').get('house_list')[i].get('title')
            community = js.get('data').get('house_list')[i].get('community_name')
            if community == '':  # 如果沒寫社區,則填入區域+街道
                section_name = js.get('data').get('house_list')[i].get('section_name')
                address      = js.get('data').get('house_list')[i].get('address')
                community    = section_name + '-' + address
            address   = js.get('data').get('house_list')[i].get('address')
            size_p    = js.get('data').get('house_list')[i].get('area')
            room_count= js.get('data').get('house_list')[i].get('room')
            price     = js.get('data').get('house_list')[i].get('price')
            unitprice = js.get('data').get('house_list')[i].get('unitprice')
            age       = js.get('data').get('house_list')[i].get('showhouseage')
            floor     = js.get('data').get('house_list')[i].get('floor')
            houseid   = js.get('data').get('house_list')[i].get('houseid')
            connection= 'https://sale.591.com.tw/home/house/detail/2/' + str(houseid) + '.html'
            # append these elements to dataframe
            df_house = pd.concat([df_house, pd.DataFrame([{'標的': '', '忽略': '', '標題': title, '社區': community, '坪數':size_p, '格局': room_count, '總價': price, '萬/坪': unitprice, '屋齡': age, '樓層': floor,'來源': source, '連結': connection}])], ignore_index=True)
    
    return df_house

# get total house counts
def get_591_total_row(js_response):
    return js_response.get('data').get('total')

def get_URL_wo_totalRows(url):
    url_wo_totalRows = ""

    # remove "&totalRows" in url
    if("totalRows" in url):
        url_wo_totalRows = url.split("&totalRows")[0]
    else:
        url_wo_totalRows = url
    
    return url_wo_totalRows

def main_get_591(area, url_from_ini):
    print("===== Start Parsing " + area + " houses from 591 =====")
    warnings.filterwarnings("ignore") 
    rs = requests.session()
    
    # url_from_ini     = global_api.getAreaIniInfo(area, "SOURCE_URL", "h591")
    df_591_total     = global_api.TABLETITLE
    first_row        = 0
    page             = 1
    url_wo_totalRows = get_URL_wo_totalRows(url_from_ini)
    token            = get_591_token(rs, url_wo_totalRows)
    XHRUrl_wo_totalRows = get_591_XHR_request_URL(url_wo_totalRows)
    total_rows       = -1
    total_page       = -1

    while True:
        js_response = ""
        
        if (page == 1):
            js_response = get_591_res(rs, token, XHRUrl_wo_totalRows)
            total_rows  =  get_591_total_row(js_response)
            total_page  = -(((-1) * int(total_rows)) // DEFAULT_ROWS_IN_591_PAGE)
            print("Total " + str(total_rows) + " houses, " + str(total_page) + " pages, Start parsing...")
        else:
            page_url     = url_wo_totalRows + '&firstRow='+ str(first_row) +'&totalRows=' + str(total_rows)
            XHR_page_url = get_591_XHR_request_URL(page_url)
            js_response  = get_591_res(rs, token, XHR_page_url)
        
        df_591_house = get_591_page_data(js_response)
        df_591_total = pd.concat([df_591_total, df_591_house], ignore_index=True)

        print("Parse page number: " + str(page) + "/" + str(total_page) + ' from 591 successfully')
        time.sleep(random.randint(2,6))

        # check whether is last page, if yes, exit the loop (30 houses on one page)
        if page >= -(((-1) * int(total_rows)) // DEFAULT_ROWS_IN_591_PAGE):
            print("Parsing " + area + " data from 591 successfully, Saving...")
            break
        first_row += DEFAULT_ROWS_IN_591_PAGE
        page      += 1

    return df_591_total



# import pandas as pd
# import requests
# from lxml import etree
# import os
# import datetime
# import time
# import json
# import random
# from bs4 import BeautifulSoup
# import warnings 
# import sqlite3
# from selenium.webdriver.common.by import By
# DEFAULT_ROWS_IN_591_PAGE = 30


# def getTotalRows(url):
#     url_wo_totalRows = ""
#     totalRows_str    = "Unknown"

#     # remove "&totalRows" in url
#     if("totalRows" in url):
#         url_wo_totalRows = url.split("&totalRows")[0]
#     else:
#         url_wo_totalRows = url
    
#     driver    = global_api.driverBySelenium(url_wo_totalRows)
#     totalRows = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[1]/div[1]/p/span')
#     totalRows_str = totalRows.text
#     driver.quit()

#     return url_wo_totalRows, totalRows_str


# def parseElementsByDriver(url, rows_in_page):
#     start_index = 3
#     end_index = 3 + rows_in_page
    
#     # create emply dataframe
#     df_house = global_api.TABLETITLE

#     driver = global_api.driverBySelenium(url)

#     for i in range(start_index, end_index):
#         xpath_title           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[1]/a'
#         xpath_community_type1 = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]/a'
#         xpath_community_type2 = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]/span'
#         xpath_section         = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]'
#         xpath_address         = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[2]'
#         xpath_size_p          = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[3]'
#         xpath_room_count      = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[2]'
#         xpath_price           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[2]/div[1]/em'
#         xpath_unitprice       = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[2]/div[3]'
#         xpath_age             = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[4]'
#         xpath_floor           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[6]'

#         try:
#             title = driver.find_element(By.XPATH, xpath_title)
#         except:
#             print("item:" + str(i) + " get title error, skip!")
#             continue

#         # if no community, than return section+address
#         try:
#             community = driver.find_element(By.XPATH, xpath_community_type1).text
#         except:
#             try:
#                 community = driver.find_element(By.XPATH, xpath_community_type2).text
#             except:
#                 try:
#                     community = driver.find_element(By.XPATH, xpath_section).text
#                     community += driver.find_element(By.XPATH, xpath_address).text
#                 except:
#                     continue

#         try:
#             size_p = driver.find_element(By.XPATH, xpath_size_p).text
#         except:
#             size_p = "Unknown"

#         try:
#             room_count = driver.find_element(By.XPATH, xpath_room_count).text
#         except:
#             room_count = "Unknown"

#         try:
#             price = driver.find_element(By.XPATH, xpath_price).text
#         except:
#             price = "Unknown"

#         try:
#             unitprice = driver.find_element(By.XPATH, xpath_unitprice).text
#         except:
#             unitprice = "Unknown"

#         try:
#             age = driver.find_element(By.XPATH, xpath_age).text
#         except:
#             age = "Unknown"

#         try:
#             floor = driver.find_element(By.XPATH, xpath_floor).text
#         except:
#             floor = "Unknown"

#         # print(title.text + community + size_p + room_count + price + unitprice + age + floor + title.get_attribute('href'))

#         # "concat" these elements to dataframe, DO NOT USE "APPEND" it`s old style
#         df_house = pd.concat([df_house, pd.DataFrame([{'標題': title.text, '社區': community, '坪數':size_p, '格局': room_count, '總價': price, '萬/坪': unitprice, '屋齡': age, '樓層': floor, '連結': title.get_attribute('href')}])], ignore_index=True)

#     # Close the browser window
#     driver.quit()

#     return df_house



# def main_get_591():
#     print("===== Start Parsing 591 houses =====")
#     url           = global_api.getIniInfo("SOURCE_URL", "h591")
#     df_591_house  = global_api.TABLETITLE
#     is_first_page = True
#     first_row     = 0
#     rows_in_page  = DEFAULT_ROWS_IN_591_PAGE
#     page          = 1
#     url_wo_totalRows, total_rows = getTotalRows(url)
#     if (total_rows == "Unknown"):
#         return
#     else:
#         print('591 has totally: '+ str(total_rows) + ' house, start parsing...')

#     total_page    = -(((-1) * int(total_rows)) // DEFAULT_ROWS_IN_591_PAGE)

#     while True:
#         # get rows of last page
#         if ((page * DEFAULT_ROWS_IN_591_PAGE) > int(total_rows)):
#             rows_in_page = (int(total_rows) % DEFAULT_ROWS_IN_591_PAGE)

#         # debuggg
#         print("parsing 591 page: " + str(page) + "/" + str(total_page) + " rows: " + str(rows_in_page))

#         urlWithRows = url_wo_totalRows + '&firstRow='+ str(first_row) +'&totalRows=' + str(total_rows)
#         df_house = parseElementsByDriver(urlWithRows, rows_in_page)

#         df_591_house = pd.concat([df_591_house, df_house], ignore_index=True)
#         # df_591_house = df_591_house.append(df_house, ignore_index = True)
#         print("Parse page number: " + str(page) + ' from 591 successfully')
#         time.sleep(random.randint(8,12))

#         # check whether is last page, if yes, exit the loop (30 houses on one page)
#         if page >= -(((-1) * int(total_rows)) // 30):
#             print('Parsing 591 data successfully, Saving...')
#             break
#         first_row += 30
#         page += 1

#     return df_591_house

################################################
###########   Sinyi crawler area   #############
################################################
import pandas as pd
import requests
from lxml import etree
import os
import random
import datetime
import time

# get Sinyi page response
def get_sinyi_res(url):
    print(url)
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    response = requests.get(url, headers = headers)
    return response

# get Sinyi total data size from the response in first page
def get_sinyi_total_row(response):
    tree      = etree.HTML(response.text)
    total_row = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[1]/div[1]/div[2]/text()[2]')
    return total_row[0]

# parsing house data and save in dataframe 'df_rakuya_house'
def sinyi_crawler(response):
    tree                 = etree.HTML(response.text)
    df_rakuya_house      = global_api.TABLETITLE
    
    # parsing 1 to 20 rows data since only 20 rows in one page
    for i in range(1,21):
        title            = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[2]/div[1]/div[1]')
        if title == []:          # If title is emply, means it`s the end
            break     
        community        = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[1]/div[1]/div[1]/div/a/span/text()')
        if community     == []:  # '社區' data on some house would not shown, if no shown, return empty ['']
            community     = ['']
        size             = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[2]/div[2]/div[1]/div[2]/span[1]/text()[2]')
        room             = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[1]/div[1]/div[3]/span[3]')
        price            = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[2]/div[1]/div[2]/div/span[2]')
        if price[0].text == '萬': # if house have two price (original price and decreasing price), then get the last one
            price         = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[2]/div[1]/div[2]/div/span[1]')
        unit_price       = float(price[0].text.replace(',','')) / float(size[0]) # Calculation
        short_unit_price = "{:.2f}".format(unit_price)  # only show two decimal (小數點兩位)
        age              = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[2]/div[2]/div[1]/div[2]/span[3]')
        floor            = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[1]/div[1]/div[3]/span[4]/text()[1]')
        total_floor      = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a/div/div[2]/div[1]/div[1]/div[3]/span[4]/text()[3]')
        href_line        = tree.xpath('//*[@id="__next"]/div/div/span/div[3]/div/div/div[3]/div[2]/div[2]/div['+ str(i) +']/a')
        href             = href_line[0].get("href")
        connection       = 'https://www.sinyi.com.tw' + str(href)

        # append these elements to dataframe
        df_rakuya_house  = df_rakuya_house.append({'標的': '-1', '忽略': '-1', '標題': title[0].text, '社區': community[0], '坪數':size[0], '格局': room[0].text, '總價': price[0].text, '萬/坪': short_unit_price, '屋齡': age[0].text, '樓層': floor[0], '連結': connection}, ignore_index = True)
    return df_rakuya_house


def main_get_sinyi():
    url             = global_api.getIniInfo("SOURCE_URL", "sinyi")
    df_sinyi_house  = global_api.TABLETITLE
    page_number     = 1
    is_first_page   = True

    while True:
        # add page data to the url
        url_add_page = url + str(page_number)
        response     = get_sinyi_res(url_add_page)

        # total row is shown when get first page
        if is_first_page == True:
            total_row = get_sinyi_total_row(response)
            print('Sinyi total: '+ str(total_row) + ' house, start parsing...')
            is_first_page = False
        
        df_house       = sinyi_crawler(response)
        df_sinyi_house = df_sinyi_house.append(df_house, ignore_index = True)
        print("Parse page number: " + str(page_number) + ' from Sinyi successfully')
        
        # check whether is last page, if yes, exit the loop (30 houses on one page)
        if page_number >= -(((-1) * int(total_row)) // 20):
            print('Parsing Sinyi data successfully, Saving...')
            break
        
        time.sleep(random.randint(8,12))
        page_number += 1
    return df_sinyi_house

################################################
###########    save data area    ###############
################################################

# def save_to_DB(area, df_house):
#     tableName = str(area)
#     global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, area, df_rakuya_house)

    # if (df_house.empty):
    #     print("df_house: " + source + " is empty, skip save to DB!")
    #     return None

    # # build main dir
    # main_dir = os.path.join('function', 'data', 'crawling')
    # if not os.path.isdir(main_dir):
    #     os.makedirs(main_dir)

    # # open splite3 and save
    # now        = datetime.datetime.now()
    # conn       = sqlite3.connect(main_dir + '\\' + now.strftime('%Y%m%d') + '.sqlite3')
    # table_name = str(area) + '_' + str(source) +'_house'
    # df_house.to_sql( table_name, conn, if_exists = 'replace')
    # df_from_DB = pd.read_sql('select * from ' + table_name, conn)
    # print('Save ' + str(table_name) + ' successfully')
    # return None

################################################
################    Main    ####################
################################################

def crawl_and_save():
    enableAreas = global_api.getEnableAreas()
    print("Total " + str(len(enableAreas)) + " areas: " + ', '.join(enableAreas))

    for area in enableAreas:
        # sources      = global_api.getAreaIniInfo(area, "SOURCES", "source").split(",")
        return_value = global_api.getAreaIniInfo(area, "SOURCE_URL", "", ";")
        if (return_value != None):
            url_list = return_value.split(";")

            for each_url in url_list:
                if("sale.591.com.tw" in each_url):
                    df_591_house = main_get_591(area, each_url)
                    global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, area, df_591_house)

                elif("www.rakuya.com.tw" in each_url):
                    df_rakuya_house = get_rakuya(area, each_url)
                    global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, area, df_rakuya_house, "append")
                
                # elif(source == 'sinyi'):
                    # df_sinyi_house = main_get_sinyi()
                    # global_api.save_to_DB(global_api.SQLiteDIR, global_api.SQLiteFILENAME, area, df_sinyi_house, "append")
        else:
            return False

    return True

if __name__ == '__main__':
    crawl_and_save()

