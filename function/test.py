from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
import global_api
import time
from bs4 import BeautifulSoup
import requests
import warnings 
import pandas as pd


def parseElementByDriver():
    # url = "https://sale.591.com.tw/?shType=list&section=80,81&regionid=7&kind=0&pattern=2&shape=2,1&houseage=0_5,5_10&floor=5$_100$&totalRows=148&firstRow=0"
    url = "https://sale.591.com.tw/?shType=list&section=80,81&regionid=7&kind=0&pattern=2&shape=2,1&houseage=0_5,5_10&floor=5$_100$"

    # # Set up the Chrome driver
    # # driver from "https://googlechromelabs.github.io/chrome-for-testing/#stable"
    # driver_path = "C:/Users/s7063/GoogleDrive/house/Zhunan/Code/house_sell_info_clean_version/house_sell_info_clean_version/datas/chromedriver/win64_121.0.6167.85/chromedriver.exe"
    # chrome_service = ChromeService(driver_path)
    # driver = webdriver.Chrome(service=chrome_service)

    # # Open the webpage
    # driver.get(url)

    # # Wait for dynamic content to load (you may need to adjust the timeout)
    # driver.implicitly_wait(10)

    driver = global_api.driverBySelenium(url)

    dynamic_content = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[1]/div[1]/p/span')
    print(dynamic_content.text)

    # Extract content using Selenium's find_element method
    # dynamic_content = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[2]/div[3]/div[1]/a')
                                                    #  //*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[1]/a
                                                    #  //*[@id="app"]/div[4]/div[2]/section/div[2]/div[32]/div[3]/div[1]/a

########################################
    for i in range(3, 4):
        xpath_title           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[1]/a'
        xpath_community_type1 = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]/a'
        xpath_community_type2 = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]/span'
        xpath_section         = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[1]'
        xpath_address         = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[3]/span[2]'
        xpath_size_p          = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[3]'
        xpath_room_count      = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[2]'
        xpath_price           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[2]/div[1]/em'
        xpath_unitprice       = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[2]/div[3]'
        xpath_age             = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[4]'
        xpath_floor           = '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[' + str(i) +']/div[3]/div[2]/span[6]'

        try:
            title = driver.find_element(By.XPATH, xpath_title)
        except:
            print("item:" + str(i) + " get title error")
            continue

        # if no community, than return section+address
        try:
            community = driver.find_element(By.XPATH, xpath_community_type1).text
        except:
            try:
                community = driver.find_element(By.XPATH, xpath_community_type2).text
            except:
                try:
                    community = driver.find_element(By.XPATH, xpath_section).text
                    community += driver.find_element(By.XPATH, xpath_address).text
                except:
                    continue

        try:
            size_p = driver.find_element(By.XPATH, xpath_size_p).text
        except:
            size_p = "Unknown"

        try:
            room_count = driver.find_element(By.XPATH, xpath_room_count).text
        except:
            room_count = "Unknown"

        try:
            price = driver.find_element(By.XPATH, xpath_price).text
        except:
            price = "Unknown"

        try:
            unitprice = driver.find_element(By.XPATH, xpath_unitprice).text
        except:
            unitprice = "Unknown"

        try:
            age = driver.find_element(By.XPATH, xpath_age).text
        except:
            age = "Unknown"

        try:
            floor = driver.find_element(By.XPATH, xpath_floor).text
        except:
            floor = "Unknown"

        # print("title: " + title.text + " ,community: " + community.text + " , url: " + title.get_attribute('href'))
        print(title.text,community,size_p,room_count,price,unitprice,age,floor)
        # print(community.text)
######################################

    # # community
    # community = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[2]/div[3]/div[3]/span[1]/a')

    # try:
    #     community2 = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[1]/span').text
    # except:
    #     community2 =  driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[1]').text
    #     community2 += driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[2]').text



    # community = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[2]/div[3]/div[3]/span[1]/a')
    # community2 = driver.find_element(By.XPATH, '//*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[1]/a')
    # //*[@id="app"]/div[4]/div[2]/section/div[2]/div[2]/div[3]/div[3]/span[1]/a
    # //*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[1]
    # //*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[2]
    # //*[@id="app"]/div[4]/div[2]/section/div[2]/div[3]/div[3]/div[3]/span[2]
    # print(dynamic_content.text)
    # print(dynamic_content.get_attribute('href'))
    # print(community.text)
    # print(community2)

    # Close the browser window
    driver.quit()




# get CSRF token, then I can parsing house data from sub_url
def get_591_token(rs, url): 
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cookie':'webp=1; PHPSESSID=irbfpo26jv46emn3h3ri9gqn55; urlJumpIp=8; urlJumpIpByTxt=%E5%8F%B0%E4%B8%AD%E5%B8%82; T591_TOKEN=irbfpo26jv46emn3h3ri9gqn55; _ga=GA1.3.452477559.1615821301; _gid=GA1.3.1762208037.1615821301; _gat=1; _ga=GA1.4.452477559.1615821301; _gid=GA1.4.1762208037.1615821301; _dc_gtm_UA-97423186-1=1; tw591__privacy_agree=0; 591_new_session=eyJpdiI6IkVMN0NyZXpmcUplY3I4Y012NGs4Wnc9PSIsInZhbHVlIjoiblVsMmUwTXpzbVpPY0I5YU5kQzBPclpEblI5aG9tc3o0UWg1blF4MHErSXdXZ1hJSkc4aTdPWmFNVENIeThXaVhWdnU5aTdkWmtWcUp0RmFsYmlGQkE9PSIsIm1hYyI6ImY0MWNlODJlOTEyN2MyYjAxZDZlNDY5OGQ3N2E3MDhlZmI0ZmRkNzNlZjBmOGMxNTIwYmQ4ZTkzNWVjNWE5YjQifQ%3D%3D',
    }
    # url = 'https://sale.591.com.tw/?shType=list&regionid=8&section=104,103,117&price=800$_1250$&pattern=2&houseage=$_16$&floor=5$_100$&direction=4,7,2,5,3,6'
    # url = 'https://sale.591.com.tw/?shType=list&section=104,103&regionid=8&kind=9&price=900$_1500$&pattern=2,3&houseage=$_15$&floor=6$_100$&direction=2,3,6,5,4,7'
    home_res = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    # requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    soup = BeautifulSoup(home_res.text, 'lxml')
    csrf_token = soup.find("meta", dict(name="csrf-token"))['content']

    return csrf_token

def getTotalRows(url):
    url_wo_totalRows = ""
    totalRows        = "Unknown"

    # remove "&totalRows" in url
    if("totalRows" in url):
        url_wo_totalRows = url.split("&totalRows")[0]
    else:
        url_wo_totalRows = url

    warnings.filterwarnings("ignore") 
    rs      = requests.session()

    token = get_591_token(rs, url)
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cookie':'webp=1; PHPSESSID=irbfpo26jv46emn3h3ri9gqn55; urlJumpIp=8; urlJumpIpByTxt=%E5%8F%B0%E4%B8%AD%E5%B8%82; T591_TOKEN=irbfpo26jv46emn3h3ri9gqn55; _ga=GA1.3.452477559.1615821301; _gid=GA1.3.1762208037.1615821301; _gat=1; _ga=GA1.4.452477559.1615821301; _gid=GA1.4.1762208037.1615821301; _dc_gtm_UA-97423186-1=1; tw591__privacy_agree=0; 591_new_session=eyJpdiI6IkVMN0NyZXpmcUplY3I4Y012NGs4Wnc9PSIsInZhbHVlIjoiblVsMmUwTXpzbVpPY0I5YU5kQzBPclpEblI5aG9tc3o0UWg1blF4MHErSXdXZ1hJSkc4aTdPWmFNVENIeThXaVhWdnU5aTdkWmtWcUp0RmFsYmlGQkE9PSIsIm1hYyI6ImY0MWNlODJlOTEyN2MyYjAxZDZlNDY5OGQ3N2E3MDhlZmI0ZmRkNzNlZjBmOGMxNTIwYmQ4ZTkzNWVjNWE5YjQifQ%3D%3D',
        'X-CSRF-TOKEN': token }
    response = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    response.encoding = 'unicode-escape'
    response.encoding = 'utf-8'
    content           = response.text
    
    if("totalRows" in content):
        totalRows = content.split("totalRows=")[1].split("&amp;")[0]
    
    return totalRows

def dataFrameTest():
    df_house = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
    # df_house = df_house.append({'標題': "AA", '社區': "AA", '坪數': "AA", '格局': "AA", '總價': "AA", '萬/坪': "AA", '屋齡': "AA", '樓層': "AA", '連結': "AA"})
    df_house = pd.concat([df_house, pd.DataFrame([{'標題': "AA", '社區': "AA", '坪數': "AA", '格局': "AA", '總價': "AA", '萬/坪': "AA", '屋齡': "AA", '樓層': "AA", '連結': "AA"}])], ignore_index=True)
    df_house = pd.concat([df_house, pd.DataFrame([{'標題': "AA", '社區': "AA", '坪數': "AA", '格局': "AA", '總價': "AA", '萬/坪': "AA", '屋齡': "AA", '樓層': "AA", '連結': "AA"}])], ignore_index=True)

    print(df_house)

def math():
    total_rows = 105
    page = 3
    DEFAULT_ROWS_IN_591_PAGE = 30

    test = (int(total_rows) - (page * DEFAULT_ROWS_IN_591_PAGE)) // DEFAULT_ROWS_IN_591_PAGE

    print(test)

def getRes():
    url = 'https://www.sinyi.com.tw/buy/list/60-13000-price/tower-other-plane-yesparking/2-3-room/1f-4f-exclude/5-100-floor/Taichung-city/433-435-zip/default-desc/1'
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    verify_path = requests.certs.where()
    response = requests.get(url, headers = headers, verify=verify_path)
    return response

def getDynamicPage():
    url = "https://sale.591.com.tw/home/search/list?type=2&shType=list&regionid=7&section=80,81&kind=0&shape=2,1&pattern=2&houseage=0_5,5_10&floor=5$_100$&firstRow=0&totalRows=100&timestamp=1707625315133&recom_community=1"
    # url = "https://api.591.com.tw/api/tools/getTimestamp"
    rs      = requests.session()

    # token = get_591_token(rs, url)
    token = "hz3QGL3SJhyvdKDXyKCIaZpX8gvjL4ASDihpM1oQ"

    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cookie':'webp=1; PHPSESSID=ek8g1s3hdl4lddooptfrrq5cg1; T591_TOKEN=ek8g1s3hdl4lddooptfrrq5cg1; is_new_index=1; is_new_index_redirect=1; __auc=a840505817f796e92ca60ef1302; newUI=1; tw591__privacy_agree=1; __utma=82835026.1091730978.1647009760.1655302319.1655302319.1; __utmc=82835026; d161625dfbb5ac1ad88659a2e77c5b5a=1; statement-privacy=%7B%22userIds%22%3A%5B2847772%5D%2C%22isAgree%22%3Atrue%7D; _ga_ZDKGFY9EDM=GS1.1.1659841667.24.1.1659841669.0; user_index_role=2; _ga_8E3GTG4K5C=GS1.3.1693721113.3.0.1693721113.60.0.0; _ga_HCHGBJRJLT=GS1.3.1693721114.3.0.1693721114.0.0.0; _ga_RN6MJ068GP=GS1.3.1693721114.3.0.1693721114.0.0.0; urlJumpIp=7; _ga=GA1.4.1091730978.1647009760; __lt__cid=c6fecd93-a4fe-4bd2-9a03-eb094e6a5c06; _fbp=fb.2.1707442764740.512421703; last_search_type=2; _gid=GA1.3.352186247.1707621548; _gid=GA1.4.352186247.1707621548; __lt__sid=c41c57bf-9234deef; fcm_pc_token=eOP6xCIpSCH6t7cpihPLxR%3AAPA91bHhzP69uZzVGYRCcbgKTGo-TZuASrFr98Rj_zZDSbmIxKKUps9G0z6hZzvXdhFbgCPgQHwKM2uzC6CeQ2180wHID-whylG-MPfCQry1Sm_jXJqw6wyUtFY7xKxm75IIxRV2ZyHC; timeDifference=-1; XSRF-TOKEN=eyJpdiI6IlJPYnU2ck9mL0hUUFNyNzAzeWkxbEE9PSIsInZhbHVlIjoiWkhkL05GeGhZd1lCVFhhWFRGVG02UWhKTHN3QUQwdk10Mmdvb1loS0hablp4ejZJMkZVaGdHbytWdldlUUFwbE1xQUxidlRNNzV2bVc3NkFLdjhkUzBydWtkODU0UGJ1dzFWSEh4NElrS1M0MThnSTEwaEh6LzMzL296MHpmbWwiLCJtYWMiOiI3ZDMwOTUyOGRiMTJmODk1Zjg5MTI1ZDNiYjIyZTQ5OWQzOGZkY2FiYmViOTUxYjRhYTEwYTBjOGNmMTQzZDY2IiwidGFnIjoiIn0%3D; user_browse_recent=a%3A5%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14906927%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14901347%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A15015851%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A14965859%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A4%3A%22type%22%3Bi%3A2%3Bs%3A7%3A%22post_id%22%3Bi%3A13995175%3B%7D%7D; 591_new_session=eyJpdiI6ImFoTCttUUZuWlhScklxN2g2S1JWOVE9PSIsInZhbHVlIjoiNGVLWUpneUlBdGs5YmZpUkJvUGFUSVBpWkhZdFlWcjZkRVgrVzBsVnpLTWo0NVdkWkwvVmhRcDNaTEJqMFV5QkdQc2EzVmhCWUdXSFlMK2V3TDdFaUhuOUUrN3VIV3pFMzJiN284WXljQU95TWhLaThrSENOK2FnK3RMcGhHekciLCJtYWMiOiI0MDA2MWU5MjA0MmRiMzUxOWQyMzk4NTgxODQ1MGM3ZDI2ZGUyOWIwOWI1NDVkM2ZjYzcyMWEwNWNlMzYxZjM2IiwidGFnIjoiIn0%3D; _gat=1; _dc_gtm_UA-97423186-1=1; _ga_H07366Z19P=GS1.3.1707621548.13.1.1707626798.60.0.0; _ga=GA1.3.1091730978.1647009760; _gat_UA-97423186-1=1; _ga_HDSPSZ773Q=GS1.1.1707621548.43.1.1707626798.0.0.0',
        'X-Csrf-Token': token,
        'X-Requested-With': 'XMLHttpRequest'}
    response = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    # response.encoding = 'unicode-escape'
    # response.encoding = 'utf-8'
    content           = response.json()

    print(content)

def dfTest():
    data = {'Name': ['Ankit', 'Amit', 
                 'Aishwarya', 'Priyanka'],
        'Age': [21, 19, 20, 18],
        'Stream': ['Math', 'Commerce', 
                   'Arts', 'Biology'],
        'Percentage': [88, 92, 95, 70]}
    df = pd.DataFrame(data, columns=['Name', 'Age',
                                 'Stream', 'Percentage'])
    print(df)
    new_df = pd.DataFrame(columns=['Name', 'Age',
                                 'Stream', 'Percentage'])
    for i in range(len(df)):
        # print(df.iloc[i])
        # new_df = pd.concat([new_df, df.loc[i]])
        new_df.loc[len(new_df.index)] = df.iloc[i]
    
    print(new_df)

def createFileFromTemplate():
    sources      = global_api.getAreaIniInfo("Qiaotou", "SOURCES", "source").split(",")
    return sources

# parseElementByDriver()

# url = "https://sale.591.com.tw/?shType=list&section=80,81&regionid=7&kind=0&pattern=2&shape=2,1&houseage=0_5,5_10&floor=5$_100$&totalRows=148&firstRow=0"
# print(getTotalRows(url))

# dataFrameTest()

# math()

# print(requests.certs.where())
# print(getRes())

# getDynamicPage()
    
# global_api.getEnableAreas()
        
# dfTest()
    
# ignore_words = global_api.getAreaIniInfo("Shalu", "BAD_COMMUNITIES", "").split(",")
# print(ignore_words)

# while("" in ignore_words): # remove empty
#     ignore_words.remove("")
# print(ignore_words)

print(createFileFromTemplate())