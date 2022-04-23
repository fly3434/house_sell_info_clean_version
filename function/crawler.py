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

def get_rakuya():
    df_rakuya_house = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
    page_count      = 1
    last_house      = False

    while last_house == False: # check if this is last page
        headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        url  = 'https://www.rakuya.com.tw/sell/result?city=8&zipcode=407%2C406%2C428&price=800~1250&room=2&floor=5~99&age=~16&other=P&sort=11&browsed=0&page=' + str(page_count)
        # url  = "https://www.rakuya.com.tw/sell/result?city=8&zipcode=407%2C406&price=500~1500&room=2%2C3&floor=7~11%2C12~&age=~15&other=P&sort=11&browsed=0&page=" + str(page_count)
        res  = requests.get(url, headers = headers)
        tree = etree.HTML(res.text)   

        # get website element
        href = tree.xpath('//a[contains(@class,"browseItemDetail")]/@href') 
        
        # parsing from first house to last house (20) on one page, get data by xpath
        for j in range(1,20):
            title          = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[1]/div/div/span[1]')
            if title == []:  # if title = empty, means there is no data behind it
                last_house = True
                break
            if title[0].text == "新上架":  # if the title is "新上架", parsing the second title
                title = tree.xpath(' /html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[1]/span[2]')
            
            addr           = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/h2/span/span[1]')
            siz            = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/ul[1]/li[2]')
            distri         = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/ul[1]/li[3]')
            price          = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[3]/span[1]/text()')
            if price == []:
                price = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[3]/span[2]/text()')

            price_per_unit = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/div[3]/span[2]')
            year           = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/ul[1]/li[4]')
            if '樓' in year[0].text:   # sometimes there is no house age data, the 樓層 will shift to front
                house_year = None
                floor      = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/ul[1]/li[4]')
            else:
                house_year = year[0].text
                floor      = tree.xpath('/html/body/div[7]/div[2]/div[1]/div[4]/section['+ str(j) +']/a/div[2]/ul[1]/li[5]')        
            
            # save these data to dataframe
            df_rakuya_house = df_rakuya_house.append({'標題': title[0].text, '社區': addr[0].text, '坪數':siz[0].text, '格局': distri[0].text, '總價': price[0], '萬/坪': price_per_unit[0].text, '屋齡': house_year, '樓層': floor[0].text, '連結': href[j-1]}, ignore_index = True)

        print('get page number: ' + str(page_count) + ' from rakuya successfully')
        page_count += 1
        time.sleep(random.randint(8,12))
    print('Parsing rakuya successfully, Saving...')
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

warnings.filterwarnings("ignore") 
rs = requests.session()

# get CSRF token, then I can parsing house data from sub_url
def get_591_token(): 
    headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept':'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cookie':'webp=1; PHPSESSID=irbfpo26jv46emn3h3ri9gqn55; urlJumpIp=8; urlJumpIpByTxt=%E5%8F%B0%E4%B8%AD%E5%B8%82; T591_TOKEN=irbfpo26jv46emn3h3ri9gqn55; _ga=GA1.3.452477559.1615821301; _gid=GA1.3.1762208037.1615821301; _gat=1; _ga=GA1.4.452477559.1615821301; _gid=GA1.4.1762208037.1615821301; _dc_gtm_UA-97423186-1=1; tw591__privacy_agree=0; 591_new_session=eyJpdiI6IkVMN0NyZXpmcUplY3I4Y012NGs4Wnc9PSIsInZhbHVlIjoiblVsMmUwTXpzbVpPY0I5YU5kQzBPclpEblI5aG9tc3o0UWg1blF4MHErSXdXZ1hJSkc4aTdPWmFNVENIeThXaVhWdnU5aTdkWmtWcUp0RmFsYmlGQkE9PSIsIm1hYyI6ImY0MWNlODJlOTEyN2MyYjAxZDZlNDY5OGQ3N2E3MDhlZmI0ZmRkNzNlZjBmOGMxNTIwYmQ4ZTkzNWVjNWE5YjQifQ%3D%3D',
    }
    url = 'https://sale.591.com.tw/?shType=list&regionid=8&section=104,103,117&price=800$_1250$&pattern=2&houseage=$_16$&floor=5$_100$&direction=4,7,2,5,3,6'
    # url = 'https://sale.591.com.tw/?shType=list&section=104,103&regionid=8&kind=9&price=900$_1500$&pattern=2,3&houseage=$_15$&floor=6$_100$&direction=2,3,6,5,4,7'
    home_res = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
    # requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    soup = BeautifulSoup(home_res.text, 'lxml')
    csrf_token = soup.find("meta", dict(name="csrf-token"))['content']

    return csrf_token

# data processing, input url and token, then house dataframe and total rows will given
def data_classify(token, url):
    # get page response
    def get_591_res(token, url):
        headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cookie':'webp=1; PHPSESSID=irbfpo26jv46emn3h3ri9gqn55; urlJumpIp=8; urlJumpIpByTxt=%E5%8F%B0%E4%B8%AD%E5%B8%82; T591_TOKEN=irbfpo26jv46emn3h3ri9gqn55; _ga=GA1.3.452477559.1615821301; _gid=GA1.3.1762208037.1615821301; _gat=1; _ga=GA1.4.452477559.1615821301; _gid=GA1.4.1762208037.1615821301; _dc_gtm_UA-97423186-1=1; tw591__privacy_agree=0; 591_new_session=eyJpdiI6IkVMN0NyZXpmcUplY3I4Y012NGs4Wnc9PSIsInZhbHVlIjoiblVsMmUwTXpzbVpPY0I5YU5kQzBPclpEblI5aG9tc3o0UWg1blF4MHErSXdXZ1hJSkc4aTdPWmFNVENIeThXaVhWdnU5aTdkWmtWcUp0RmFsYmlGQkE9PSIsIm1hYyI6ImY0MWNlODJlOTEyN2MyYjAxZDZlNDY5OGQ3N2E3MDhlZmI0ZmRkNzNlZjBmOGMxNTIwYmQ4ZTkzNWVjNWE5YjQifQ%3D%3D',
        'X-CSRF-TOKEN': token }
        response = rs.get( url, stream = True, verify = False, headers = headers, timeout=None )
        return response

    # cutting content, due to html content mixing in json format content
    def res_classify(response):
        response.encoding = 'unicode-escape'
        response.encoding = 'utf-8'
        content           = response.text

        # exclude the content that can`t be read by json format
        content_cut1   = content.replace('"page":"<','"page":"CUTTING_AREA<')
        content_cut2   = content_cut1.replace('>","total"','>CUTTING_AREA","total"')
        content_cutted = content_cut2.split('CUTTING_AREA',2)
        content_join   = content_cutted[0] + content_cutted[2]
        js             = json.loads(content_join) # json format, house details
        html_data      = content_cutted[1]        # html format, contain total house counts
        return js, html_data

    # extract elements which we need
    def get_591_page_data(js):
        # create emply dataframe
        df_house = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
        
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
                area      = js.get('data').get('house_list')[i].get('area')
                room      = js.get('data').get('house_list')[i].get('room')
                price     = js.get('data').get('house_list')[i].get('price')
                unitprice = js.get('data').get('house_list')[i].get('unitprice')
                age       = js.get('data').get('house_list')[i].get('showhouseage')
                floor     = js.get('data').get('house_list')[i].get('floor')
                houseid   = js.get('data').get('house_list')[i].get('houseid')
                connection= 'https://sale.591.com.tw/home/house/detail/2/' + str(houseid) + '.html'
                # append these elements to dataframe
                df_house  = df_house.append({'標題': title, '社區': community, '坪數':area, '格局': room, '總價': price, '萬/坪': unitprice, '屋齡': age, '樓層': floor, '連結': connection}, ignore_index = True)
        return df_house

    # get total house counts
    def get_591_total_row(html_data):
        soup      = BeautifulSoup(html_data, 'html.parser')
        a_tags    = soup.find_all('a')
        total_row = a_tags[1].get('data-total').replace('\\"','') # data contain \\", ignore it
        return total_row

    response              = get_591_res(token, url)
    js_details, html_data = res_classify(response)
    df_house              = get_591_page_data(js_details)
    total_row             = get_591_total_row(html_data)
    
    return df_house, total_row

def main_get_591():
    token         = get_591_token()
    first_url     = 'https://sale.591.com.tw/home/search/list?type=2&shType=list&regionid=8&section=104,103,117&price=800$_1250$&pattern=2&houseage=$_16$&direction=4,7,2,5,3,6&floor=5$_100'
    df_591_house  = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
    is_first_page = True
    first_row     = 0
    page          = 1

    while True:
        if is_first_page == True:
            df_house, total_row = data_classify(token, first_url)
            is_first_page = False
            print('591 total: '+ str(total_row) + ' house, start parsing...')
        else:
            # add page data to the url
            page_url = first_url + '&firstRow='+ str(first_row) +'&totalRows=' + str(total_row)
            df_house, total_row_ignr = data_classify(token, page_url)

        df_591_house = df_591_house.append(df_house, ignore_index = True)
        print("Parse page number: " + str(page) + ' from 591 successfully')
        time.sleep(random.randint(8,12))

        # check whether is last page, if yes, exit the loop (30 houses on one page)
        if page >= -(((-1) * int(total_row)) // 30):
            print('Parsing 591 data successfully, Saving...')
            break
        first_row += 30
        page += 1

    return df_591_house

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
    df_rakuya_house      = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
    
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
        df_rakuya_house  = df_rakuya_house.append({'標題': title[0].text, '社區': community[0], '坪數':size[0], '格局': room[0].text, '總價': price[0].text, '萬/坪': short_unit_price, '屋齡': age[0].text, '樓層': floor[0], '連結': connection}, ignore_index = True)
    return df_rakuya_house


def main_get_sinyi():
    url             = 'https://www.sinyi.com.tw/buy/list/800-1250-price/plane-yesparking/16-down-year/2-2-room/1f-4f-sfroof-exclude/5-100-floor/Taichung-city/406-407-428-zip/Taipei-R-mrtline/03-mrt/yesparking/default-desc/'
    df_sinyi_house  = pd.DataFrame(columns=['標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','連結'])
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

def save_to_DB(source, df_house):
    # build main dir
    main_dir = os.path.join('function', 'data', 'crawling')
    if not os.path.isdir(main_dir):
        os.makedirs(main_dir)

    # open splite3 and save
    now = datetime.datetime.now()
    conn = sqlite3.connect(main_dir + '\\' + now.strftime('%Y%m%d') + '.sqlite3')
    df_house.to_sql( str(source) +'_daily_house', conn, if_exists = 'replace')
    df_from_DB = pd.read_sql('select * from ' + str(source) +'_daily_house', conn)
    print('Save ' + str(source) + ' house successfully')
    return None

################################################
################    Main    ####################
################################################

def crawl_and_save():
    # crawl rakuya and save
    df_rakuya_house = get_rakuya()
    save_to_DB('rakuya', df_rakuya_house)

    # crawl 591 and save
    df_591_house = main_get_591()
    save_to_DB('h591', df_591_house)

    # crawl Sinyi and save
    df_sinyi_house = main_get_sinyi()
    save_to_DB('sinyi', df_sinyi_house)

    return None

if __name__ == '__main__':
    crawl_and_save()

