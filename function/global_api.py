import configparser
import os
import platform
import datetime
import pandas as pd
import sqlite3
import shutil
SQLiteDIR      = os.path.join('results', 'raws')
SQLiteFILENAME = datetime.datetime.now().strftime('%Y%m%d')
AREAINIDIR     = os.path.join("configs", "area_configs")
AREAINTTEMPNAME= "TEMPLATE.ini"
TABLETITLE     = pd.DataFrame(columns=['紀錄','標的','忽略','標題','社區','坪數','格局','總價','萬/坪','屋齡','樓層','來源','連結','有效期'])

def getEnableAreas():
    enable_areas_list = []
    fileAbDir         = os.path.abspath(os.path.dirname(__file__))
    AbsAreaIniDir     = os.path.join(fileAbDir, "..", AREAINIDIR)
    files_in_dir_list = os.listdir(AbsAreaIniDir)

    for fileName in files_in_dir_list:
        iniPath = os.path.join(fileAbDir, "..", "configs", "area_configs", fileName)
        if ("desktop.ini" in iniPath): # this ini is created by system
            continue
        if not (".ini" in iniPath): # only get *.ini
            continue
        config = configparser.RawConfigParser()
        config.read(iniPath, encoding="utf-8")
        if (config["SETTINGS"]["Is_Enable"].lower() == "true" or config["SETTINGS"]["Is_Enable"].lower() == "1"):
            enable_areas_list.append(fileName.replace(".ini",""))
    return enable_areas_list

def getAreaIniInfo(area, section, key = "", splitChar = ","):
    fileName = area + ".ini"
    
    # get ini file path
    fileAbDir = os.path.abspath(os.path.dirname(__file__))
    iniPath   = os.path.join(fileAbDir, "..", AREAINIDIR, fileName)

    config = configparser.RawConfigParser()

    if not os.path.exists(iniPath):
        print(fileName + " doesn`t exists in configs\\area_configs folder! Copy One for you!")
        templatePath = os.path.join(fileAbDir, "..", AREAINIDIR, AREAINTTEMPNAME)
        shutil.copy(templatePath, iniPath)
        config.read(iniPath)
        config.set('SETTINGS', 'Is_Enable','true')

        with open(iniPath, 'w') as configfile:
            config.write(configfile, space_around_delimiters=False)

        return "None"
    else:
        config.read(iniPath, encoding="utf-8")

        if(key.lower() == "all" or key == ""): # if key isn`t specified, return all words in all keys
            return_str = ""
            for eachKey in config[section]:
                return_str += config[section][eachKey] + splitChar
            return return_str
        else:                                  # only return [section][key]
            return config[section][key]

def getSettingIniInfo(section, key):
    # get ini file path
    fileAbDir = os.path.abspath(os.path.dirname(__file__))
    inipath   = os.path.join(fileAbDir, "..", "configs", "settings.ini")

    if not os.path.exists(inipath):
        print("'settings.ini' doesn`t exists in configs folder! Please check!")
    
    config = configparser.ConfigParser()
    config.read(inipath, encoding="utf-8")

    return config[section][key]

def save_to_DB(desDir, fileName, tableName, dataFrame, if_exists = 'replace'):
    if (dataFrame.empty):
        print("dataFrame: " + tableName + " is empty, skip save to DB!")
        return None

    # build destination dir
    if not os.path.isdir(desDir):
        os.makedirs(desDir)

    # open splite3 and save
    sqlitePath = os.path.join(str(desDir), str(fileName)) + '.sqlite3'
    conn       = sqlite3.connect(sqlitePath)
    dataFrame.to_sql( tableName, conn, if_exists = if_exists, index=False) # if_exists=replace, append
    df_from_DB = pd.read_sql('select * from ' + tableName, conn)
    print('Save ' + str(tableName) + ' successfully')
    return None

# def read_from_DB(desDir, fileName, tableName):
#     # open splite3 and save
#     now        = datetime.datetime.now()
#     conn       = sqlite3.connect(str(desDir) + '\\' + str(fileName) + '.sqlite3')
#     dataFrame.to_sql( tableName, conn, if_exists = 'replace')
#     df_from_DB = pd.read_sql('select * from ' + tableName, conn)
#     print('Save ' + str(tableName) + ' successfully')
#     return None

# def driverBySelenium(url):
#     driverVer = "121.0.6167.85"
#     # get chrome version
#     # https://gist.github.com/primaryobjects/d5346bf7a173dbded1a70375ff7461b4

#     # get chromedriver file path
#     fileAbDir = os.path.abspath(os.path.dirname(__file__))
#     driverPath = ""

#     if (getPlatformInfo() == "mac-arm64"):
#         driverPath= os.path.join(fileAbDir, "..", "datas", "chromedriver", "mac-arm64_" + driverVer, "chromedriver")
#     else: # windows
#         driverPath= os.path.join(fileAbDir, "..", "datas", "chromedriver", "win64_" + driverVer, "chromedriver.exe")

#     # driver can get from "https://googlechromelabs.github.io/chrome-for-testing/#stable"

#     if not os.path.exists(driverPath):
#         print("'chromedriver.exe' doesn`t exists in datas folder! Please check!")
    
#     chrome_service = ChromeService(driverPath)
#     driver = webdriver.Chrome(service=chrome_service)

#     # sepress the warning
#     chrome_options=webdriver.ChromeOptions() 
#     chrome_options.add_experimental_option('excludeSwitch',['enable-logging'])
#     chrome_options.add_argument('--log-level=3') 

#     # Open the webpage
#     driver.get(url)

#     # set larger window size to show more details on window
#     driver.set_window_size(1920, 1080)

#     # Wait for dynamic content to load (you may need to adjust the timeout)
#     driver.implicitly_wait(3)
    # return driver


def getPlatformInfo():
    
    OS           = ""
    architecture = ""

    if(platform.system() == "Darwin"):
        OS = "mac"
    elif(platform.system() == "Windows"):
        OS = "win"
    else:
        OS = "unknown"

    # mac is arm64
    architecture = platform.machine()

    # ex: mac-arm64
    return OS + "-" + architecture

