# coding: utf-8
from bs4 import BeautifulSoup
#region (Selenium 相關套件)
from selenium import webdriver

from selenium.webdriver.support.ui import Select #下拉選單的method
from selenium.webdriver.common.by import By 

from  selenium.webdriver.support.ui  import  WebDriverWait 
from  selenium.webdriver.support  import  expected_conditions  as  EC
#endregion

from datetime import datetime, timedelta
import random, time, pymysql

# 資料庫連線
def connect_db(host, user, pwd, dbname, port):
    try:
        db = pymysql.connect(
            host = host,
            user = user,
            passwd = pwd,
            database = dbname,
            port = int(port)
        )
        # print("連線成功")
        return db
    except Exception as e:
        print('連線資料庫失敗: {}'.format(str(e)))
    return None

# 修改氣象站資訊
def UpdateStation(id, td_wDir, td_wPower, td_temp, td_rain, td_humidity, td_presure, td_sunlight, td_weather, td_obsTime):
    with db.cursor() as cursor:
        try:
            cursor.execute(
                """
                UPDATE `station` SET 
                    `WDIR`=%s, `WLevel`=%s, `TEMP`=%s, `24R`=%s, 
                    `Humidity`=%s, `presure`=%s, `D_TS`=%s, 
                    `Weather`=%s, `record_time`=%s, `status`=%s 
                WHERE `station`.`id` = %s;
                """,
                (td_wDir, td_wPower, td_temp, td_rain, 
                 td_humidity, td_presure,td_sunlight,
                 td_weather, td_obsTime, '正常',
                 id)
            )
            db.commit()

        except pymysql.MySQLError as e:
            print(f"[UpdateStation]執行時發生問題: {e}")

# 解析driver的網頁資訊 (soup)
def parseDriverContent(driver):
    #指定 lxml 作為解析器
    soup = BeautifulSoup(driver.page_source, features='lxml')

    #<tbody id='stations'>
    tbody = soup.find('tbody',{'id':'stations'})

    #<tbody>内所有<tr>標籤
    trs = tbody.find_all('tr')

    # 等等下面可以使用(將時間串成datetime)
    date_str = datetime.now().strftime("%Y-%m-%d") # 時間格式

    #對list中的每一項 <tr>
    for tr in trs:
        id = tr.get("data-href")
        id = id.replace("OBS_Station.html?ID=", "") # 取得 id，從<tr data-href='<取得這個>'>
        name = tr.find('th', {'headers': 'station'}) # 包含在 tr中的 thead中的 <a>

        #region (特殊情況處理 - EX: 儀器故障)
        if(not name.a):
            name = name.getText() # 這邊因為儀器故障，所以沒有<a>，站名則直接在<th>上了
            status = tr.find('td').span.getText()  # 氣象站運行狀態 (儀器故障
            # print("【{0}】- 站名: {1}".format(status, name))
            with db.cursor() as cursor:
                try:
                    sql = f"UPDATE `station` SET `status`='{status}'   WHERE `station`.`id` = '{id}';"
                    cursor.execute(sql)   
                    db.commit()
                except pymysql.MySQLError as e:
                    print("存入資料發生問題" + e)
                continue
        #endregion

        #region (捕捉並定義變數)
        td_obsTime = tr.find('td', {'headers': 'OBS_Time'}).getText() # 觀測時間

        # 【-1】: 代表無（觀測）資料。
        # 【-99】: 代表無（觀測）資料。
        name = name.a.getText() # 其他有在運作的氣象站則讀取<a>的文字
        td_temp = tr.find('td', {'headers': 'temp'}).getText() # 氣溫
        td_temp = td_temp if td_temp != '-' else -99
        
        td_weather = tr.find('td', {'headers': 'weather'}) # 找到 headers為 weather的td
        td_weather = td_weather.img.get("title") if (td_weather.img) else '-' # 得到該 td中底下 <img>的 title屬性

        td_wDir = tr.find('td', {'headers': 'w-1'}).getText() # 風向
        td_wDir = td_wDir if td_wDir != '-' else -99

        td_wPower = tr.find('td', {'headers': 'w-2'}).getText() # 風力(級數)
        td_wPower = td_wPower if td_wPower != '-' else -99

        td_wGust = tr.find('td', {'headers': 'w-3'}).getText() # 陣風(級數) ->通常沒資料 

        td_humidity = tr.find('td', {'headers': 'hum'}).getText() # 濕度
        td_humidity = td_humidity if td_humidity != '-' else -99

        td_presure = tr.find('td', {'headers': 'pre'}).getText() # 氣壓
        td_presure = td_presure if td_presure != '-' else -99

        td_rain = tr.find('td', {'headers': 'rain'}).getText() # 當日累積降雨量(mm)
        td_rain = td_rain if td_rain != '-' else -99
        
        td_sunlight = tr.find('td', {'headers': 'sunlight'}).getText() # 日照時數(hr)
        td_sunlight = td_sunlight if td_sunlight != '-' else -99

        print(f"【{td_obsTime} - {(id + '_' + name)}】 溫度: {td_temp}℃ 、 天氣狀況: {td_weather} 、 風向: {td_wDir} 、 風力: {td_wPower}級 、 陣風: {td_wGust}級、 濕度: {td_humidity}% 、 降雨量: {td_rain}mm 、 日照時數: {td_sunlight}")
        
        #endregion

        #region (更新氣象站數據)
        try:
            td_obsTime = datetime.strptime(f"{date_str} {td_obsTime}", "%Y-%m-%d %H:%M") # 日期格式
            UpdateStation(id, td_wDir, td_wPower, td_temp, td_rain, td_humidity, td_presure, td_sunlight, td_weather, td_obsTime)
        except Exception as e:
            print(f"更新區域發生錯誤: {e}")
        #endregion


if __name__ == '__main__':
    start = time.time()

    map_CityID_Dict = {
        "臺北市": 63,
        "高雄市": 64,
        "新北市": 65,
        "臺南市": 67,
        "臺中市": 66,
        "桃園市": 68,
        "宜蘭縣": 10002,
        "新竹縣": 10004,
        "苗栗縣": 10005,
        "彰化縣": 10007,
        "南投縣": 10008,
        "雲林縣": 10009,
        "嘉義縣": 10010,
        "屏東縣": 10013,
        "臺東縣": 10014,
        "花蓮縣": 10015,
        "澎湖縣": 10016,
        "基隆市": 10017,
        "新竹市": 10018,
        "嘉義市": 10020,
        "金門縣": '09020',
        "連江縣": '09007',
    }
    
    now_time = datetime.now() # 現在的時間點

    CHROMEDRIVER_PATH = './chromedriver.exe'  # chromedriver

    db = connect_db(
        host='127.0.0.1',
        user='root',
        pwd='Ru,6e.4vu4wj/3',
        dbname='greenhouse',
        port=3306,
    ) # 資料庫連線

    try:
        #region (Driver Option)
        option = webdriver.ChromeOptions()

        # 【參考】https://ithelp.ithome.com.tw/articles/10244446
        option.add_argument("headless") # 不開網頁搜尋
        option.add_argument('blink-settings=imagesEnabled=false') # 不加載圖片提高效率
        option.add_argument('--log-level=3') # 這個option可以讓你跟headless時網頁端的console.log說掰掰
        """下面參數能提升爬蟲穩定性"""
        option.add_argument('--disable-dev-shm-usage') # 使用共享內存RAM
        option.add_argument('--disable-gpu') # 規避部分chrome gpu bug
        #endregion

        #region (啟動模擬瀏覽器)
        driver = webdriver.Chrome(CHROMEDRIVER_PATH, chrome_options=option)

        #取得網頁代碼
        url = f"https://www.cwb.gov.tw/V8/C/W/OBS_County.html?ID=10017"
        driver.get(url)

        if not driver.title:
            print(f"📛未成功進入頁面...")
            pass
        
        print(f"✅成功進入頁面...({driver.title})")
        #endregion

        
        driver.implicitly_wait(5) # 引性等待 => 等待頁面跑完在往下

        #region (捕捉下拉選單)
        SelectCounty = WebDriverWait(driver, 10, 1).until(
            EC.presence_of_element_located(
                (By.ID, 'County')
            )
        )
        SelectCounty = Select( SelectCounty ) # (*下拉選單處理)
        #endregion


        #region (切換縣市)
        for city, id in map_CityID_Dict.items():
            print(f"【♻️ {city}】")
            wait_sec = random.uniform(0.5, 1.5)
            time.sleep(wait_sec) # 等待秒數
            SelectCounty.select_by_visible_text( city ) # 選擇縣市

            time.sleep(0.5) # 等待Loading
            parseDriverContent(driver)

            print("----------------------------")
        #endregion

    except Exception as e:
        print(f"啟動chromedirver發生錯誤: {e}")
    
    finally:
        driver.close()
        driver.quit()

        print(f"程式執行時間: {format( time.time() - start )}秒")
        print(f"程式執行結束，2秒後將關閉視窗")
        time.sleep(2)


    # count = 0
    # for city, id in map_CityID_Dict.items():
    #     #全臺測站分區 - 測站列表
    #     url = f"https://www.cwb.gov.tw/V8/C/W/OBS_County.html?ID={str(id)}"
    #     getWeatherStation(url)
    #     count += 1
    #     print(f"----------------({city} - 抓取完畢[{count}/{len(map_CityID_Dict)}])----------------")
    #     time.sleep(random.randint(1,3)) # 1~5秒 random跑一次(避免爬取速度太快)
    

    # plus_time = timedelta(minutes = 10)
    # print(f"下一次排程時間為: {(now_time + plus_time).strftime('%Y-%m-%d %H:%M:%S')}")

