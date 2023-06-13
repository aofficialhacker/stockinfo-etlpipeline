import time
import os
import zipfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from mysql.connector import connect

url = 'https://www.bseindia.com/markets/marketinfo/BhavCopy.aspx'

month = 'May'
year = '2023'
host = '127.0.0.1'
user = 'root'
password = 'root'
database = 'bhavcopydata'

cnx = connect(host=host, user=user, password=password, database=database)
cursor = cnx.cursor()

cursor.execute('drop table if exists equity')
cursor.execute(""" 
CREATE TABLE IF NOT EXISTS equity (
        id INT AUTO_INCREMENT PRIMARY KEY,
        SC_CODE int,
        SC_NAME VARCHAR(255),
        SC_GROUP VARCHAR(10),
        SC_TYPE VARCHAR(10),
        OPEN FLOAT,
        HIGH FLOAT,
        LOW FLOAT,
        CLOSE FLOAT,
        LAST FLOAT,
        PREVCLOSE FLOAT,
        NO_TRADES INT,
        NO_OF_SHRS INT,
        NET_TURNOV FLOAT,
        TDCLOINDI float,
        TRADING_DATE DATE
    )
""")

driver = webdriver.Chrome()
driver.get(url)
bhavcopy_folder = 'C:/Users/Dell/Downloads/bhavcopyfiles'
for day in range(1, 32):
    day_str = '{:02d}'.format(day)

    select_day = Select(driver.find_element('id', 'ContentPlaceHolder1_fdate1'))
    select_day.select_by_visible_text(day_str)

    select_month = Select(driver.find_element('id', 'ContentPlaceHolder1_fmonth1'))
    select_month.select_by_visible_text(month)

    select_year = Select(driver.find_element('id', 'ContentPlaceHolder1_fyear1'))
    select_year.select_by_visible_text(year)

    driver.find_element('id', 'ContentPlaceHolder1_btnSubmit').click()

    try:
        download_link = driver.find_element('id', 'ContentPlaceHolder1_btnHylSearBhav')
        download_url = download_link.get_attribute('href')

        driver.execute_script("arguments[0].click();", download_link)
        time.sleep(1)  # Wait for the file to download

        # Find the downloaded file in the default downloads folder
        downloads_folder = os.path.expanduser('~') + '/Downloads'
        file_name = max([downloads_folder + '/' + f for f in os.listdir(downloads_folder)], key=os.path.getctime)

        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(bhavcopy_folder)

        os.remove(file_name)

        csv_file = f"{bhavcopy_folder}/EQ{day_str}0523.CSV"

        df = pd.read_csv(csv_file)

        df['trading_date'] = f"{year}-{'05'}-{day_str}"

        df['TDCLOINDI'] = df['TDCLOINDI'].fillna(0)

        sql = "INSERT INTO equity (SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, " \
                  "NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI, TRADING_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, " \
                  "%s, %s, %s, %s, %s, %s, %s, %s)"

        data=[tuple(x) for x in df.values]
        cursor.executemany(sql, data)
        cnx.commit()


    except Exception as e:
        print(f'Exception occured : {str(e)}')

cnx.close()
