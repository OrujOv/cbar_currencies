#!/usr/bin/env python
# coding: utf-8

import urllib.request
import datetime
import time
import xml.etree.ElementTree as et
import collections
# --
from oracleconnect import ConnectToOracle

CBAR_URL = 'https://www.cbar.az/currencies/{day}.{month}.{year}.xml'
url_xml_date = ''
done_dates_cnt = 0


def main():
    global done_dates_cnt
    try:
        conn = ConnectToOracle()
        if conn.conn_check:
            oracle_con = conn.conn
        else:
            raise Exception('Oracle connection error')
        curr_dates = get_dates(oracle_con)
        for curr_date in curr_dates:
            print(curr_date.date())
            currencies = collections.defaultdict(list)
            url_xml = get_xml(curr_date)
            if url_xml is not None:
                parse_xml(url_xml, currencies)
            if datetime.datetime.strptime(url_xml_date, '%d.%m.%Y') == curr_date:
                set_data(currencies, oracle_con)
                done_dates_cnt += 1
                # local_file('w',url_xml) #write the xml to a file
    except Exception as err:
        log_error(err)
        return False
    else:
        if (done_dates_cnt > 0 and done_dates_cnt >= len(curr_dates)) or len(curr_dates) == 0:
            print('Done({0} dates)'.format(done_dates_cnt))
            return True
        else:
            return False
    finally:
        if oracle_con is not None:
            oracle_con.close()

    
def get_xml(cur_date=None):
    if not cur_date:
        cur_date = datetime.datetime.today()
    fh = None
    fh_xml = None
    try:
        fh_url = CBAR_URL.format(day=('0'+str(cur_date.day) if len(str(cur_date.day)) == 1 else cur_date.day),
                                 month=('0' + str(cur_date.month) if len(str(cur_date.month)) == 1 else cur_date.month),
                                 year=cur_date.year)
        fh = urllib.request.urlopen(fh_url)
        fh_xml = fh.read()
    except Exception as err:
        log_error(err)
    finally:
        if fh is not None:
            fh.close()
    return fh_xml.decode("utf-8") if fh_xml is not None else fh_xml


def parse_xml(p_xml, data):
    root = et.fromstring(p_xml)
    global url_xml_date
    url_xml_date = root.find(".").get("Date")
    for currency in root.findall("./ValType[@Type='Xarici valyutalar']/Valute"):
        cur_code = currency.get("Code")
        for tags in currency.iter():
            if len(tags.text.strip()) > 0:
                data[cur_code].append(tags.text)


def local_file(mode='r', file_content=None):
    if mode == 'w' and file_content:
        fh = None
        try:
            fh = open("cbar_xml_{0}.xml".format(url_xml_date), mode, encoding="utf-8")
            if mode == 'w' and file_content: 
                fh.write(file_content)
            elif mode == 'r':
                pass
        finally:
            if fh is not None:
                fh.close()    

            
def get_dates(p_con):
    dates = []
    #  Select the required dates
    sql_slc = """select dd.date from date_dictionary_table dd where dd.work_day = 'Y'
               and not exists (select 1 from cbar_currencies_table s where s.act_date = dd.date)
               and dd.date <= trunc(sysdate)
               order by dd.date"""
    try:
        cur = p_con.cursor()
        cur.execute(sql_slc)
        for date in cur:
            dates.append(date[0])
    except Exception as err:
        log_error(err)
    finally:
        if cur is not None:
            cur.close()
    return dates


def set_data(data, p_con):
    try:
        cur = p_con.cursor()
        for key, value in data.items():
            cur.callproc('insert_currencies_proc', (url_xml_date, key, value[1], value[2]))  # ins_cbar_currencies - procedure in database for inserting currencies
    except Exception as err:
        log_error(err)
    finally:
        if cur is not None:
            cur.close()


def log_error(error):
    print(error)
    error_time = datetime.datetime.strftime(datetime.datetime.now(), '%d.%m.%Y %H:%M:%S')
    row_delimiter = '\n' + '-' * 30
    error_record = '[' + error_time + '] ' + str(error) + row_delimiter
    with open('error_log.txt', 'a', encoding='utf-8') as log_file:
        log_file.write(error_record+'\n')


if __name__ == '__main__':
    done = False
    while done == False:
        cur_time = datetime.datetime.now().time()
        done = main()
        if done==False and cur_time.hour < 12:
            print('Sleep -', cur_time)
            time.sleep(600)  # 10 minutes
        else:
            done = True
    print('\n-----=====-----', 'Executed', sep='\n')
