# -*- coding: utf-8 -*-

import urllib.request
from urllib.request import Request
from urllib.request import urlopen
import json
import sqlite3
from datetime import date
import dateutil
from dateutil.rrule import rrule, DAILY

##############################################################################


class FetchData(object):

    def __init__(self):
        return

    def fetch_astro(self, date):
        url = 'https://api.weather.com/v2/astro?apiKey=6532d6454b8aa370768e63d6ba5a832e&geocode=47.61%2C-52.75&days=1&date=' + date + '&format=json'
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        return urlopen(req).read()

    def fetch_weather(self, date):
        url = 'https://api.weather.com/v1/location/CYYT:9:CA/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=e&startDate=' + date + '&endDate=' + date
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            result = urlopen(req).read()
        except:
            result = None
        return result

    def get_daily_astro(self, jsn, date):
        obj = json.loads(jsn)
        dawnLocal = obj["astroData"][0]["sun"]["twilight"]["astronomical"]["dawnLocal"]
        duskLocal = obj["astroData"][0]["sun"]["twilight"]["astronomical"]["duskLocal"]
        riseLocal = obj["astroData"][0]["moon"]["riseSet"]["riseLocal"]
        setLocal = obj["astroData"][0]["moon"]["riseSet"]["setLocal"]
        moonage = obj["astroData"][0]["moon"]["riseSet"]["moonage"]
        percentIlluminated = obj["astroData"][0]["moon"]["riseSet"]["percentIlluminated"]
        return (date, dawnLocal, duskLocal, riseLocal, setLocal, moonage, percentIlluminated, dawnLocal[-6:]) # timeshift tag

    def get_daily_weather(self, jsn, timeshift):
        if jsn == None:
            return None
        obj = json.loads(jsn)
        records = []
        for o in obj["observations"]:
            valid_time_gmt = o["valid_time_gmt"]  # GMT; minus 30*60 sec to get starttime
            stime_local = str(int(valid_time_gmt) - 30*60 - int(timeshift[2])*3600 - 30*60)
            day_ind = o["day_ind"]                # day/night indicator
            wx_phrase = o["wx_phrase"]            # weather phrase
            temp = o["temp"]                      # temperature (F)
            heat_index = o["heat_index"]          # heat index
            feels_like = o["feels_like"]          # feels like (F)
            dewPt = o["dewPt"]                    # dew point (F)
            rh = o["rh"]                          # humidity (%)
            pressure = o["pressure"]              # pressure (in)
            vis = o["vis"]                        # visibility (km)
            wc = o["wc"]                          # wind chill
            wdir = o["wdir"]                      # wind direction
            wdir_cardinal = o["wdir_cardinal"]    # wind cardinal
            gust = o["gust"]                      # wind gust (mph)
            wspd = o["wspd"]                      # wind speed (mph)
            uv_index = o["uv_index"]              # UV index
            uv_desc = o["uv_desc"]                # UV description
            record = (stime_local, day_ind, wx_phrase, temp, heat_index, feels_like, dewPt, rh, pressure, vis, wc, wdir, wdir_cardinal, gust, wspd, uv_index, uv_desc)
            records.append(record)
        return records

    def restore_db_astro(self, record):
        if record == None:
            return
        else:
            conn = None
            try:
                conn = sqlite3.connect('clearnight.db')
                sql = 'INSERT INTO Astro(date,dawn_local,dusk_local,rise_local,set_local,moonage,percent_illuminated,time_shift) VALUES(?,?,?,?,?,?,?,?)'
                conn.cursor().execute(sql, record)
                conn.commit()
            except:
                print("Error database")
            finally:
                conn.close()

    def restore_db_weather(self, records):
        if records == None:
            return
        conn = None
        try:
            conn = sqlite3.connect('clearnight.db')
            for record in records:
                if record == None:
                    continue
                else:
                    sql = 'INSERT INTO weather(stime_local,day_ind,wx_phrase,temperature,heat_index,feels_like,dewPt,humidity,pressure,visibility,wind_chill,wind_direction,wdir_cardinal,gust,wind_speed,uv_index,uv_desc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                    conn.cursor().execute(sql, record)
            conn.commit()
        except:
            print("Error database")
        finally:
            conn.close()

##############################################################################


def main():
    obj = FetchData()
    
    st = date(2021, 3, 23) #1951-1-1
    en = date(2021, 4, 13) #including

    for dt in rrule(DAILY, dtstart=st, until=en):
        d = dt.strftime("%Y%m%d")
        print(d)
        jastro = obj.fetch_astro(d)
        arecord = obj.get_daily_astro(jastro, d)
        obj.restore_db_astro(arecord)
        jweather = obj.fetch_weather(d)
        wrecords = obj.get_daily_weather(jweather, arecord[7])
        obj.restore_db_weather(wrecords)

##############################################################################


if __name__ == '__main__':
    main()
