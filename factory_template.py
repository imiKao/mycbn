import numpy as np
import pandas as pd
import os
import time
from influxdb import InfluxDBClient
from datetime import datetime, timedelta

class FactoryBasicTemplate(object):
    def __init__(self):
        self.host = "10.118.251.78"
        self.factory_jet_lag = {"CDE": 0, "TwoWing": 0, "FLEX": 12}

    def insert_into_influxdb(self, database, json_body, time_precison='m', batch_size=10000):
        host = self.host
        client = InfluxDBClient(host=host, port=8086, database=database)
        client.write_points(json_body, time_precision=time_precison, batch_size=batch_size)

    def display_current_time(self, display_name):
        t = time.localtime()
        a = time.strftime("%H:%M:%S", t)
        print("{}: {}".format(display_name, a))

    def df_change_column_to_numeric(self, df, column_name_list):
        for col_name in column_name_list:
            df[col_name] = df[col_name].apply(pd.to_numeric, args=('coerce',))
        return df

    def df_drop_row_by_particular_string(self, df, column_name, drop_string_list):
        for drop_str in drop_string_list:
            df = df[~df[column_name].str.contains(drop_str)]
        df = df.reset_index(drop=True)
        return df

    def df_drop_item_in_col(self, df, col_name, drop_item_list):
        for arg in drop_item_list:
            df_tmp = df[df[col_name]==arg]
            df.drop(df_tmp.index, axis=0, inplace=True)
            df = df.reset_index(drop=True)
        return df

    def influxdb_json_point(self, measurement, timestamp, tags_dict, fields_dict):
        json_point = {
            "measurement": measurement,
            "time": timestamp,
            "tags": tags_dict,
            "fields": fields_dict
        }
        return json_point

    def list_drop_item(self, target_list, drop_item_list):
        for item in drop_item_list:
            if item in target_list:
                target_list.remove(item)
        return target_list

class CpkTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(CpkTemplate, self).__init__()

    def std_calc(self, x):
        rbar_list = []
        for i in range(1, len(x)):
            rbar_list.append(abs(x["ITEM_VALUE"][i]-x["ITEM_VALUE"][i-1]))
        if rbar_list!=[]:
            result = (sum(rbar_list) / len(rbar_list)) / 1.128
        else:
            result = 0
        return result

    def cpk_calc(self, x):
        avg=x['ITEM_VALUE'].mean()
        std = self.std_calc(x)
        count=len(x['ITEM_VALUE'])
        usl=x['USL'].mean()
        lsl=x['LSL'].mean()
        if avg!=0 and count>=30:
            if np.isnan(x["USL"].mean()):
                cpk=(avg-lsl)/(3*std)
            elif np.isnan(x["LSL"].mean()):
                cpk=(usl-avg)/(3*std)
            else:
                cpk=np.min([(usl-avg)/(3*std),(avg-lsl)/(3*std)])
            return avg, std, cpk, lsl, usl, count
        else:
            cpk = None
            std = None
            return avg, std, cpk, lsl, usl, count

    def cpk_cal_to_df(self, x):
        d={}
        mu=x['ITEM_VALUE'].mean()
        std = self.std_calc(x)
        num=len(x['ITEM_VALUE'])
        USL=x['USL'].mean()
        LSL=x['LSL'].mean()
        if std!=0 and num>=30:
            if np.isnan(x.USL.mean()):
                cpk=(mu-LSL)/(3*std)
            elif np.isnan(x.LSL.mean()):
                cpk=(USL-mu)/(3*std)
            else:
                cpk=np.min([(USL-mu)/(3*std),(mu-LSL)/(3*std)])
            d = {'AVG':mu, 'STD':std, 'CPK':cpk, 'LSL':LSL, 'USL':USL, 'num':num}
            return pd.Series(d, index=['AVG', 'STD', 'CPK', 'LSL', 'USL', 'num'])
        else:
            d = {'AVG':mu, 'STD':None, 'CPK':None, 'LSL':LSL, 'USL':USL, 'num':num}
            return pd.Series(d, index=['AVG', 'STD', 'CPK', 'LSL', 'USL', 'num'])

    def cp_calc(self, x):
        avg=x['ITEM_VALUE'].mean()
        std = self.std_calc(x)
        count=len(x['ITEM_VALUE'])
        usl=x['USL'].mean()
        lsl=x['LSL'].mean()
        if avg!=0 and count>=30:
            if np.isnan(x["USL"].mean()):
                cp=(avg-lsl)/(3*std)
            elif np.isnan(x["LSL"].mean()):
                cp=(usl-avg)/(3*std)
            else:
                cp=(usl-lsl)/(6*std)
            return cp

    def ck_calc(self, x):
        avg=x['ITEM_VALUE'].mean()
        count=len(x['ITEM_VALUE'])
        usl=x['USL'].mean()
        lsl=x['LSL'].mean()
        if avg!=0 and count>=30:
            if np.isnan(x["USL"].mean()):
                ck=None
            elif np.isnan(x["LSL"].mean()):
                ck=None
            else:
                ck=(((usl+lsl)/2)-avg)/((usl-lsl)/2)
                ck = abs(ck)
            return ck

    def leveling_by_cp_and_ca(self, cp, ca):
        level_score = {"critical": 3, "warning": 1, "normal": 0}
        if ca==None and cp!=None:
            cp_level = "critical" if cp<1.0 else "warning" if cp<1.33 else "normal"
            cp_score = level_score[cp_level] * 1.3
            return cp_score
        elif cp!=None:
            cp_level = "critical" if cp<1.0 else "warning" if cp<1.33 else "normal"
            cp_score = level_score[cp_level]
            ca_level = "critical" if (1-ca)<0.5 else "warning" if (1-ca)<0.75 else "normal"
            ca_score = level_score[ca_level]
            return cp_score + ca_score
        else:
            return 0

    def calc_percentage_for_stdev_area(self, df, column_name, avg, std):
        if std!=None:
            std=std
        else:
            std=1
        cut_list = [-999, avg-3*std, avg-2*std, avg-std, avg+std, avg+2*std, avg+3*std, 999]
        result_series = pd.cut(df[column_name], cut_list, labels=['n3sigma', 'n2sigma', 'n1sigma', 'avg', 'p1sigma', 'p2sigma', 'p3sigma'])
        return result_series

class FpyTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(FpyTemplate, self).__init__()

    def df_add_station_type(self, df, stationtype_name, station_dict):
        station_list = []
        for index in range(len(df)):
            for stationtype in stationtype_name:
                if df.loc[index, "Station"] in station_dict[stationtype]:
                    station_list.append(stationtype)
        df["StationType"] = station_list
        return df

    def df_calc_station_type_yield(self, df):
        df = df.groupby(['StationType']).sum()
        df["Yield"] = df["PASS"] / (df["PASS"]+df["FAIL"]+0.0001)
        df["TOTAL"] = df["PASS"] + df["FAIL"]
        df = df[df["PASS"]!=0]   # Avoid station which FPY=0 display on dashboard
        return df

    def df_fpy_station_preprocessing(self, df):
        station_type_name, station_type_dict = self.get_station_type_empty_dict()
        df_sub_station = df["Station"].unique()
        station_type_dict = self.get_station_type_dict_value(station_type_dict, df_sub_station)
        df = self.df_add_station_type(df, station_type_name, station_type_dict)
        df_substation = df[["Station", "StationType", "PASS", "FAIL"]]
        return df_substation

    def df_fpy_preprocessing(self, df):
        station_type_name, station_type_dict = self.get_station_type_empty_dict()
        df_sub_station = df["Station"].unique()
        station_type_dict = self.get_station_type_dict_value(station_type_dict, df_sub_station)
        df = self.df_add_station_type(df, station_type_name, station_type_dict)
        df = df[["StationType", "PASS", "FAIL"]]
        return df

    def get_this_hour_data(self, folder_path, shift_period):
        this_hour = (datetime.now() - timedelta(hours=shift_period)).strftime('%Y%m%d-%H')
        this_hour_file = [i for i in os.listdir(folder_path) if this_hour in i and self.factory_name in i and "Yield" in i and "!" not in i]
        return this_hour_file

    def get_last_hour_data(self, folder_path, shift_period):
        last_hour = (datetime.now() - timedelta(hours=shift_period+1)).strftime('%Y%m%d-%H')
        last_hour_file = [i for i in os.listdir(folder_path) if last_hour in i and self.factory_name in i and "Yield" in i and "!" not in i]
        return last_hour_file

    def get_today_file(self, folder_path):
        today_file = [i for i in os.listdir(folder_path) if self.factory_name in i and "Yield" in i and "!" not in i]
        today_file = sorted(today_file)
        return today_file

    def insert_station_hourly_fpy_to_influxdb(self, table_name, df, time_in_db):
        json = []
        for index in range(len(df)):
            json_point = {"measurement": table_name,
                          "time":time_in_db.strftime('%Y-%m-%dT%H:12:00Z'),
                          "tags":{"STATIONTYPE": df["StationType"][index],
                                  "STATION": df["Station"][index]},
                          "fields":{"PASS": df["PASS"][index],
                                    "FAIL": df["FAIL"][index],
                                    "Yield": df["Yield"][index]}
                }
            json.append(json_point)
        print(json)
        self.insert_into_influxdb(self.factory_name, json, time_precision='m', batch_size=10000)

    def insert_average_fpy_into_influxdb(self, table_name, df, shift_period):
        time_in_db = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        threshold = 0.1*(df["TOTAL"].mean())
        df_cal = df[df["TOTAL"].gt(threshold)]
        df_cal = df_cal[df_cal["TOTAL"].gt(30)]
        print(df_cal)
        if not df_cal.empty:
            df_cal = df_cal[df_cal.index!="Others"]
            hour_average_fpy = df_cal["Yield"].mean()
            json = [{"measurement": table_name,
                     "time":time_in_db.strftime('%Y-%m-%dT%H:12:00Z'),
                     "tags":{"STATIONTYPE": "TOTAL"},
                     "fields":{"PASS": df["PASS"].sum(),
                               "FAIL": df["FAIL"].sum(),
                               "Yield": hour_average_fpy}
            }]
            print(json)
            self.insert_into_influxdb(self.factory_name, json, time_precision='m', batch_size=10000)

    def two_file_substract(self, folder_path, this_hour_file, last_hour_file):
        df_last_hour = pd.read_csv(folder_path+last_hour_file).drop(['Yield'], axis=1)
        df_this_hour = pd.read_csv(folder_path+this_hour_file).drop(['Yield'], axis=1)
        df = pd.merge(left=df_this_hour, right=df_last_hour, left_on='Station', right_on='Station')
        df["PASS"] = df["PASS_x"] - df["PASS_y"]
        df["FAIL"] = df["FAIL_x"] - df["FAIL_y"]
        df["Yield"] = 100 * df["PASS"] / (df["PASS"]+df["FAIL"]+0.0001)
        return df

    def get_station_type_empty_dict(self):
        stationtype_dict = dict()
        stationtype_name = ["PREWLAN", "PRECONFIG", "RETURN_LOSS", "BCD", "FUNCTION", "VOICE", "WLAN", "GIGACHECK", \
                        "FINALCHECK", "Others"]
        for i in stationtype_name:
            stationtype_dict[i] = []
        return stationtype_name, stationtype_dict

    def get_station_type_dict_value(self, station_type_dict, station_regex_list):
        for item in station_regex_list:
            if "OBA" in item:
                station_type_dict["Others"].append(item)
            elif "RETURNLOSS" in item:
                station_type_dict["RETURN_LOSS"].append(item)
            elif "BCD" in item:
                station_type_dict["BCD"].append(item)
            elif "PRECONFIG" in item:
                station_type_dict["PRECONFIG"].append(item)
            elif "GIGA_CHECK" in item:
                station_type_dict["GIGACHECK"].append(item)
            elif "VOICE" in item:
                station_type_dict["VOICE"].append(item)
            elif "FUNC" in item:
                station_type_dict["FUNCTION"].append(item)
            elif "CONFIG" in item:
                station_type_dict["BCD"].append(item)
            elif "FINAL_CHECK" in item or "FINALCHK" in item:
                station_type_dict["FINALCHECK"].append(item)
            elif "PREWLAN" in item and "REP" not in item:
                station_type_dict["PREWLAN"].append(item)
            elif "WLAN" in item:
                station_type_dict["WLAN"].append(item)
            else:
                station_type_dict["Others"].append(item)
        return station_type_dict
