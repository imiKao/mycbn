import numpy as np
import pandas as pd
import os
import time
from influxdb import InfluxDBClient
from ftplib import FTP
from datetime import datetime, timedelta

class FactoryBasicTemplate(object):
    def __init__(self):
        self.host = "10.118.251.78"
        self.factory_jet_lag = {"CDE": 0, "TwoWing": 0, "FLEX": 12}
        self.jenkins_home = "/home/cbn_generic_2/jenkins_slave/workspace/Factory_CPK/Factory_catch_file_from_ftp"

    def insert_into_influxdb(self, database, json_body, time_precision='m', batch_size=10000):
        host = self.host
        client = InfluxDBClient(host=host, port=8086, database=database)
        client.write_points(json_body, time_precision=time_precision, batch_size=batch_size)

    def display_current_time(self, display_name):
        t = time.localtime()
        a = time.strftime("%H:%M:%S", t)
        print("{}: {}".format(display_name, a))

    def df_change_column_to_numeric(self, df, column_name_list):
        for col_name in column_name_list:
            df[col_name] = df[col_name].apply(pd.to_numeric, args=('coerce',))
        return df

    def df_change_column_type(self, df, col_new_type_dict):
        df = df.astype(col_new_type_dict)
        return df

    def df_combine_two_column(self, df, new_col, col_1, col_2, hy_pen):
        df[new_col] = df[col_1] + hy_pen + df[col_2]
        return df

    def df_column_choose_value(self, df, col_name, choose_value):
        df = df[df[col_name]==choose_value]
        df = df.reset_index(drop=True)
        return df

    def df_column_sort_value(self, df, col_name):
        df = df.sort_values(by=col_name)
        df = df.reset_index(drop=True)
        return df

    def df_drop_column(self, df, drop_column_name_list):
        df = df.drop(columns=drop_column_name_list)
        return df

    def df_drop_row_by_particular_string(self, df, column_name, drop_string_list):
        for drop_str in drop_string_list:
            df = df[~df[column_name].str.contains(drop_str)]
        df = df.reset_index(drop=True)
        return df

    def df_drop_multi_layer_index_by_particular_string(self, df, drop_string_list, index_layer=1):
        """
        level: multi-level index's layer
        """
        for drop_str in drop_string_list:
            try:
                df = df.drop(drop_str, axis=0, level=index_layer-1)
            except:
                continue
        return df

    def df_drop_item_in_col(self, df, col_name, drop_item_list):
        for arg in drop_item_list:
            df_tmp = df[df[col_name]==arg]
            df.drop(df_tmp.index, axis=0, inplace=True)
            df = df.reset_index(drop=True)
        return df

    def df_get_empty_dataframe(self, column_list):
        df = pd.DataFrame(columns=column_list)
        return df

    def df_rename_column(self, df, rename_dict):
        df = df.rename(rename_dict, axis=1)
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

class Ftp(object):
    def __init__(self):
        pass

    def ftp_login(self, host="ftp2.compalbn.com", user_name="cbn_grafana", password="HGB7Z3mf"):
        ftp=FTP()
        ftp.set_debuglevel(2)
        ftp.connect(host)
        ftp.login(user_name, password)
        return ftp

    def ftp_get_filename_list(self, ftp):
        filelist = []
        ftp.retrlines("LIST", filelist.append)
        filenamelist = []
        for item in filelist:
            words = item.split(None, 8)
            filename = words[-1].lstrip()
            filenamelist.append(filename)
        return filenamelist

    def ftp_copy_file_to_local(self, ftp, today_file_list, folder_path):
        bufsize=1024
        for item in today_file_list:
            ftp.retrbinary("RETR %s"%(item), open(folder_path+item, 'wb').write, bufsize)

    def ftp_delete_file(self, ftp, today_file_list):
        for item in today_file_list:
            ftp.delete(item)

class CpkTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(CpkTemplate, self).__init__()

    def std_rbar(self, x):
        x= [i for i in x]
        rbar_list = []
        for i in range(1, len(x)):
            rbar_list.append(abs(x[i] - x[i-1]))
        if rbar_list!=[]:
            result = (sum(rbar_list) / len(rbar_list)) / 1.128
        else:
            result = 0
        return result
    
    def std_calc(self, x):
        rbar_list = []
        for i in range(1, len(x)):
            rbar_list.append(abs(x["ITEM_VALUE"][i]-x["ITEM_VALUE"][i-1]))
        if rbar_list!=[]:
            result = (sum(rbar_list) / len(rbar_list)) / 1.128
        else:
            result = 0
        return result

    def cpk_column(self, df, cpk_col, usl_avg_col, avg_lsl_col):
        df.loc[:, cpk_col] = df[[usl_avg_col, avg_lsl_col]].min(axis=1)
        return df

    def cp_column(self, df, cp_col, usl_lsl_col, usl_avg_col, avg_lsl_col):
        df.loc[:, cp_col] = df.loc[:,usl_lsl_col]
        df.loc[:, cp_col] = np.where(~np.isnan(df.loc[:,cp_col]), df.loc[:,cp_col], df.loc[:,usl_avg_col])
        df.loc[:, cp_col] = np.where(~np.isnan(df.loc[:,cp_col]), df.loc[:,cp_col], df.loc[:,avg_lsl_col])
        return df

    def ck_column(self, df, ck_col, usl_col, lsl_col, avg_col):
        df.loc[:, ck_col] = (((df.loc[:,usl_col]+df.loc[:,lsl_col])/2)-df.loc[:,avg_col])/((df.loc[:,usl_col]-df.loc[:,lsl_col])/2)
        df.loc[:, ck_col] = abs(df.loc[:, ck_col])
        return df

    def level_column_by_cp_ck(self, df, level_col, cp_score_col, ck_score_col, cp_col, ck_col):
        level_score = {"critical": 3, "warning": 1, "normal": 0}
        df.loc[:,cp_score_col] = np.where(df.loc[:,cp_col]<1.0, level_score["critical"], level_score["normal"])
        df.loc[:,cp_score_col] = np.where((df.loc[:,cp_col]>=1.0)&(df.loc[:,cp_col]<1.33), level_score["warning"], df.loc[:,cp_score_col])
        df.loc[:,cp_score_col] = np.where(np.isnan(df.loc[:,ck_col]), df.loc[:,cp_score_col]*1.3, df.loc[:,cp_score_col])
        df.loc[:,ck_score_col] = 1-df.loc[:,ck_col]
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<0.5, level_score["critical"], df.loc[:,ck_score_col])
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<0.75, level_score["warning"], df.loc[:,ck_score_col])
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<1.0, level_score["normal"], df.loc[:,ck_score_col])
        df.loc[:,level_col] = df.loc[:,cp_score_col].fillna(0) + df.loc[:,ck_score_col].fillna(0)
        return df

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

    def get_df_from_cpk_daily_file(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1-1: Start setting time & other information")
        self.date_time = (datetime.now() - timedelta(days=shift_period))
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(self.date_time.strftime('%Y%m%d'))
        self.display_current_time("===== Factory: {}".format(factory_name))
        self.display_current_time("===== Host = {}".format(self.host))
        self.display_current_time("===== Date: {}".format(self.date_time.strftime("%Y-%m-%d")))

        self.display_current_time("========== Step1-2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step1-3: Start reading dataset")
        column_list = ['MODEL', 'TEST_TIME', 'MSN', 'STATION_TYPE', 'LINE', 'STATION_NAME', 'TEST_ITEM', 'PORT', 'ITEM_VALUE', 'LSL', 'USL']
        df_all = pd.DataFrame(columns=column_list)
        for save_file in save_file_name_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df_all = pd.concat([df_all, df_sub], axis=0, ignore_index=True)
        return df_all
    
    def get_df_from_cpk_hourly_file(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1-1: Start setting time & other information")
        self.shift_period = shift_period
        shift_period = self.shift_period + self.factory_jet_lag[factory_name]
        current_hour = (datetime.now() - timedelta(hours=shift_period)).strftime('%Y%m%d-%H')
        self.date_time = (datetime.now() - timedelta(hours=shift_period))
        self.display_current_time("===== Factory: {}".format(factory_name))
        self.display_current_time("===== Host: {}".format(self.host))
        self.display_current_time("===== Time: {}".format(self.date_time.strftime("%Y-%m-%d %H:00:00")))
        self.time_in_json = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(self.date_time.strftime('%Y%m%d'))
        self.display_current_time("========== Step1-2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]
        self.display_current_time("========== Step1-3: Start reading dataset")
        df_all = pd.read_csv(self.folder_path+save_file_name)
        return df_all

    def get_df_cpk(self, df, groupby_columns):
        self.display_current_time("Get amount of item value AVG, STD, USL/LSL columns and Cpk calculation group by columns")
        df = df.groupby(groupby_columns).agg({"ITEM_VALUE": ["mean", self.std_rbar, "size"], "USL": ["mean"], "LSL": ["mean"]})
        df = df[df.loc[:,("ITEM_VALUE","size")].gt(30)]   # Cpk calculation requires amount of data bigger than 30
        df = self.df_drop_multi_layer_index_by_particular_string(df, ["CYCLETIME", "UPTIME"], index_layer=len(groupby_columns))
        df.loc[:, ("CPK","usl_avg")] = (df.loc[:,("USL","mean")] - df.loc[:, ("ITEM_VALUE","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","avg_lsl")] = (df.loc[:, ("ITEM_VALUE","mean")] - df.loc[:,("LSL","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","cpk")] = df[[("CPK","usl_avg"), ("CPK","avg_lsl")]].min(axis=1)
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl")])
        df = df.reset_index()
        return df

class FpyTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(FpyTemplate, self).__init__()

    def check_week_or_month_interval_num(self, interval="WEEK"):
        if interval=='WEEK':
            df = self.df_fpy_week()
            field_name_list = ['Weeknum', 'Week_PASS', 'Week_FAIL', 'Weekly_fpy']
            interval_num = self.week_num
            return df, field_name_list, interval_num
        elif interval=='MONTH':
            df = self.df_fpy_month()
            field_name_list = ['Month', 'Month_PASS', 'Month_FAIL', 'Monthly_fpy']
            interval_num = self.month
            return df, field_name_list, interval_num
        else:
            return 0

    def check_this_week_file(self):
        self.week_num = (datetime.now() - timedelta(days=self.shift_day)).isocalendar()[1]
        check_thisweek_list = [self.today]
        for day in range(1,7):
            if (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).isocalendar()[1] != self.week_num:
                continue
            this_week_day = (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%Y%m%d')
            check_thisweek_list.append(this_week_day)
        return check_thisweek_list

    def check_this_month_file(self):
        self.month = (datetime.now() - timedelta(days=self.shift_day)).strftime('%m')
        check_thismonth_list = [self.today]
        for day in range(1,30):
            if (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%m') != self.month:
                continue
            this_month_day = (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%Y%m%d')
            check_thismonth_list.append(this_month_day)
        return check_thismonth_list

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

    def df_fpy_week(self):
        check_thisweek_list = self.check_this_week_file()
        df = pd.DataFrame(columns=["Station", "PASS", "FAIL"])
        df = self.df_merge_all_week_month_file(df, check_thisweek_list)
        return df

    def df_fpy_month(self):
        check_thismonth_list = self.check_this_month_file()
        df = pd.DataFrame(columns=["Station", "PASS", "FAIL"])
        df = self.df_merge_all_week_month_file(df, check_thismonth_list)
        return df

    def df_merge_all_week_month_file(self, df, folder_date_list):
        for day in folder_date_list:
            folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(day)
            if not os.path.isdir(folder_path):
                continue
            last_file = [i for i in os.listdir(folder_path) if self.factory_name in i and "Yield" in i and "!" not in i]
            if last_file==[]:
                continue
            last_file = sorted(last_file, reverse=True)
            df_sub = pd.read_csv(folder_path+last_file[0])
            df_sub = df_sub.drop(['Yield'], axis=1)
            df = df.set_index("Station").add(df_sub.set_index("Station"), fill_value=0).reset_index()
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
        self.insert_into_influxdb(self.factory_name, json, 'm', 10000)

    def insert_station_type_daily_fpy_to_influxdb(self, table_name, df, time_in_db):
        json = []
        for index in range(len(df)):
            json_point = {
                    "measurement": table_name,
                    "time":time_in_db.strftime('%Y-%m-%dT00:00:00Z'),
                    "tags":{"STATIONTYPE": df.index[index]},
                    "fields":{"PASS": df["PASS"][index],
                              "FAIL": df["FAIL"][index],
                              "Yield": df["Yield"][index]}
            }
            json.append(json_point)
        print(json)
        self.insert_into_influxdb(self.factory_name, json, 'm', 10000)

    def insert_average_fpy_into_influxdb(self, table_name, df, shift_period):
        time_in_db = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        threshold = 0.1*(df["TOTAL"].mean())
        df_cal = df[df["TOTAL"].gt(threshold)]
        df_cal = df_cal[df_cal["TOTAL"].gt(30)]
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
            self.insert_into_influxdb(self.factory_name, json, 'm', 10000)
    
    def insert_average_daily_fpy_into_influxdb(self, table_name, df, time_in_db):
        threshold = 0.1*(df["TOTAL"].mean())
        df_cal = df[df["TOTAL"].gt(threshold)]
        df_cal = df_cal[df_cal["TOTAL"].gt(100)]
        if not df_cal.empty:
            df_cal = df_cal[df_cal.index!="Others"]
            daily_average_fpy = df_cal["Yield"].mean()
            json = [{
                    "measurement": table_name,
                    "time":time_in_db.strftime('%Y-%m-%dT00:00:00Z'),
                    "tags":{"STATIONTYPE": "TOTAL"},
                    "fields":{"PASS": df["PASS"].sum(),
                              "FAIL": df["FAIL"].sum(),
                              "Yield": daily_average_fpy}
                    }]
            print(json)
            self.insert_into_influxdb(self.factory_name, json, 'm', 10000)

    def insert_weekly_or_monthly_fpy_to_influxdb(self, factory_name, interval='WEEK'):
        df, field_name_list, interval_num = self.check_week_or_month_interval_num(interval)
        df = self.df_fpy_preprocessing(df)
        df = self.df_calc_station_type_yield(df)
        threshold = 0
        df_cal = df[df["TOTAL"].gt(threshold)]
        if not df_cal.empty:
            df_cal = df_cal[df_cal.index!="Others"]
            average_fpy = df_cal["Yield"].mean()
            json = [{
                    "measurement": self.table_name,
                    "time":(datetime.now() - timedelta(days=self.shift_day)).strftime('%Y-%m-%dT00:00:00Z'),
                    "fields":{field_name_list[0]:interval_num,
                              field_name_list[1]:df_cal["PASS"].sum(),
                              field_name_list[2]:df_cal["FAIL"].sum(),
                              field_name_list[3]:average_fpy}
            }]
            print(json)
            self.insert_into_influxdb(factory_name, json, 'm', 10000)

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

class ProductionTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(ProductionTemplate, self).__init__()

    def df_drop_data_of_this_hour(self, df, col_name, shift_hour):
        df[col_name] = pd.to_datetime(df[col_name])
        df['hour'] = df.DateCompleted.apply(lambda x: x.hour)
        df = df[df['hour']<(datetime.now() - timedelta(hours=shift_hour)).hour]
        df = df.drop(columns=['hour']).reset_index(drop=True)
        return df

    def df_combine_multiple_df(self, file_list, col_name_list):
        df = pd.DataFrame(columns=col_name_list)
        for save_file in file_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df = pd.concat([df, df_sub], axis=0, ignore_index=True)
        return df

    def df_all_save_file(self, factory_name, save_file_name, shift_period):
        col_name_list = ['MODEL', 'MSN', 'DateCompleted']
        df = self.df_combine_multiple_df(save_file_name, col_name_list)
        df = self.df_drop_data_of_this_hour(df, 'DateCompleted', shift_period)
        return df

    def if_savefile_and_targetfile_exist(self, factory_name, df_save_file, target_file_name):
        target_total, model_target_dict = self.targetfile_preprocessing(target_file_name)
        df_save_file['DateCompleted'] = pd.to_datetime(df_save_file['DateCompleted']) - pd.Timedelta(hours=8)
        json = []
        json_daily = []
        model_list = df_save_file['MODEL'].unique()
        for model in model_list:
            df_model = df_save_file[df_save_file['MODEL']==model]
            if model in model_target_dict['DAILY_TARGET'].keys():
                self.display_current_time("If model exist: Actual: O / Target: O")
                acc_production = len(df_model)
                daily_target = model_target_dict['DAILY_TARGET'][model]
                achievement_rate = 100*acc_production/daily_target
                if achievement_rate>100:
                    achievement_rate == 100.0
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
            else:
                self.display_current_time("If model exist: Actual: O / Target: X")
                acc_production = len(df_model)
                daily_target = acc_production
                achievement_rate = 100*acc_production/daily_target
                if achievement_rate>100:
                    achievement_rate == 100.0
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        print(json)
        print(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)
        
        json = []
        json_daily = []
        for model in model_target_dict['DAILY_TARGET'].keys():
            if model not in df_save_file['MODEL'].unique():
                acc_production = 0
                daily_target = model_target_dict['DAILY_TARGET'][model]
                achievement_rate = 100*acc_production/daily_target
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        print(json)
        print(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def if_only_savefile_exist(self, factory_name, df_save_file):
        target_total = 0.0
        df_save_file['DateCompleted'] = pd.to_datetime(df_save_file['DateCompleted']) - pd.Timedelta(hours=8)
        json = []
        json_daily = []
        model_list = df_save_file['MODEL'].unique()
        for model in model_list:
            df_model = df_save_file[df_save_file['MODEL']==model]
            acc_production = len(df_model)
            daily_target = acc_production
            target_total += daily_target
            achievement_rate = 100*acc_production/daily_target
            json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        print(json)
        print(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def if_only_targetfile_exist(self, factory_name, target_file_name):
        target_total, model_target_dict = self.targetfile_preprocessing(target_file_name)
        json = []
        json_daily = []
        for model in model_target_dict['DAILY_TARGET'].keys():
            acc_production = 0
            daily_target = model_target_dict['DAILY_TARGET'][model]
            achievement_rate = 100*acc_production/daily_target
            json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        print(json)
        print(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def json_point_daily_production(self, model, acc_production, daily_target, daily_target_for_report):
        tags = {"model":model}
        fields = {"act_production":acc_production, "target_production":daily_target, "target4report": daily_target_for_report}
        json_point = self.influxdb_json_point(self.table_name_daily, self.time_in_db_daily.strftime('%Y-%m-%dT00:00:00Z'), tags, fields)
        return json_point

    def json_point_hourly_production(self, model, acc_production, daily_target, target_total, achievement_rate):
        tags = {"model":model}
        fields = {"acc_production":acc_production, "daily_target":daily_target, "daily_target_tot": target_total, "production_rate":achievement_rate}
        json_point = self.influxdb_json_point(self.table_name, self.time_in_db.strftime('%Y-%m-%dT%H:12:00Z'), tags, fields)
        return json_point

    def json_for_target_and_daily(self, json, json_daily, model, acc_production, daily_target, target_total, achievement_rate):
        json_point = self.json_point_hourly_production(model, acc_production, daily_target, target_total, achievement_rate)
        json.append(json_point)
        daily_target_for_report = 0 if acc_production==0 else acc_production if acc_production<=daily_target*0.1 else daily_target
        json_point = self.json_point_daily_production(model, acc_production, daily_target, daily_target_for_report)
        json_daily.append(json_point)
        return json, json_daily

    def targetfile_preprocessing(self, target_file_name):
        df_target_file = pd.read_csv(self.folder_path+target_file_name[0])
        df_target_file = df_target_file.dropna(axis=0, how='any')
        target_total = df_target_file['DAILY_TARGET'].sum()
        target_total = float(target_total)
        model_target_dict = target_total.set_index('MODEL').to_dict()
        return target_total, model_target_dict

class CycletimeTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(CycletimeTemplate, self).__init__()