import numpy as np
import pandas as pd
import os
from factory_template import CpkTemplate
from datetime import datetime, timedelta

class CpkDaily(CpkTemplate):
    def __init__(self):
        super(CpkDaily, self).__init__()
        self.table_name = "PREPROCESS_CPK_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        table_name = self.table_name
        date_time = (datetime.now() - timedelta(days=shift_period))
        print('host = {}'.format(self.host))
        print("Date: {}".format(date_time.strftime("%Y-%m-%d")))

        self.display_current_time("========== Step2: Start to get save file list")
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(date_time.strftime('%Y%m%d'))
        save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        self.display_current_time("Save file list = {}".format(save_file_name_list))

        self.display_current_time("========== Step3: Start reading dataset")
        column_list = ['MODEL', 'TEST_TIME', 'MSN', 'STATION_TYPE', 'LINE', 'STATION_NAME', 'TEST_ITEM', 'PORT', 'ITEM_VALUE', 'LSL', 'USL']
        df_all = pd.DataFrame(columns=column_list)
        for save_file in save_file_name_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df_all = pd.concat([df_all, df_sub], axis=0, ignore_index=True)
        self.display_current_time(df_all.head())

        self.display_current_time("========== Step4: Start preprocessing dataframe")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"

        self.display_current_time("========== Step5: Start inserting data")
        station_type_list = df_all["STATION_TYPE"].unique()
        model_list = df_all['MODEL'].unique()
        json = []
        for model_name in model_list:  # Model for-loop
            df = df_all[df_all['MODEL']==model_name]
            for station_type in station_type_list:  # StationType for-loop
                """A. Dataframe preprocessing"""
                df_station_type = df.loc[df["STATION_TYPE"]==station_type].reset_index(drop=True)
                df_test_item_list = [i for i in df_station_type["TEST_ITEM"].unique()]
                df_test_item_list = self.list_drop_item(df_test_item_list, ['UPTIME', 'CYCLETIME'])
                temp_list = [elem for elem in df_test_item_list if 'SNR' in str(elem) or '_2' in str(elem) or str(elem)=="nan"]
                df_test_item_list = self.list_drop_item(df_test_item_list, temp_list)

                """B. Calculate Cpk and insert into influxdb"""
                for item in df_test_item_list:
                    df_new = df_station_type.loc[df_station_type["TEST_ITEM"]==item].reset_index(drop=True)
                    avg, std, cpk, lsl, usl, count = self.cpk_calc(df_new)
                    cp = self.cp_calc(df_new)
                    ck = self.ck_calc(df_new)
                    level_score = self.leveling_by_cp_and_ca(cp, ck)
                    df_cpk = pd.DataFrame({'TEST_TIME':date_time.strftime('%Y-%m-%d'), 'AVG':avg, 'STD':std, 'CPK':cpk, 'LSL':lsl, 'USL':usl, 'num':count, 'CP':cp, 'CK':ck, 'level': level_score}, index=[0]).set_index('TEST_TIME')
                    for index in range(len(df_cpk)):
                        if df_cpk["CPK"][index]!=None:
                            tags = {"MODEL":df_new["MODEL"][0],
                                    "STATION_TYPE":df_new["STATION_TYPE"][0],
                                    "TEST_ITEM":item}
                            fields_ori = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index], "CP":df_cpk["CP"][index], "CK":df_cpk["CK"][index], "LEVEL":df_cpk["level"][index]}
                            fields_no_lsl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index], "CP":df_cpk["CP"][index], "LEVEL":df_cpk["level"][index]}
                            fields_no_usl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"COUNT":df_cpk["num"][index], "CP":df_cpk["CP"][index], "LEVEL":df_cpk["level"][index]}
                            if np.isnan(df_cpk["LSL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_lsl)
                                json.append(json_point)
                            elif np.isnan(df_cpk["USL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_usl)
                                json.append(json_point)
                            else:
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_ori)
                                json.append(json_point)
        print(json)
        self.insert_into_influxdb(database=factory_name, json_body=json, time_precison='m', batch_size=10000)

class CpkDailyEachStation(CpkTemplate):
    def __init__(self):
        super(CpkDailyEachStation, self).__init__()
        self.table_name = "PREPROCESS_CPK"

    def main(self, factory_name, shift_period=0):
        print('host = {}'.format(self.host))
        self.display_current_time("========== Step1: Start setting time & other information =====")
        table_name = self.table_name
        self.shift_period = shift_period
        shift_period = self.shift_period + self.factory_jet_lag[factory_name]
        date_time = (datetime.now() - timedelta(hours=shift_period))
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(date_time.strftime('%Y%m%d'))

        self.display_current_time("========== Step2: Start to get save file list =====")
        current_hour = date_time.strftime('%Y%m%d-%H')
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        self.display_current_time("Save file list = {}".format(save_file_name_list))
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]

        self.display_current_time("========== Step3: Start reading dataset =====")
        df_all = pd.read_csv(self.folder_path+save_file_name)
        self.display_current_time(df_all.head())

        self.display_current_time("========== Step4: Start preprocessing dataframe =====")
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)

        self.display_current_time("========== Step5: Start inserting data =====")
        station_name_list = df_all["STATION_NAME"].unique()
        model_list = df_all['MODEL'].unique()
        json = []
        for model_name in model_list:  # all model for-loop
            df = df_all[df_all['MODEL']==model_name]
            for station_name in station_name_list:  # all station for-loop
                """A. Dataframe preprocessing"""
                df_station_name = df.loc[df["STATION_NAME"]==station_name].reset_index(drop=True)
                df_test_item_list = [i for i in df_station_name["TEST_ITEM"].unique()]
                df_test_item_list = self.list_drop_item(df_test_item_list, ['UPTIME', 'CYCLETIME'])
                temp_list = [elem for elem in df_test_item_list if 'SNR' in str(elem) or '_2' in str(elem) or str(elem)=="nan"]
                df_test_item_list = self.list_drop_item(df_test_item_list, temp_list)
                if df_test_item_list==[]:
                    continue
                """B. Calculate Cpk and insert into influxdb"""
                for item in df_test_item_list:
                    df_new = df_station_name.loc[df_station_name["TEST_ITEM"]==item].reset_index(drop=True)
                    df_cpk = df_new.groupby(pd.Grouper(key='TEST_TIME', freq='H')).apply(self.cpk_cal_to_df)
                    for index in range(len(df_cpk)):
                        if not np.isnan(df_cpk["CPK"][index]):
                            tags = {"MODEL":df_new["MODEL"][0],
                                    "STATION_TYPE":df_new["STATION_TYPE"][0],
                                    "STATION_NAME":df_new["STATION_NAME"][0],
                                    "TEST_ITEM":item}
                            fields_ori = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index]}
                            fields_no_lsl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index]}
                            fields_no_usl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"COUNT":df_cpk["num"][index]}
                            if np.isnan(df_cpk["LSL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_lsl)
                                json.append(json_point)
                            elif np.isnan(df_cpk["USL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_usl)
                                json.append(json_point)
                            else:
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_ori)
                                json.append(json_point)
        print(json)
        self.insert_into_influxdb(database=factory_name, json_body=json, time_precison='m', batch_size=10000)

class CpkHourly(CpkTemplate):
    def __init__(self):
        super(CpkHourly, self).__init__()
        self.table_name = "PREPROCESS_CPK_HOURLY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('host = %s'%(self.host))
        self.shift_period = shift_period
        table_name = self.table_name
        shift_period = self.shift_period + self.factory_jet_lag[factory_name]
        current_hour = (datetime.now() - timedelta(hours=shift_period)).strftime('%Y%m%d-%H')
        date_time = (datetime.now() - timedelta(hours=shift_period))
        time_in_json = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(date_time.strftime('%Y%m%d'))

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]

        self.display_current_time("========== Step3: Start reading dataset")
        df_all = pd.read_csv(self.folder_path+save_file_name)

        self.display_current_time("========== Step4: Start preprocessing dataframe =====")
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)

        self.display_current_time("========== Step5: Start inserting data =====")
        station_name_list = df_all["STATION_TYPE"].unique()
        model_list = df_all['MODEL'].unique()
        json = []
        for model_name in model_list:
            df = df_all[df_all['MODEL']==model_name]
            for station_name in station_name_list:
                """A. Dataframe preprocessing"""
                df_station_name = df.loc[df["STATION_TYPE"]==station_name].reset_index(drop=True)  
                df_test_item_list = [i for i in df_station_name["TEST_ITEM"].unique()]
                df_test_item_list = self.list_drop_item(df_test_item_list, ['UPTIME', 'CYCLETIME'])
                temp_list = [elem for elem in df_test_item_list if 'SNR' in str(elem) or '_2' in str(elem) or str(elem)=="nan"]
                df_test_item_list = self.list_drop_item(df_test_item_list, temp_list)
                if df_test_item_list==[]:
                    continue
                check_all_type_nodata = dict()
                """B. Calculate Cpk and insert into influxdb"""
                for item in df_test_item_list:
                    df_new = df_station_name.loc[df_station_name["TEST_ITEM"]==item].reset_index(drop=True)
                    check_all_type_nodata[item] = len(df_new)
                    df_cpk = df_new.groupby(pd.Grouper(key='TEST_TIME', freq='H')).apply(self.cpk_cal_to_df)
                    for index in range(len(df_cpk)):
                        if not np.isnan(df_cpk["CPK"][index]):
                            tags = {"MODEL":df_new["MODEL"][0],
                                    "STATION_TYPE":df_new["STATION_TYPE"][0],
                                    "TEST_ITEM":item}
                            fields_ori = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index]}
                            fields_no_lsl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"USL":df_cpk["USL"][index],"COUNT":df_cpk["num"][index]}
                            fields_no_usl = {"AVG":df_cpk["AVG"][index],"STD":df_cpk["STD"][index],"CPK":df_cpk["CPK"][index],"LSL":df_cpk["LSL"][index],"COUNT":df_cpk["num"][index]}
                            if np.isnan(df_cpk["LSL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_lsl)
                                json.append(json_point)
                            elif np.isnan(df_cpk["USL"][index]):
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_no_usl)
                                json.append(json_point)
                            else:
                                json_point = self.influxdb_json_point(measurement=table_name, timestamp=df_cpk.index[index], tags_dict=tags, fields_dict=fields_ori)
                                json.append(json_point)
                """C. Check no data is all-empty or data count<30"""
                json_check_all_type_nodata = self.check_all_type_nodata(check_all_type_nodata, table_name, time_in_json, model_name, station_name)
                if len(check_all_type_nodata)==len(json_check_all_type_nodata) :
                    self.insert_into_influxdb(database=factory_name, json_body=json_check_all_type_nodata, time_precison='m', batch_size=10000)
                    
        print(json)
        self.insert_into_influxdb(database=factory_name, json_body=json, time_precison='m', batch_size=10000)

    def check_all_type_nodata(self, check_all_type_nodata, table_name, time_in_json, model_name, station_name):
        json_check_all_type_nodata = []
        for k, v in check_all_type_nodata.items():
            if v>=30:
                break
            tags = {"MODEL":model_name, "STATION_TYPE":station_name, "TEST_ITEM":k}
            fields = {"COUNT":float(v)}
            json_point = self.influxdb_json_point(table_name, time_in_json.strftime('%Y-%m-%dT%H:00:00Z'), tags, fields)
            json_check_all_type_nodata.append(json_point)
        return json_check_all_type_nodata


if __name__=="__main__":
    daily_cpk = CpkDaily()
    day_range = 2
    for factory in ["CDE", "FLEX", "TwoWing"]:
        for shift_period in reversed(range(day_range)):
            try:
                daily_cpk.main(factory_name=factory, shift_period=shift_period)
            except:
                print('Exception')

    daily_cpk_station = CpkDailyEachStation()
    hourly_cpk_station = CpkHourly()
    hour_range = 8
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        for shift_hour in reversed(range(hour_range)):
            try:
                daily_cpk_station.main(factory_name=factory_name, shift_period=shift_hour)
                hourly_cpk_station.main(factory_name=factory_name, shift_period=shift_hour)
            except:
                print('Exception')
    
    
