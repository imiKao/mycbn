import os
import pandas as pd
from factory_template import CycletimeTemplate
from datetime import datetime, timedelta

class Cycletime(CycletimeTemplate):
    def __init__(self):
        super(Cycletime, self).__init__()
        self.table_name = "CYCLE_TIME"
        self.table_name_raw = "CYCLE_TIME_RAW"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('factory = %s'%(factory_name))
        self.shift_period = self.factory_jet_lag[factory_name] + shift_period
        self.today = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d')
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(self.today)
        current_hour = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d-%H')

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]

        self.display_current_time("========== Step3: Start reading dataset")
        df_all = pd.read_csv(self.folder_path+save_file_name)

        self.display_current_time("========== Step4: Start preprocessing dataframe")
        self.display_current_time("Step4-1: Normal column process")
        df =self.df_column_choose_value(df=df_all, col_name="TEST_ITEM", choose_value="CYCLETIME")
        df = df[['MODEL', 'MSN', 'TEST_TIME', 'STATION_TYPE', 'STATION_NAME', 'PORT', 'ITEM_VALUE']]
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"int"})
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"str", "ITEM_VALUE":"int"})
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])

        self.display_current_time("Step4-2: Time column process")
        df["SECONDS"] = pd.to_timedelta(df["ITEM_VALUE"], "s")
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)
        df["START_TIME"] = df["TEST_TIME"] - df["SECONDS"]
        df_cycletime = df.copy(deep=True)
        df = df[["MODEL", "TEST_TIME", "START_TIME", "STATION_TYPE", "STATION_PORT"]]

        self.display_current_time("Step4-3: Calculate cycle-time quartile")
        df_cycletime = df_cycletime[["MODEL", "MSN", "TEST_TIME", "STATION_TYPE", "STATION_PORT", "ITEM_VALUE"]]
        self.insert_cycletime_raw(df_cycletime)

        self.display_current_time("Step4-4: Divided test start and end dataframe")
        df_start = df.copy(deep=True)
        df_end = df.copy(deep=True)
        df_start = df_start[["MODEL", "START_TIME", "STATION_TYPE", "STATION_PORT"]]
        df_start = self.df_rename_column(df=df_start, rename_dict={"START_TIME":"TEST_TIME"})
        df_end = df_end[["MODEL", "TEST_TIME", "STATION_TYPE", "STATION_PORT"]]

        self.display_current_time("Step4-5: Add onesecond before start and onesecond after end dataframe")
        df_start_one = df_start.copy(deep=True)
        df_end_one = df_end.copy(deep=True)
        timediff = pd.Timedelta(1, unit='s')
        df_start_one["ONESECOND"] = df_start_one["TEST_TIME"] - timediff
        df_end_one["ONESECOND"] = df_end_one["TEST_TIME"] + timediff
        df_start_one = df_start_one[["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT"]]
        df_end_one = df_end_one[["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT"]]
        df_start_one = self.df_rename_column(df=df_start_one, rename_dict={"ONESECOND":"TEST_TIME"})
        df_end_one = self.df_rename_column(df=df_end_one, rename_dict={"ONESECOND":"TEST_TIME"})

        self.display_current_time("Step4-6: Give a threshold to display on grafana dashboard")
        df_start["CHECK"] = 1
        df_end["CHECK"] = 1
        df_start_one["CHECK"] = 0
        df_end_one["CHECK"] = 0
        res = pd.concat([df_start, df_end, df_start_one, df_end_one], axis=0)
        res = self.df_column_sort_value(df=res, col_name="TEST_TIME")

        self.display_current_time("========== Step5: Start inserting data")
        json = []
        for index in range(len(res)):
            json_point = {
                    "measurement": self.table_name,
                    "time": res["TEST_TIME"][index],
                    "tags": {"MODEL":res["MODEL"][index],
                            "STATION_TYPE": res["STATION_TYPE"][index],
                            "STATION_PORT": res["STATION_PORT"][index]},
                    "fields": {"CHECK":res["CHECK"][index]}
            }
            json.append(json_point)
        print(json)
        self.insert_into_influxdb(factory_name, json, None, 10000)

    def insert_cycletime_raw(self, df_cycletime):
        json = []
        for index in range(len(df_cycletime)):
            json_point = {
                    "measurement": self.table_name_raw,
                    "time": df_cycletime["TEST_TIME"][index],
                    "tags": {"MODEL":df_cycletime["MODEL"][index],
                             "STATION_TYPE": df_cycletime["STATION_TYPE"][index],
                             "STATION_PORT": df_cycletime["STATION_PORT"][index]},
                    "fields": {"CYCLE_TIME": df_cycletime["ITEM_VALUE"][index],
                               "MSN": df_cycletime["MSN"][index]}
            }
            json.append(json_point)
        print(json)
        self.insert_into_influxdb(factory_name, json, None, 10000)

class CycletimeRolling(CycletimeTemplate):
    def __init__(self):
        super(CycletimeRolling, self).__init__()
        self.table_name_raw = "CYCLE_TIME_RAW"
        self.factory_jet_lag_day = {"CDE": 0, "TwoWing": 0, "FLEX": 1}

    def main(self, factory_name, shift_day=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('factory = %s'%(factory_name))
        self.shift_day = shift_day
        shift_day = self.factory_jet_lag_day[factory_name] + self.shift_day
        self.today = (datetime.now() - timedelta(days=shift_day)).strftime('%Y%m%d')
        self.folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(self.today)

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step3: Start reading dataset")
        df_all = pd.DataFrame(columns=['TEST_TIME', 'MODEL', 'MSN', 'STATION_TYPE', 'LINE', 'STATION_NAME', \
                                       'TEST_ITEM', 'PORT', 'ITEM_VALUE', 'LSL', 'USL'])
        for save_file in save_file_name_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df_all = pd.concat([df_all, df_sub], axis=0, ignore_index=True)
        df_all = df_all.reset_index(drop=True)

        self.display_current_time("========== Step4: Start preprocessing dataframe")
        self.display_current_time("Step4-1: Normal column process")
        df = self.df_column_choose_value(df=df_all, col_name="TEST_ITEM", choose_value="CYCLETIME")
        df = df[['MODEL', 'TEST_TIME', 'STATION_TYPE', 'STATION_NAME', 'PORT', 'ITEM_VALUE']]
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"int"})
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"str", "ITEM_VALUE":"int"})
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)

        self.display_current_time("Step4-2: Calculate cycle-time quartile & insert into influxdb")
        df = df[["MODEL", "TEST_TIME", "STATION_TYPE", "STATION_PORT", "ITEM_VALUE"]]
        model_list = df["MODEL"].unique()
        json = []
        for model in model_list:
            df_model = df[df["MODEL"]==model]
            station_port = df_model["STATION_PORT"].unique()
            for station in station_port:
                df_station = df_model[df_model["STATION_PORT"]==station].reset_index(drop=True)
                if len(df_station)<=3:
                    continue
                df_station["rolling_mean"] = df_station["ITEM_VALUE"].rolling(window=3, center=True).mean()
                print(df_station.head())
                for index in range(len(df_station)):
                    json_point = {
                            "measurement": self.table_name_raw,
                            "time": df_station["TEST_TIME"][index],
                            "tags": {"MODEL":df_station["MODEL"][index],
                                     "STATION_TYPE": df_station["STATION_TYPE"][index],
                                     "STATION_PORT": df_station["STATION_PORT"][index]},
                            "fields": {"ROLL_MEAN": df_station["rolling_mean"][index]}
                    }
                    json.append(json_point)
        print(json)
        self.insert_into_influxdb(factory_name, json, None, 10000)

if __name__=="__main__":
    cycletime = Cycletime()
    running_hour = 8
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        for shift_hour in reversed(range(running_hour)):
            try:
                cycletime.main(factory_name, shift_hour)
            except:
                print('Exception')

    cycletime_rolling = CycletimeRolling()
    running_day = 1
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        for shift_day in reversed(range(running_day)):
            try:
                cycletime_rolling.main(factory_name, shift_day)
            except:
                print('Exception')