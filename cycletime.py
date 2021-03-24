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
        df = df[['MODEL', 'MSN', 'TEST_TIME', 'STATION_TYPE', 'STATION_NAME', 'PORT', 'ITEM_VALUE', 'USL']]
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"int", "USL":"float64"})
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"str", "ITEM_VALUE":"int"})
        df["PORT"] = df["PORT"].str.zfill(2)
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])

        self.display_current_time("Step4-2: Time column preprocessing")
        df["SECONDS"] = pd.to_timedelta(df["ITEM_VALUE"], "s")
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)

        df["START_TIME"] = df["TEST_TIME"] - df["SECONDS"]
        df_cycletime = df.copy(deep=True)
        df = df[["MODEL", "TEST_TIME", "START_TIME", "STATION_TYPE", "STATION_PORT"]]

        self.display_current_time("Step4-3: Calculate cycle-time quartile")
        df_cycletime = df_cycletime[["MODEL", "MSN", "TEST_TIME", "STATION_TYPE", "STATION_PORT", "ITEM_VALUE", "USL"]]
        self.insert_cycletime_raw(factory_name, df_cycletime, self.table_name_raw)

        self.display_current_time("Step4-4: Create dataframe for periodic wave plot")
        start_col = ["MODEL", "START_TIME", "STATION_TYPE", "STATION_PORT"]
        end_col = ["MODEL", "TEST_TIME", "STATION_TYPE", "STATION_PORT"]
        start_onesecond_col = ["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT"]
        end_onesecond_col = ["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT"]
        res = self.create_periodic_wave_of_cycletime(df, start_col, end_col, start_onesecond_col, end_onesecond_col)

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
        df["PORT"] = df["PORT"].str.zfill(2)
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)

        self.display_current_time("Step4-2: Calculate cycle-time quartile & insert into influxdb")
        df = df[["MODEL", "TEST_TIME", "STATION_TYPE", "STATION_PORT", "ITEM_VALUE"]]
        print(df.head())
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

class Idletime(CycletimeTemplate):
    def __init__(self):
        super(Idletime, self).__init__()
        self.table_name_idle = "CYCLE_TIME_IDLE"
        self.factory_jet_lag_day = {"CDE": 0, "TwoWing": 0, "FLEX": 1}

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('factory = %s'%(factory_name))
        self.shift_period = self.factory_jet_lag_day[factory_name] + shift_period
        self.today = (datetime.now() - timedelta(days=self.shift_period)).strftime('%Y%m%d')
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
        df =self.df_column_choose_value(df=df_all, col_name="TEST_ITEM", choose_value="CYCLETIME")
        df = df[['MODEL', 'MSN', 'TEST_TIME', 'STATION_TYPE', 'STATION_NAME', 'PORT', 'ITEM_VALUE', 'USL']]
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"int", "USL":"float64"})
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"str", "ITEM_VALUE":"int"})
        df["PORT"] = df["PORT"].str.zfill(2)
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])

        self.display_current_time("Step4-2: Time column preprocessing")
        df = df.sort_values(by=["TEST_TIME"])
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"])
        df = df.set_index("TEST_TIME")
        print(df.head())

        self.display_current_time("Step5: Preprocess by classify work & break time interval and insert data into influxdb")
        for model in df["MODEL"].unique():
            df_model = df[df["MODEL"]==model]
            for station_port in df_model["STATION_PORT"].unique():
                df_station_port = df_model[df_model["STATION_PORT"]==station_port]
                df_res = pd.DataFrame(columns=["TEST_ITEM", "MODEL", "STATION_TYPE", "STATION_PORT", "IDLE_TIME"])
                day_working_time_period = [("08:00", "11:39"), ("13:00", "16:39"), ("17:00", "19:39")]
                for period_tuple in day_working_time_period:
                    df_period = self.df_classify_by_work_and_break_time(df_station_port, period_tuple)
                    df_res = pd.concat([df_res, df_period], axis=0, ignore_index=True)
                df_res = df_res.reset_index(drop=True)
                json = []
                for index in range(len(df_res)):
                    json_point = {
                            "measurement": self.table_name_idle,
                            "time": df_res["TEST_TIME"][index],
                            "tags": {"MODEL":df_res["MODEL"][index],
                                     "STATION_TYPE": df_res["STATION_TYPE"][index],
                                     "STATION_PORT": df_res["STATION_PORT"][index]},
                            "fields": {"IDLE_TIME":df_res["IDLE_TIME"][index]}
                    }
                    json.append(json_point)
                print(json)
                self.insert_into_influxdb(factory_name, json, None, 10000)

if __name__=="__main__":
    def runCycleTime(run_class, factory_list, time_period):
        run_cycletime = run_class
        for factory_name in factory_list:
            for period in reversed(range(time_period)):
                try:
                    run_cycletime.main(factory_name, period)
                except:
                    print('Exception')

    factory_list = ["CDE", "FLEX", "TwoWing"]
    """========== HOURLY CPK =========="""
    running_hour = 8
    runCycleTime(Cycletime(), factory_list, running_hour)

    """========== DAILY CPK =========="""
    running_day = 1
    runCycleTime(CycletimeRolling(), factory_list, running_day)
    runCycleTime(Idletime(), factory_list, running_day)