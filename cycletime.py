import os
import pandas as pd
from factory_template import CycletimeTemplate
from datetime import datetime, timedelta

class Cycletime(CycletimeTemplate):
    def __init__(self):
        super(Cycletime, self).__init__()
        self.table_name = "CYCLE_TIME"
        self.table_name_wave = "CYCLE_TIME_WAVE"
        self.table_name_raw = "CYCLE_TIME_RAW"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('factory = %s'%(factory_name))
        self.shift_period = self.factory_jet_lag[factory_name] + shift_period
        self.today = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d')
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(self.today)
        current_hour = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d-%H')

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "FAIL_result" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]
        print(save_file_name)

        self.display_current_time("========== Step3: Start reading dataset")
        df = pd.read_csv(self.folder_path+save_file_name)
        reorder_columns = ["TEST_TIME", "MODEL", "MSN", "CSN", "STATION_TYPE", "STATION_NAME", "PORT", "CYCLETIME", "TEST_RESULT"]
        df = df[reorder_columns]

        self.display_current_time("========== Step4: Insert amount of pass & fail data")
        self.cycletime_insert_hourly_amount_pass_fail(df, factory_name, self.table_name)

        self.display_current_time("========== Step5: Data preprosseing & insert data into database")
        df_pass = df[df["TEST_RESULT"]==1].reset_index(drop=True)
        df_pass = self.cycletime_data_preprocseeing(df_pass)
        self.cycletime_insert_raw_data(factory_name, df_pass, self.table_name_raw, "PASS")
        df_pass = self.cycletime_periodic_wave_plot(df_pass)
        self.cycletime_insert_wave_plot_data(factory_name, df_pass, self.table_name_wave, "PASS")
        df_fail = df[df["TEST_RESULT"]==0].reset_index(drop=True)
        df_fail = self.cycletime_data_preprocseeing(df_fail)
        self.cycletime_insert_raw_data(factory_name, df_fail, self.table_name_raw, "FAIL")
        df_fail = self.cycletime_periodic_wave_plot(df_fail)
        self.cycletime_insert_wave_plot_data(factory_name, df_fail, self.table_name_wave, "FAIL")

class Idletime(CycletimeTemplate):
    def __init__(self):
        super(Idletime, self).__init__()
        self.table_name_idle = "CYCLE_TIME_IDLE"
        self.factory_jet_lag_day = {"CDE": 0, "TwoWing": 0, "FLEX": 1}

    def main(self, factory_name, day_working_time_period_dict, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('factory = %s'%(factory_name))
        self.shift_period = self.factory_jet_lag_day[factory_name] + shift_period
        self.today = (datetime.now() - timedelta(days=self.shift_period)).strftime('%Y%m%d')
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(self.today)

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "FAIL_result" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step3: Start reading dataset")
        df = pd.DataFrame(columns=["MODEL", "MSN", "CSN", "TEST_TIME", "STATION_TYPE", "STATION_NAME", "TEST_RESULT", "CYCLETIME", "PORT"])
        for save_file in save_file_name_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df = pd.concat([df, df_sub], axis=0, ignore_index=True)
        df = df.reset_index(drop=True)

        self.display_current_time("========== Step4: Start preprocessing dataframe")
        self.display_current_time("Step4-1: Normal column process")
        df = df[["MODEL", "MSN", "CSN", "TEST_TIME", "STATION_TYPE", "STATION_NAME", "PORT", "CYCLETIME"]]
        df = self.pass_fail_data_combine_stationtype(df)
        df = self.pass_fail_data_combine_station_and_port(df)
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])

        self.display_current_time("Step4-2: Time column preprocessing")
        df = df.sort_values(by=["TEST_TIME"])
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"])
        df = df.set_index("TEST_TIME")

        self.display_current_time("Step5: Preprocess by classify work & break time interval and insert data into influxdb")
        for model in df["MODEL"].unique():
            df_model = df[df["MODEL"]==model]
            for station_port in df_model["STATION_PORT"].unique():
                df_station_port = df_model[df_model["STATION_PORT"]==station_port]
                df_res = pd.DataFrame(columns=["TEST_ITEM", "MODEL", "STATION_TYPE", "STATION_PORT", "IDLE_TIME"])
                day_working_time_period = day_working_time_period_dict[factory_name]
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
                self.print_json_log(json)
                self.insert_into_influxdb(factory_name, json, None, 10000)

if __name__=="__main__":
    factory_list = ["CDE", "FLEX", "TwoWing"]
    """========== Cycle-time =========="""
    run_cycletime = Cycletime()
    running_hour = 8
    for factory_name in factory_list:
        for period in reversed(range(running_hour)):
            try:
                run_cycletime.main(factory_name, period)
            except:
                print("Exception")

    """========== Idle-time =========="""
    run_idletime = Idletime()
    running_day = 1
    day_working_time_period_dict = {
            "CDE": [("08:00", "11:39"), ("13:00", "16:39"), ("17:00", "19:39")],
            "FLEX": [("00:00", "02:19"), ("07:00", "11:29"), ("13:00", "16:49"), ("17:00", "19:29"), ("21:30", "23:59")],
            "TwoWing": [("00:39", "05:39"), ("05:40", "08:09"), ("08:20", "11:49"), ("12:50", "17:19"), ("18:10", "20:39"), ("20:50", "23:49")]
    }
    for factory_name in factory_list:
        for period in reversed(range(running_day)):
            try:
                run_idletime.main(factory_name, day_working_time_period_dict, period)
            except Exception as e:
                print(e)
