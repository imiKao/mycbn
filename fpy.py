import os
import pandas as pd
from datetime import datetime, timedelta
from factory_template import FpyTemplate, CycletimeTemplate

class Fpy(FpyTemplate, CycletimeTemplate):
    def __init__(self):
        super(Fpy, self).__init__()
        self.table_name_hour_passfail = "FPY_HOUR_PASS_FAIL"
        self.table_name_hour_firstpassyield = "FPY_HOUR_FIRST_PASS_YIELD"
        self.table_name_day_passfail = "FPY_DAY_PASS_FAIL"
        self.table_name_day_firstpassyield = "FPY_DAY_FIRST_PASS_YIELD"
        self.factory_jet_lag_day = {"CDE": 0, "TwoWing": 0, "FLEX": 1}

    def main(self, factory_name, period_type="Hour", shift_period=0):
        self.display_current_time("========== Step1: Setting information")
        if period_type=="Hour":
            self.shift_period = self.factory_jet_lag[factory_name] + shift_period
            self.today = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d')
            current_hour = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d-%H')
            self.time_in_db = datetime.now() - timedelta(hours=self.shift_period+1) - timedelta(hours=8)
        elif period_type=="Day":
            self.shift_period = self.factory_jet_lag_day[factory_name] + shift_period
            self.today = (datetime.now() - timedelta(days=self.shift_period)).strftime('%Y%m%d')
            print(self.today)
            self.time_in_db = datetime.now() - timedelta(days=self.shift_period)
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(self.today)

        self.display_current_time("========== Step2: Get save file list")
        if period_type=="Hour":
            save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "FAIL_result" in i and "!" not in i]
        elif period_type=="Day":
            save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "FAIL_result" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step3: Dataframe")
        if period_type=="Hour":
            save_file_name = save_file_name_list[0]
            df = pd.read_csv(self.folder_path+save_file_name)
            reorder_columns = ["TEST_TIME", "MODEL", "STATION_TYPE", "STATION_NAME", "PORT", "TEST_RESULT"]
            df = df[reorder_columns]
        elif period_type=="Day":
            df = self.df_pass_fail_empty_df()
            for save_file in save_file_name_list:
                df_sub = pd.read_csv(self.folder_path+save_file)
                df = pd.concat([df, df_sub], axis=0, ignore_index=True)
            df = df.reset_index(drop=True)

        self.display_current_time("========== Step4: Data preprocessing")
        df = self.pass_fail_data_combine_stationtype(df)
        df_pass = self.fpy_df_pass_fail_count(df, "PASS")
        df_fail = self.fpy_df_pass_fail_count(df, "FAIL")
        df = pd.merge(df_pass, df_fail, on=["MODEL", "STATION_TYPE", "STATION_NAME"], how="outer")
        df = df.where(pd.notnull(df), 0)
        df_cal_fpy = df.copy(deep=True)
        df_cal_fpy = self.cal_fpy_groupby_stationtype(df_cal_fpy, factory_name, period_type)

        self.display_current_time("========== Step5: Insert into influxdb")
        if period_type=="Hour":
            time_in_json = self.time_in_db.strftime('%Y-%m-%dT%H:12:00Z')
            table_name = self.table_name_hour_passfail
        elif period_type=="Day":
            time_in_json = self.time_in_db.strftime('%Y-%m-%dT00:00:00Z')
            table_name = self.table_name_day_passfail
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index], "STATION_TYPE": df["STATION_TYPE"][index], "STATION_NAME": df["STATION_NAME"][index]}
            fields = {"PASS": df["PASS"][index], "FAIL": df["FAIL"][index]}
            json_point = self.influxdb_json_point(table_name, time_in_json, tags, fields)
            json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

if __name__=="__main__":
    def run_fpy(running_period, period_type="Hour"):
        fpy = Fpy()
        for factory_name in factory_name_list:
            for period in reversed(range(running_period)):
                try:
                    fpy.main(factory_name, period_type, period)
                except Exception as e:
                    print(e)

    factory_name_list = ["CDE", "FLEX", "TwoWing"]
    run_fpy(8, "Hour")
    run_fpy(2, "Day")
