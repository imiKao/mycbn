import pandas as pd
import os
from factory_template import FpyTemplate
from datetime import datetime, timedelta

class FpyHourly(FpyTemplate):
    def __init__(self):
        super(FpyHourly, self).__init__()
        self.table_name_FPY = "FPY"
        self.table_name_FPY_STATION = "FPY_STATION"
    
    def main(self, factory_name, shift_period):
        self.display_current_time("========== Step1: Start setting time & other information")
        self.factory_name = factory_name
        print('host = %s'%(self.host))
        self.shift_period = shift_period
        shift_period = self.factory_jet_lag[self.factory_name] + self.shift_period
        date_time = datetime.now() - timedelta(hours=shift_period)
        today = date_time.strftime('%Y%m%d')
        folder_path = os.path.dirname(os.getcwd()) + "/Raw_data/%s/"%(today)

        self.display_current_time("========== Step2: Start to get save file list")
        this_hour_file_list = self.get_this_hour_data(folder_path, shift_period)
        this_hour_file = this_hour_file_list[0] if this_hour_file_list!=[] else ""
        last_hour_file_list = self.get_last_hour_data(folder_path, shift_period)
        last_hour_file = last_hour_file_list[0] if last_hour_file_list!=[] else ""
        today_file = self.get_today_file(folder_path)

        self.display_current_time("========== Step3: Start reading dataset & check multiple situation")
        if this_hour_file!="" and last_hour_file!="":
            self.display_current_time("===== Step3-1: Check if file exist: This hour(O) / Last hour(O)")
            self.if_file_this_hour_exist_and_last_hour_exist(shift_period, folder_path, this_hour_file, last_hour_file)
        elif this_hour_file!="" and last_hour_file=="" and this_hour_file==today_file[0]:
            self.display_current_time("===== Step3-2: Check if file exist: This hour is the first file today")
            self.if_file_only_this_hour_exist(shift_period, folder_path, this_hour_file, today_file)
        elif this_hour_file!="" and last_hour_file=="":
            self.display_current_time("===== Step3-3: Check if file exist: This hour(O) / Last hour(X)")
            print("Last hour file does not exist.")
        elif this_hour_file=="" and last_hour_file!="":
            self.display_current_time("===== Step3-4: Check if file exist: This hour(X) / Last hour(O)")
            this_hour_file = last_hour_file_list[0]
            this_hour = (datetime.now() - timedelta(hours=shift_period)).strftime('%Y%m%d-%H')
            df = pd.read_csv(folder_path+last_hour_file_list[0])
            csv_name = folder_path + '%s_'%(self.factory_name) + this_hour + '0000_Yield.csv'
            df.to_csv(csv_name, index=0, encoding="utf_8_sig")
            print("This hour file does not exist.")
        else:
            self.display_current_time("===== Step3-5: Check if file exist: This hour(X) / Last hour(X)")
            print("File does not exist.")

    def if_file_this_hour_exist_and_last_hour_exist(self, shift_period, folder_path, this_hour_file, last_hour_file):
        table_name_FPY = self.table_name_FPY
        table_name_FPY_STATION = self.table_name_FPY_STATION
        time_in_db = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        df = self.two_file_substract(folder_path, this_hour_file, last_hour_file)
        df = self.df_drop_row_by_particular_string(df, "Station", ["OBA", "REP", "SAGE", "PING", "EPIDO", "GIGA_CHECK", "MTTNSDSCAL", "CBNSGDSCAL"])
        self.display_current_time("========== Step: Uniform Station format as STATION_NAME column")
        df = self.fpy_uniform_station_format_equal_to_station_name(df)
        self.display_current_time("========== Step: FPY calculation preprocessing")
        df_station = self.df_fpy_station_preprocessing(df)
        df_station.loc[:, "Total"] = df_station.loc[:, "PASS"] + df_station.loc[:, "FAIL"]
        df_station.loc[:, "Yield"] = df_station.loc[:, "PASS"] / (df_station.loc[:, "Total"]+0.00001)
        df_station.loc[:, "Total"] = df_station.loc[:, "Total"].dropna(axis=0).reset_index(drop=True)
        df_station = df_station[df_station["Total"]!=0].reset_index(drop=True)
        self.insert_station_hourly_fpy_to_influxdb(table_name_FPY_STATION, df_station, time_in_db)
        df = self.df_fpy_preprocessing(df)
        df = self.df_calc_station_type_yield(df)
        json = []
        for index in range(len(df)):
            tags = {"STATION_TYPE": df.index[index]}
            fields = {"PASS": df["PASS"][index], "FAIL": df["FAIL"][index], "Yield": df["Yield"][index]}
            json_point = self.influxdb_json_point(table_name_FPY, time_in_db.strftime('%Y-%m-%dT%H:12:00Z'), tags, fields)
            json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(self.factory_name, json, 'm', 10000)
        self.insert_average_fpy_into_influxdb(table_name_FPY, df, shift_period)

    def if_file_only_this_hour_exist(self, shift_period, folder_path, this_hour_file, today_file):
        table_name_FPY = self.table_name_FPY
        table_name_FPY_STATION = self.table_name_FPY_STATION
        time_in_db = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        df = pd.read_csv(folder_path+this_hour_file)
        df = self.df_drop_row_by_particular_string(df, "Station", ["OBA", "REP", "SAGE", "PING", "EPIDO", "GIGA_CHECK", "MTTNSDSCAL", "CBNSGDSCAL"])
        self.display_current_time("========== Step: Uniform Station format as STATION_NAME column")
        df = self.fpy_uniform_station_format_equal_to_station_name(df)
        self.display_current_time("========== Step: FPY calculation preprocessing")
        df_station = self.df_fpy_station_preprocessing(df)
        df_station.loc[:, "Total"] = df_station.loc[:, "PASS"] + df_station.loc[:, "FAIL"]
        df_station.loc[:, "Yield"] = df_station.loc[:, "PASS"] / (df_station.loc[:, "Total"]+0.00001)
        df_station = df_station[df_station["Total"]!=0].reset_index(drop=True)
        self.insert_station_hourly_fpy_to_influxdb(table_name_FPY_STATION, df_station, time_in_db)
        df = self.df_fpy_preprocessing(df)
        df = self.df_calc_station_type_yield(df)
        json = []
        for index in range(len(df)):
            tags = {"STATION_TYPE": df.index[index]}
            fields = {"PASS": df["PASS"][index], "FAIL": df["FAIL"][index], "Yield": df["Yield"][index]}
            json_point = self.influxdb_json_point(table_name_FPY, time_in_db.strftime('%Y-%m-%dT%H:12:00Z'), tags, fields)
            json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(self.factory_name, json, 'm', 10000)
        self.insert_average_fpy_into_influxdb(table_name_FPY, df, shift_period)

class FpyDaily(FpyTemplate):
    def __init__(self):
        super(FpyDaily, self).__init__()
        self.table_name = "FPY_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        self.factory_name = factory_name
        print("factory = {}".format(self.factory_name))
        print('host = {}'.format(self.host))
        shift_period = self.factory_jet_lag[self.factory_name] + shift_period
        date_time = datetime.now() - timedelta(hours=shift_period)
        today = date_time.strftime('%Y%m%d')
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(today)
        self.time_in_db = date_time

        self.display_current_time("========== Step2: Start to get save file list")
        all_hour_file_list = [i for i in os.listdir(self.folder_path) if self.factory_name in i and "Yield" in i ]
        all_hour_file_list = sorted(all_hour_file_list, reverse=True)
        if all_hour_file_list==[]:
            print("File does not exist.")
            return 0
        this_hour_file = all_hour_file_list[0]

        self.display_current_time("========== Step3: Start reading dataset & preprocessing dataframe")
        df = pd.read_csv(self.folder_path+this_hour_file)
        df = df.drop(['Yield'], axis=1)
        df = self.fpy_uniform_station_format_equal_to_station_name(df)
        df = self.df_fpy_station_preprocessing(df)
        df = self.df_calc_station_type_yield(df)

        self.display_current_time("========== Step4: Start insert into database")
        self.insert_station_type_daily_fpy_to_influxdb(self.table_name, df, self.time_in_db)
        self.insert_average_daily_fpy_into_influxdb(self.table_name, df, self.time_in_db)

if __name__=="__main__":
    def runFpy(run_class, factory_list, time_period):
        run_fpy = run_class
        for factory_name in factory_list:
            for period in reversed(range(time_period)):
                try:
                    run_fpy.main(factory_name, period)
                except Exception as e:
                    print(e)

    factory_list = ["CDE", "FLEX", "TwoWing"]
    hour_range = 8

    runFpy(FpyHourly(), factory_list, hour_range)
    runFpy(FpyDaily(), factory_list, hour_range)


    
    
    