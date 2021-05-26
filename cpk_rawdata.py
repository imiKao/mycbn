import numpy as np
import pandas as pd
import os
import time
from factory_template import CpkTemplate
from datetime import datetime, timedelta

class FactoryCpkRawdata(CpkTemplate):
    def __init__(self):
        super(FactoryCpkRawdata, self).__init__()
        self.table_name = "TEST_STATION_INFO"

    def main(self, factory_name, shift_period=0, period_type="hour"):
        time_start = time.time()
        self.display_current_time("Host = %s"%(self.host))
        self.display_current_time("Factory = %s"%(factory_name))
        table_name = self.table_name
        database = factory_name + '_Rawdata'
        self.shift_period = shift_period

        self.display_current_time("========== Get dataframe")
        if period_type=="hour":
            df = self.main_hourly_data(shift_hour=self.shift_period, factory_name=factory_name)
        elif period_type=="day":
            df = self.main_daily_data(shift_day=shift_period, factory_name=factory_name)

        if df.empty:
            return 0
        df = self.df_change_column_to_numeric(df, ['ITEM_VALUE', 'LSL', 'USL'])
        df['TEST_TIME'] = pd.to_datetime(df['TEST_TIME']) - pd.Timedelta(hours=8)
        df = self.df_drop_item_in_col(df, 'TEST_ITEM', ['UPTIME', 'CYCLETIME'])
        df.loc[df["STATION_NAME"].str.contains("M200-VOICE7"), "STATION_TYPE"] = "M200-Voice"

        self.display_current_time("========== Insert data into influxdb")
        json = []
        for index in range(len(df)):
            tags = {
                "MODEL":df["MODEL"][index],
                "STATION_TYPE":df["STATION_TYPE"][index],
                "STATION_NAME":df["STATION_NAME"][index],
                "TEST_ITEM":df["TEST_ITEM"][index]
            }
            fields = {
                "ITEM_VALUE":df["ITEM_VALUE"][index],
                "LSL":df["LSL"][index],
                "USL":df["USL"][index],
                "MSN":df["MSN"][index]
            }
            fields_LSL_nan = {
                "ITEM_VALUE":df["ITEM_VALUE"][index],
                "USL":df["USL"][index],
                "MSN":df["MSN"][index]
            }
            fields_USL_nan = {
                "ITEM_VALUE":df["ITEM_VALUE"][index],
                "LSL":df["LSL"][index],
                "MSN":df["MSN"][index]
            }
            if np.isnan(df["LSL"][index]):
                json_point = self.influxdb_json_point(measurement=table_name,
                                                      timestamp=df["TEST_TIME"][index],
                                                      tags_dict=tags,
                                                      fields_dict=fields_LSL_nan)
                json.append(json_point)
            elif np.isnan(df["USL"][index]):
                json_point = self.influxdb_json_point(measurement=table_name,
                                                      timestamp=df["TEST_TIME"][index],
                                                      tags_dict=tags,
                                                      fields_dict=fields_USL_nan)
                json.append(json_point)
            else:
                json_point = self.influxdb_json_point(measurement=table_name,
                                                      timestamp=df["TEST_TIME"][index],
                                                      tags_dict=tags,
                                                      fields_dict=fields)
                json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(database, json, None, 10000)

        time_end = time.time()
        time_c = time_end - time_start
        print("time cost:", time_c, "s")

    def main_daily_data(self, shift_day, factory_name):
        date_time = datetime.now() - timedelta(days=shift_day)
        print(date_time)
        today = date_time.strftime('%Y%m%d')
        folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(today)
        save_file_name_list = [i for i in os.listdir(folder_path) if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            df = pd.DataFrame(index=range(1,10))
            print("File does not exist.")
            return df
        df = pd.concat(self.load_files(save_file_name_list, folder_path))
        df = df.reset_index(drop=True)
        return df

    def main_hourly_data(self, shift_hour, factory_name):
        shift_hour = shift_hour + self.factory_jet_lag[factory_name]
        date_time = datetime.now() - timedelta(hours=shift_hour)
        print(date_time)
        today = date_time.strftime('%Y%m%d')
        current_hour = date_time.strftime('%Y%m%d-%H')
        folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(today)
        save_file_name_list = [i for i in os.listdir(folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            df = pd.DataFrame(index=range(1,10))
            print("File does not exist.")
            return df
        save_file_name = save_file_name_list[0]
        df = pd.read_csv(folder_path+save_file_name)
        return df

if __name__=="__main__":
    cpk_rawdata = FactoryCpkRawdata()
    time_range = 8
    for factory_name in ['CDE', 'FLEX', 'TwoWing']:
        for shift_time in reversed(range(time_range)):
            try:
                cpk_rawdata.main(factory_name=factory_name, shift_period=shift_time, period_type="hour")   # period_type: hour / day
            except Exception as e:
                print(e)
