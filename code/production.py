import pandas as pd
import os
from factory_template import ProductionTemplate
from datetime import datetime, timedelta

class ProductionHourly(ProductionTemplate):
    def __init__(self):
        super(ProductionHourly, self).__init__()
        self.table_name = "PRODUCTION"
        self.db_time_minutes = 12   # XX:12:00

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('host = %s'%(self.host))
        table_name = self.table_name
        self.shift_period = self.factory_jet_lag[factory_name] + shift_period
        date_time = datetime.now() - timedelta(hours=self.shift_period)
        today = date_time.strftime('%Y%m%d')
        current_hour = date_time.strftime('%Y%m%d-%H')
        folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(today)

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(folder_path) if current_hour in i and "INFO" in i and factory_name in i and "TEST_STATION_INFO" not in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step3: Start reading dataset")
        df = pd.read_csv(folder_path+save_file_name_list[0])
        df = self.df_drop_data_of_this_hour(df, col_name='DateCompleted', shift_hour=self.shift_period)
        df['DateCompleted'] = pd.to_datetime(df['DateCompleted']) - pd.Timedelta(hours=8)
        json = []
        model_list = df['MODEL'].unique()
        for model in model_list:
            df_model = df[df['MODEL']==model]
            df_production = df_model.groupby(pd.Grouper(key='DateCompleted', freq='H')).size()
            df_production.index = df_production.index + timedelta(minutes=self.db_time_minutes)
            production_sum = float(sum(df_production))
            tags = {"model":model}
            fields = {"productive":production_sum}
            json = [{
                "measurement": table_name,
                "time":df_production.index[0],
                "tags":tags,
                "fields":fields
            }]
            self.print_json_log(json)
            self.insert_into_influxdb(factory_name, json, 'm', 10000)

class ProductionDaily(ProductionTemplate):
    def __init__(self):
        super(ProductionDaily, self).__init__()
        self.table_name = "PRODUCTION"
        self.table_name_daily = "PRODUCTION_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('host = %s'%(self.host))
        self.shift_period = self.factory_jet_lag[factory_name] + shift_period
        date_time = datetime.now() - timedelta(hours=self.shift_period)
        today = date_time.strftime('%Y%m%d')
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(today)
        self.time_in_db = datetime.now() - timedelta(hours=self.shift_period+1) - timedelta(hours=8)
        self.time_in_db_daily = datetime.now() - timedelta(hours=self.shift_period)

        self.display_current_time("========== Step2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if "INFO" in i and factory_name in i and "TEST_STATION_INFO" not in i and "!" not in i]
        target_file_name_list = [i for i in os.listdir(self.folder_path) if today in i and factory_name in i and "production_target" in i and "!" not in i]

        self.display_current_time("========== Step3: Start multiple situation")
        if save_file_name_list!=[]:
            df_save_file = self.df_all_save_file(factory_name, save_file_name_list, self.shift_period)
        if target_file_name_list!=[] and save_file_name_list!=[]:
            self.display_current_time("Save and target file all exist")
            self.if_savefile_and_targetfile_exist(factory_name, df_save_file, target_file_name_list)
        elif target_file_name_list==[] and save_file_name_list!=[]:
            self.display_current_time("Only save file all exist")
            self.if_only_savefile_exist(factory_name, df_save_file)
        elif target_file_name_list!=[] and save_file_name_list==[]:
            self.display_current_time("Only target file all exist")
            self.if_only_targetfile_exist(factory_name, target_file_name_list)
        else:
            print("File does not exist!")

if __name__=="__main__":
    def runProduction(run_class, factory_list, time_period):
        run_prod = run_class
        for factory_name in factory_list:
            for period in reversed(range(time_period)):
                try:
                    run_prod.main(factory_name, period)
                except Exception as e:
                    print(e)

    factory_list = ["CDE", "FLEX", "TwoWing"]
    hour_range = 8
    """========== Hourly production =========="""
    runProduction(ProductionHourly(), factory_list, hour_range)

    """========== Daily Production & production target =========="""
    runProduction(ProductionDaily(), factory_list, hour_range)
