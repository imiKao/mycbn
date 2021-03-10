import pandas as pd
import os
from factory_template import FactoryBasicTemplate
from datetime import datetime, timedelta

class FacotryMac(FactoryBasicTemplate):
    def __init__(self):
        super(FacotryMac, self).__init__()
        self.table_name = "MAC"

    def main(self, factory_name, shift_period=0):
        self.shift_period = shift_period + self.factory_jet_lag[factory_name]
        date_time = datetime.now() - timedelta(hours=self.shift_period)
        self.shift_hour = (datetime.now() - timedelta(hours=self.shift_period)).strftime('%Y%m%d-%H')
        date = date_time.strftime('%Y%m%d')
        time_in_db = date_time - timedelta(hours=8)
        folder_path = os.getcwd() + "/" + "Raw_data/%s/"%(date)
        
        file_name_list = [i for i in os.listdir(folder_path) if factory_name in i and "Mac" in i and self.shift_hour in i and "!" not in i]
        if file_name_list==[]:
            print("File does not exist.")
            return 0
        excel_name = folder_path + file_name_list[0]
        print(excel_name)
        df = pd.read_csv(excel_name, header=None)
        df = df[0].str.split(":", expand=True)
        df.columns = ['MAC', 'Count']
        df['Count'] = df['Count'].astype('int')
        print(df)
        json = []
        for index in range(len(df)):
            tags = {"MAC":df['MAC'][index]}
            fields = {"Count": df["Count"][index]}
            json_point = self.influxdb_json_point(self.table_name, time_in_db.strftime("%Y-%m-%dT%H:12:00Z"), tags, fields)
            json.append(json_point)
        print(json)
        self.insert_into_influxdb(factory_name, json, "m", 10000)

if __name__=="__main__":
    fac_mac = FacotryMac()
    day_range = 1
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        for shift_period in reversed(range(day_range)):
            try:
                fac_mac.main(factory_name, shift_period)
            except:
                print('Exception')
