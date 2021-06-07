import pandas as pd
import os
from datetime import datetime
from influxdb import InfluxDBClient

class CdeWip:
    def __init__(self):
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/WIP/"
        self.table_name = "WIP"

    def cde_info(self):
        file_name_regex = "PE11"
        sheet_name = "PE11庫待修狀況"
        return file_name_regex, sheet_name

    def cde_wfr(self):
        file_name_regex, sheet_name = self.cde_info()
        df = self.get_df(file_name_regex, sheet_name)
        table_name = self.table_name
        df_wfr = df.copy()
        df_wfr = df_wfr.iloc[:, 1:]
        string_col = [m for m, n in enumerate(df_wfr.columns) if type(n) != type(datetime.now())]
        df_wfr.drop(df_wfr.columns[string_col], axis=1, inplace=True)
        df_wfr = df_wfr.dropna(how='all')
        json = []
        for column in df_wfr.columns:
            if not pd.isna(df_wfr[column][0]):
                json_point = {
                        "measurement": table_name,
                        "time":column,
                        "fields":{"WFR": df_wfr[column][0],
                                  "Target": df_wfr[column][1]}
                        }
                json.append(json_point)
        for i in json:
            print(i)
        self.insert_into_influxdb(database='CDE', json_body=json)

    def cde_input_output(self):
        table_name = self.table_name
        file_name_regex, sheet_name = self.cde_info()
        df = self.get_df(file_name_regex, sheet_name, skiprows=range(0,2))
        df = df.iloc[:, 1:11]
        index_for_total = []
        for index in range(len(df)):
            if "Total" in str(df.iloc[index, 0]):
                index_for_total.append(index)
        index_for_total = index_for_total[0]
        df = df.iloc[[index_for_total], 2:].reset_index(drop=True)
        df_input = df.iloc[:, [0,2,4,6]]
        df_output = df.iloc[:, [1,3,5,7]]
        df_output.columns = df_input.columns
        json = []
        for col in df_input.columns:
            json_point = {"measurement":table_name,
                    "time":col,
                    "fields":{"input":df_input[col][0]}}
            json.append(json_point)
        self.insert_into_influxdb(database='CDE', json_body=json)
        for col in df_output.columns:
            json_point = {"measurement":table_name,
                    "time":col,
                    "fields":{"output":df_output[col][0]}}
            json.append(json_point)
        self.insert_into_influxdb(database='CDE', json_body=json)

    def get_df(self, file_name_regex, sheet_name, skiprows=range(0,1)):
        folder_path = self.folder_path
        excel_name = [i for i in os.listdir(folder_path) if "PE11" in i]
        file_name = folder_path + excel_name[0]
        df = pd.read_excel(file_name, sheet_name=sheet_name, skiprows = skiprows, na_values=None)
        return df

    def insert_into_influxdb(self, database, json_body, time_precison='m'):
        client = InfluxDBClient(host='10.118.251.78', port=8086, username="cbn", password="cbn@cbn", database=database)
        client.write_points(json_body, time_precision=time_precison)

class FactoryWip:
    def __init__(self):
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/WIP/"
        self.table_name = "WIP"

    def main(self, factory_name):
        excel_name = [i for i in os.listdir(self.folder_path) if "WIP" in i]
        file_name = self.folder_path + excel_name[0]
        df = pd.read_excel(file_name, sheet_name="WIP2", na_values=None)
        df = df.sort_values(by="Date").reset_index(drop=True)
        df = df.loc[:, ["Date", factory_name]]
        df["Change"] = df[factory_name].diff()
        df = df.iloc[1:, :].reset_index(drop=True)

        mask = df["Change"] >= 0
        df["Input"] = df["Change"].mask(~mask)
        df["Output"] = df["Change"].mask(mask).abs()
        df = df.fillna(value=0)

        df["Date"] = pd.to_datetime(df["Date"])
        json = []
        for index in range(len(df)):
            json_point = {
                    "measurement": "WIP",
                    "time": df["Date"][index],
                    "fields":{"WIP": df[factory_name][index],
                              "Target": 500,
                              "input": df["Input"][index],
                              "output": df["Output"][index]}
            }
            json.append(json_point)
        for i in json:
            print(i)
        self.insert_into_influxdb(database=factory_name, json_body=json)

    def insert_into_influxdb(self, database, json_body, time_precison='m'):
        client = InfluxDBClient(host='10.118.251.78', port=8086, username="cbn", password="cbn@cbn", database=database)
        client.write_points(json_body, time_precision=time_precison)

if __name__=="__main__":
    cde_wip = CdeWip()
    try:
        cde_wip.cde_wfr()
        cde_wip.cde_input_output()
    except Exception as e:
        print(e)

    flex_twowing_wip = FactoryWip()
    factory_name_list = ["FLEX", "TwoWing"]
    for factory_name in factory_name_list:
        try:
            flex_twowing_wip.main(factory_name)
        except Exception as e:
            print(e)
