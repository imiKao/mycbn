import pandas as pd
import numpy as np
import os
from factory_template import FactoryBasicTemplate, Ftp
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

class FpyQ(FactoryBasicTemplate, Ftp):
    def __init__(self):
        super(FpyQ, self).__init__()
        self.table_name = "FPY_Q"

    def main(self, factory_name, shift_period):
        weekno = (datetime.now() - timedelta(days=shift_period)).weekday()
        self.day = shift_period+2 if weekno==6 else shift_period
        file_date = datetime.now() - timedelta(days=self.day)
        file_date_style = '%YY.%-m.%-d'   # e.g. 2021Y.3.18
        file_name_date = file_date.strftime(file_date_style)
        self.time_in_db = datetime.now() - timedelta(days=self.day)
        folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(file_date.strftime('%Y%m%d'))

        """Get data from ftp"""
        yesterday_for_date_point = file_date.strftime(file_date_style)
        print(yesterday_for_date_point)
        self.catch_fpy_q_file_from_ftp(folder_path, yesterday_for_date_point)
        file_name_list = [i for i in os.listdir(folder_path) if file_name_date in i and "FPY" in i ]
        sheet_name = self.time_in_db.strftime('%m')
        if file_name_list == []:
            print("File does not exist")
            return 0
        client = InfluxDBClient(host=self.host, port=8086, username="cbn", password="cbn@cbn", database=factory_name)
        excel_name = folder_path + file_name_list[0]
        df = pd.read_excel(excel_name, sheet_name=sheet_name, skiprows = range(0, 3), na_values=None)
        df_day = df.copy()
        self.insert_daily_fpy_q_into_influxdb(client, df_day)
        df_week = df.copy()
        self.insert_weekly_fpy_q_into_influxdb(client, df_week)
        df_month = df.copy()
        self.insert_monthly_fpy_q_into_influxdb(client, df_month)

    def insert_daily_fpy_q_into_influxdb(self, client, df):
        date = self.time_in_db.strftime('%d').lstrip('0')
        df.columns = pd.Series([np.nan if 'Unnamed:' in str(x) else x for x in df.columns.values]).ffill().values.flatten()
        title_day = [i for i in df.columns if str(date)==str(i)][0]
        title_day_index = [index for index, item in enumerate(df.columns) if str(title_day)==str(item)]
        if title_day_index!=[]:
            daily_fpy = self.df_fpy_preprocess_for_CDE(df, title_day_index, finalchk_name='FINALCHK')
            if daily_fpy is not None:
                json = [{
                    "measurement": self.table_name,
                    "time": self.time_in_db.strftime('%Y-%m-%dT00:00:00Z'),
                    "fields": {"Daily_FPY":daily_fpy*100.0}
                }]
                print(json)
                client.write_points(json, time_precision='m')

    def insert_weekly_fpy_q_into_influxdb(self, client, df):
        weeknum = (datetime.now() - timedelta(days=self.day-1)).isocalendar()[1]
        weeknum += 1
        df.columns = pd.Series([np.nan if 'Unnamed:' in str(x) else x for x in df.columns.values]).ffill().values.flatten() 
        title_week = [i for i in df.columns if str(weeknum) in str(i) and 'Week' in str(i)][0]
        title_week_index = [index for index, item in enumerate(df.columns) if str(title_week) in str(item)]
        if title_week_index!=[]:
            weekly_fpy = self.df_fpy_preprocess_for_CDE(df, title_week_index, finalchk_name='FINALCHK')
            if weekly_fpy is not None:
                json = [{
                    "measurement": self.table_name,
                    "time": self.time_in_db.strftime('%Y-%m-%dT00:00:00Z'),
                    "fields": {"Weekly_FPY":weekly_fpy*100.0,
                               "Weeknum":weeknum}
                }]
                print(json)
                client.write_points(json, time_precision='m')

    def insert_monthly_fpy_q_into_influxdb(self, client, df):
        month_title = self.time_in_db.strftime("%b")
        df.columns = pd.Series([np.nan if 'Unnamed:' in str(x) else x for x in df.columns.values]).ffill().values.flatten()
        title_month = [i for i in df.columns if str(month_title) in str(i)][0]
        title_month_index = [index for index, item in enumerate(df.columns) if str(title_month) in str(item)]
        if title_month_index!=[]:
            monthly_fpy = self.df_fpy_preprocess_for_CDE(df, title_month_index, finalchk_name='FINALCHK')
            if monthly_fpy is not None:
                json = [{
                    "measurement": self.table_name,
                    "time": self.time_in_db.strftime('%Y-%m-%dT00:00:00Z'),
                    "fields": {"Monthly_FPY":monthly_fpy*100.0,
                               "Month":month_title}
                }]
                print(json)
                client.write_points(json, time_precision='m')

    def catch_fpy_q_file_from_ftp(self, folder_path, condition):
        ftp = self.ftp_login()
        filenamelist = self.ftp_get_filename_list(ftp=ftp)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        today_file_list = [item for item in filenamelist if condition in item]
        if today_file_list!=[]:
            self.ftp_copy_file_to_local(ftp=ftp, today_file_list=today_file_list, folder_path=folder_path)
            self.ftp_delete_file(ftp=ftp, today_file_list=today_file_list)
        else:
            print("File does not exist.")
        ftp.set_debuglevel(0)
        ftp.quit()

    def df_fpy_preprocess_for_CDE(self, df, title_name_index_list, finalchk_name='FINALCHK'):
        df = pd.concat([df[['Models', 'Group']], df.iloc[:, title_name_index_list]], axis=1)
        df = df.iloc[1:, :].reset_index(drop=True)
        for i in ['SMT', 'FPY']:
            df = df[df['Group']!=i].reset_index(drop=True)
        df = df.iloc[:, :6]
        df.columns = ['Models', 'Group', 'Yield', 'PASS', 'FAIL', 'TOTAL']
        model_list = df['Models'].unique().tolist()
        model_list = [i for i in model_list if str(i)!='nan']
        result = 0
        tot = 0
        for model in model_list:
            df_model = df[df['Models']==model].reset_index(drop=True)
            if pd.isnull(df_model['Yield']).all():
                continue
            if df_model.loc[df_model['Group']==finalchk_name, 'TOTAL'].values[0]==0 or pd.isna(df_model.loc[df_model['Group']==finalchk_name, 'TOTAL'].values[0]):
                continue
            product = 1
            for i in df_model['Yield'].tolist():
                if str(i) not in ['nan', '0', 'None']:
                    product*=i
            finalchk_tot = df_model.loc[df_model['Group']==finalchk_name, 'TOTAL'].values[0]
            result += (product*finalchk_tot)
            tot += finalchk_tot
        if str(tot)!='0':
            result /= tot
            return result
        return None

if __name__=="__main__":
    fpy_q = FpyQ()
    shift_period = 1   # last 1 day
    for factory_name in ["CDE"]:
        try:
            fpy_q.main(factory_name, shift_period)
        except Exception as e:
            print(e)
