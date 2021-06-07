import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import time
from influxdb import InfluxDBClient
from ftplib import FTP
from datetime import datetime, timedelta

class FactoryBasicTemplate(object):
    def __init__(self):
        self.host = "10.118.251.78"
        self.factory_jet_lag = {"CDE": 0, "TwoWing": 0, "FLEX": 12}
        self.jenkins_home = "/home/cbn_generic_2/jenkins_slave/workspace/Factory_CPK/Factory_catch_file_from_ftp"

    def insert_into_influxdb(self, database, json_body, time_precision='m', batch_size=10000):
        host = self.host
        client = InfluxDBClient(host=host, port=8086, username="cbn", password="cbn@cbn", database=database)
        client.write_points(json_body, time_precision=time_precision, batch_size=batch_size)

    def display_current_time(self, display_name):
        t = time.localtime()
        a = time.strftime("%H:%M:%S", t)
        print("{}: {}".format(display_name, a))

    def df_change_column_to_numeric(self, df, column_name_list):
        for col_name in column_name_list:
            df[col_name] = df[col_name].apply(pd.to_numeric, args=('coerce',))
        return df

    def df_change_column_type(self, df, col_new_type_dict):
        df = df.astype(col_new_type_dict)
        return df

    def df_combine_two_column(self, df, new_col, col_1, col_2, hy_pen):
        df[new_col] = df[col_1] + hy_pen + df[col_2]
        return df

    def df_column_choose_value(self, df, col_name, choose_value):
        df = df[df[col_name]==choose_value]
        df = df.reset_index(drop=True)
        return df

    def df_column_corr_heatmap_save_png(self, df, save_png_name, png_style='triangle', figsize=(32, 12), text_in_cell=False, cmap='BrBG', title_text='Correlation Heatmap'):
        self.display_current_time("===== Save Correlation of dataframe")
        plt.figure(figsize=figsize)
        """png_style: 'square' or 'triangle'"""
        if png_style=="triangle":
            mask = np.triu(np.ones_like(df.corr(), dtype=np.bool))
            heatmap = sns.heatmap(df.corr(), mask=mask, vmin=-1, vmax=1, annot=text_in_cell, cmap=cmap)
        else:
            heatmap = sns.heatmap(df.corr(), vmin=-1, vmax=1, annot=text_in_cell, cmap=cmap)
        heatmap.set_title(title_text, fontdict={'fontsize': 8}, pad=12)
        plt.savefig(save_png_name, dpi=300, bbox_inches='tight')

    def df_column_search_particular_string(self, df, column_name, target_text):
        df = df[df[column_name].str.contains(target_text, na=False)]
        df = df.reset_index(drop=True)
        res_text = df.iloc[0][column_name]
        return res_text

    def df_column_sort_value(self, df, col_name):
        df = df.sort_values(by=col_name)
        df = df.reset_index(drop=True)
        return df

    def df_drop_column(self, df, drop_column_name_list):
        df = df.drop(columns=drop_column_name_list)
        return df

    def df_drop_row_by_particular_string(self, df, column_name, drop_string_list):
        for drop_str in drop_string_list:
            df = df[~df[column_name].str.contains(drop_str)]
        df = df.reset_index(drop=True)
        return df

    def df_drop_multi_layer_index_by_particular_string(self, df, drop_string_list, index_layer=1):
        """
        level: multi-level index's layer
        """
        for drop_str in drop_string_list:
            try:
                df = df.drop(drop_str, axis=0, level=index_layer-1)
            except:
                continue
        return df

    def df_drop_item_in_col(self, df, col_name, drop_item_list):
        for arg in drop_item_list:
            df_tmp = df[df[col_name]==arg]
            df.drop(df_tmp.index, axis=0, inplace=True)
            df = df.reset_index(drop=True)
        return df

    def df_get_empty_dataframe(self, column_list):
        df = pd.DataFrame(columns=column_list)
        return df

    def df_rename_column(self, df, rename_dict):
        df = df.rename(rename_dict, axis=1)
        return df

    def influxdb_json_point(self, measurement, timestamp, tags_dict, fields_dict):
        json_point = {
            "measurement": measurement,
            "time": timestamp,
            "tags": tags_dict,
            "fields": fields_dict
        }
        return json_point

    def list_drop_item(self, target_list, drop_item_list):
        for item in drop_item_list:
            if item in target_list:
                target_list.remove(item)
        return target_list

    def load_files(self, filename_list, folder_path=os.path.dirname(os.getcwd())):
        for filename in filename_list:
            if "/" in filename:
                yield pd.read_csv(filename)
            else:
                yield pd.read_csv(folder_path + filename)

    def print_json_log(self, json_list):
        for j in json_list:
            print(j)

class Ftp(FactoryBasicTemplate):
    def __init__(self):
        super(Ftp, self).__init__()

    def ftp_login(self, host="ftp2.compalbn.com", user_name="cbn_grafana", password="HGB7Z3mf"):
        ftp=FTP()
        ftp.set_debuglevel(2)
        ftp.connect(host)
        ftp.login(user_name, password)
        return ftp

    def ftp_get_filename_list(self, ftp):
        filelist = []
        ftp.retrlines("LIST", filelist.append)
        filenamelist = []
        for item in filelist:
            words = item.split(None, 8)
            filename = words[-1].lstrip()
            filenamelist.append(filename)
        return filenamelist

    def file_timestamp(self, shift_hour):
        file_timestamp = datetime.now() - timedelta(hours=shift_hour)
        file_timestamp_day = file_timestamp.strftime('%Y%m%d')
        file_timestamp_hour = file_timestamp.strftime('%Y%m%d-%H')
        return file_timestamp_day, file_timestamp_hour

    def ftp_check_file(self, filenamelist, file_timestamp_hour):
        today_file_list = [item for item in filenamelist if file_timestamp_hour in item]
        return today_file_list

    def ftp_copy_file_to_local(self, ftp, today_file_list, local_path):
        bufsize=1024
        for item in today_file_list:
            ftp.retrbinary("RETR %s"%(item), open(local_path+item, 'wb').write, bufsize)

    def ftp_delete_file(self, ftp, today_file_list):
        for item in today_file_list:
            ftp.delete(item)

class CpkTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(CpkTemplate, self).__init__()

    def std_rbar(self, x):
        x= [i for i in x]
        rbar_list = []
        for i in range(1, len(x)):
            rbar_list.append(abs(x[i] - x[i-1]))
        if rbar_list!=[]:
            result = (sum(rbar_list) / len(rbar_list)) / 1.128
        else:
            result = 0
        return result

    def std_calc(self, x):
        rbar_list = []
        for i in range(1, len(x)):
            rbar_list.append(abs(x["ITEM_VALUE"][i]-x["ITEM_VALUE"][i-1]))
        if rbar_list!=[]:
            result = (sum(rbar_list) / len(rbar_list)) / 1.128
        else:
            result = 0
        return result

    def cpk_column(self, df, cpk_col, usl_avg_col, avg_lsl_col):
        df.loc[:, cpk_col] = df[[usl_avg_col, avg_lsl_col]].min(axis=1)
        return df

    def cp_column(self, df, cp_col, usl_lsl_col, usl_avg_col, avg_lsl_col):
        df.loc[:, cp_col] = df.loc[:,usl_lsl_col]
        df.loc[:, cp_col] = np.where(~np.isnan(df.loc[:,cp_col]), df.loc[:,cp_col], df.loc[:,usl_avg_col])
        df.loc[:, cp_col] = np.where(~np.isnan(df.loc[:,cp_col]), df.loc[:,cp_col], df.loc[:,avg_lsl_col])
        return df

    def ck_column(self, df, ck_col, usl_col, lsl_col, avg_col):
        df.loc[:, ck_col] = (((df.loc[:,usl_col]+df.loc[:,lsl_col])/2)-df.loc[:,avg_col])/((df.loc[:,usl_col]-df.loc[:,lsl_col])/2)
        df.loc[:, ck_col] = abs(df.loc[:, ck_col])
        return df

    def level_column_by_cp_ck(self, df, level_col, cp_score_col, ck_score_col, cp_col, ck_col):
        level_score = {"critical": 3, "warning": 1, "normal": 0}
        df.loc[:,cp_score_col] = np.where(df.loc[:,cp_col]<1.0, level_score["critical"], level_score["normal"])
        df.loc[:,cp_score_col] = np.where((df.loc[:,cp_col]>=1.0)&(df.loc[:,cp_col]<1.33), level_score["warning"], df.loc[:,cp_score_col])
        df.loc[:,cp_score_col] = np.where(np.isnan(df.loc[:,ck_col]), df.loc[:,cp_score_col]*1.3, df.loc[:,cp_score_col])
        df.loc[:,ck_score_col] = 1-df.loc[:,ck_col]
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<0.5, level_score["critical"], df.loc[:,ck_score_col])
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<0.75, level_score["warning"], df.loc[:,ck_score_col])
        df.loc[:,ck_score_col] = np.where(df.loc[:,ck_score_col]<=1.0, level_score["normal"], df.loc[:,ck_score_col])
        df.loc[:,level_col] = df.loc[:,cp_score_col].fillna(0) + df.loc[:,ck_score_col].fillna(0)
        return df

    def calc_percentage_for_stdev_area(self, df, column_name, avg, std):
        if std!=None:
            std=std
        else:
            std=1
        cut_list = [-999, avg-3*std, avg-2*std, avg-std, avg+std, avg+2*std, avg+3*std, 999]
        result_series = pd.cut(df[column_name], cut_list, labels=['n3sigma', 'n2sigma', 'n1sigma', 'avg', 'p1sigma', 'p2sigma', 'p3sigma'])
        return result_series

    def get_df_from_cpk_daily_file(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1-1: Start setting time & other information")
        self.date_time = (datetime.now() - timedelta(days=shift_period))
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(self.date_time.strftime('%Y%m%d'))
        self.display_current_time("===== Factory: {}".format(factory_name))
        self.display_current_time("===== Host = {}".format(self.host))
        self.display_current_time("===== Date: {}".format(self.date_time.strftime("%Y-%m-%d")))

        self.display_current_time("========== Step1-2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0

        self.display_current_time("========== Step1-3: Start reading dataset")
        df_all = pd.concat(self.load_files(save_file_name_list, self.folder_path))
        df_all = df_all.reset_index(drop=True)
        return df_all

    def get_df_from_cpk_hourly_file(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1-1: Start setting time & other information")
        self.shift_period = shift_period
        shift_period = self.shift_period + self.factory_jet_lag[factory_name]
        current_hour = (datetime.now() - timedelta(hours=shift_period)).strftime('%Y%m%d-%H')
        self.date_time = (datetime.now() - timedelta(hours=shift_period))
        self.display_current_time("===== Factory: {}".format(factory_name))
        self.display_current_time("===== Host: {}".format(self.host))
        self.display_current_time("===== Time: {}".format(self.date_time.strftime("%Y-%m-%d %H:00:00")))
        self.time_in_json = datetime.now() - timedelta(hours=shift_period+1) - timedelta(hours=8)
        self.folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(self.date_time.strftime('%Y%m%d'))
        self.display_current_time("========== Step1-2: Start to get save file list")
        save_file_name_list = [i for i in os.listdir(self.folder_path) if current_hour in i and factory_name in i and "TEST_STATION_INFO" in i and "!" not in i]
        if save_file_name_list==[]:
            print("File does not exist.")
            return 0
        save_file_name = save_file_name_list[0]
        self.display_current_time("========== Step1-3: Start reading dataset")
        df_all = pd.read_csv(self.folder_path+save_file_name)
        return df_all

    def get_df_cpk(self, df, groupby_columns):
        self.display_current_time("Get amount of item value AVG, STD, USL/LSL columns and Cpk calculation group by columns")
        df = df.groupby(groupby_columns).agg({"ITEM_VALUE": ["mean", self.std_rbar, "size"], "USL": ["mean"], "LSL": ["mean"]})
        df = df[df.loc[:,("ITEM_VALUE","size")].gt(30)]   # Cpk calculation requires amount of data bigger than 30
        df = self.df_drop_multi_layer_index_by_particular_string(df, ["CYCLETIME", "UPTIME"], index_layer=len(groupby_columns))
        df.loc[:, ("CPK","usl_avg")] = (df.loc[:,("USL","mean")] - df.loc[:, ("ITEM_VALUE","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","avg_lsl")] = (df.loc[:, ("ITEM_VALUE","mean")] - df.loc[:,("LSL","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","usl_lsl")] = (df.loc[:,("USL","mean")] - df.loc[:,("LSL","mean")]) / (6 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df = self.cpk_column(df, ("CPK","cpk"), ("CPK","usl_avg"), ("CPK","avg_lsl"))
        return df

    def get_file_name_list_of_multiple_days(self, factory_name, days_num):
        self.date_time = (datetime.now() - timedelta(days=days_num))
        self.folder_layer1 = os.path.dirname(os.getcwd()) + "/" + "Raw_data/"
        date_name_list = [(datetime.now() - timedelta(days=day_num)).strftime('%Y%m%d') for day_num in reversed(range(days_num))]
        folder_name_list = []
        for folder_name in os.listdir(self.folder_layer1):
            if folder_name in date_name_list:
                folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(folder_name)
                folder_name_list.append(folder_path)
            else:
                continue
        save_file_name_list = []
        for folder_name in folder_name_list:
            for i in os.listdir(folder_name):
                if factory_name in i and "TEST_STATION_INFO" in i and "!" not in i:
                    foler_path = folder_name + i
                    save_file_name_list.append(foler_path)
        save_file_name_list = sorted(save_file_name_list)
        return save_file_name_list

class ProductionTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(ProductionTemplate, self).__init__()

    def df_drop_data_of_this_hour(self, df, col_name, shift_hour):
        df[col_name] = pd.to_datetime(df[col_name])
        df['hour'] = df.DateCompleted.apply(lambda x: x.hour)
        df = df[df['hour']<(datetime.now() - timedelta(hours=shift_hour)).hour]
        df = df.drop(columns=['hour']).reset_index(drop=True)
        return df

    def df_combine_multiple_df(self, file_list, col_name_list):
        df = pd.DataFrame(columns=col_name_list)
        for save_file in file_list:
            df_sub = pd.read_csv(self.folder_path+save_file)
            df = pd.concat([df, df_sub], axis=0, ignore_index=True)
        return df

    def df_all_save_file(self, factory_name, save_file_name, shift_period):
        col_name_list = ['MODEL', 'MSN', 'DateCompleted']
        df = self.df_combine_multiple_df(save_file_name, col_name_list)
        df = self.df_drop_data_of_this_hour(df, 'DateCompleted', shift_period)
        return df

    def if_savefile_and_targetfile_exist(self, factory_name, df_save_file, target_file_name):
        target_total, model_target_dict = self.targetfile_preprocessing(target_file_name)
        df_save_file['DateCompleted'] = pd.to_datetime(df_save_file['DateCompleted']) - pd.Timedelta(hours=8)
        json = []
        json_daily = []
        model_list = df_save_file['MODEL'].unique()
        for model in model_list:
            df_model = df_save_file[df_save_file['MODEL']==model]
            if model in model_target_dict['DAILY_TARGET'].keys():
                self.display_current_time("If model exist: Actual: O / Target: O")
                acc_production = len(df_model)
                daily_target = model_target_dict['DAILY_TARGET'][model]
                achievement_rate = 100*acc_production/daily_target
                if achievement_rate>100:
                    achievement_rate == 100.0
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
            else:
                self.display_current_time("If model exist: Actual: O / Target: X")
                acc_production = len(df_model)
                daily_target = acc_production
                achievement_rate = 100*acc_production/daily_target
                if achievement_rate>100:
                    achievement_rate == 100.0
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        self.print_json_log(json)
        self.print_json_log(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

        json = []
        json_daily = []
        for model in model_target_dict['DAILY_TARGET'].keys():
            if model not in df_save_file['MODEL'].unique():
                acc_production = 0
                daily_target = model_target_dict['DAILY_TARGET'][model]
                achievement_rate = 100*acc_production/daily_target
                json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        self.print_json_log(json)
        self.print_json_log(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def if_only_savefile_exist(self, factory_name, df_save_file):
        target_total = 0.0
        df_save_file['DateCompleted'] = pd.to_datetime(df_save_file['DateCompleted']) - pd.Timedelta(hours=8)
        json = []
        json_daily = []
        model_list = df_save_file['MODEL'].unique()
        for model in model_list:
            df_model = df_save_file[df_save_file['MODEL']==model]
            acc_production = len(df_model)
            daily_target = acc_production
            print("daily_target:",daily_target)
            target_total += daily_target
            print("target_total:",target_total)
            achievement_rate = 100*acc_production/daily_target
            json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        self.print_json_log(json)
        self.print_json_log(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def if_only_targetfile_exist(self, factory_name, target_file_name):
        target_total, model_target_dict = self.targetfile_preprocessing(target_file_name)
        json = []
        json_daily = []
        for model in model_target_dict['DAILY_TARGET'].keys():
            acc_production = 0
            daily_target = model_target_dict['DAILY_TARGET'][model]
            achievement_rate = 100*acc_production/daily_target
            json, json_daily = self.json_for_target_and_daily(json, json_daily, model, acc_production, daily_target, target_total, achievement_rate)
        self.print_json_log(json)
        self.print_json_log(json_daily)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.insert_into_influxdb(factory_name, json_daily, 'm', 10000)

    def json_point_daily_production(self, model, acc_production, daily_target, daily_target_for_report):
        tags = {"model":model}
        fields = {"act_production":acc_production, "target_production":daily_target, "target4report": daily_target_for_report}
        json_point = self.influxdb_json_point(self.table_name_daily, self.time_in_db_daily.strftime('%Y-%m-%dT00:00:00Z'), tags, fields)
        return json_point

    def json_point_hourly_production(self, model, acc_production, daily_target, target_total, achievement_rate):
        tags = {"model":model}
        fields = {"acc_production":acc_production, "daily_target":daily_target, "daily_target_tot": target_total, "production_rate":achievement_rate}
        json_point = self.influxdb_json_point(self.table_name, self.time_in_db.strftime('%Y-%m-%dT%H:12:00Z'), tags, fields)
        return json_point

    def json_for_target_and_daily(self, json, json_daily, model, acc_production, daily_target, target_total, achievement_rate):
        json_point = self.json_point_hourly_production(model, acc_production, daily_target, target_total, achievement_rate)
        json.append(json_point)
        daily_target_for_report = 0 if acc_production==0 else acc_production if acc_production<=daily_target*0.1 else daily_target
        json_point = self.json_point_daily_production(model, acc_production, daily_target, daily_target_for_report)
        json_daily.append(json_point)
        return json, json_daily

    def targetfile_preprocessing(self, target_file_name):
        df_target_file = pd.read_csv(self.folder_path+target_file_name[0])
        df_target_file = df_target_file.dropna(axis=0, how='any')
        target_total = df_target_file['DAILY_TARGET'].sum()
        target_total = float(target_total)
        model_target_dict = target_total.set_index('MODEL').to_dict()
        return target_total, model_target_dict

class CycletimeTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(CycletimeTemplate, self).__init__()

    def pass_fail_data_combine_stationtype(self, df):
        df = self.df_drop_row_by_particular_string(df, "STATION_TYPE", ["PREWLANIQ", "GIGACHECK", "FCHECK", "OBA", "RLOSS", "RLoss"])
        df["STATION_TYPE"] = df["STATION_TYPE"].str.replace("FINALCHK", "FinalChk")
        df["STATION_TYPE"] = df["STATION_TYPE"].str.replace("DSCAL", "BCD")
        df["STATION_TYPE"] = df["STATION_TYPE"].str.replace("FUNC", "Func")
        df["STATION_TYPE"] = df["STATION_TYPE"].str.replace("VOICE", "Voice")
        return df

    def pass_fail_data_combine_station_and_port(self, df):
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"int", "CYCLETIME":"int"})
        df = self.df_change_column_type(df, col_new_type_dict={"PORT":"str", "MSN":"str", "CSN":"str"})
        df["PORT"] = df["PORT"].str.zfill(2)
        df = self.df_combine_two_column(df, new_col="STATION_PORT", col_1="STATION_NAME", col_2="PORT", hy_pen="_")
        return df

    def cycletime_data_preprocseeing(self, df):
        df = self.pass_fail_data_combine_stationtype(df)
        df = self.pass_fail_data_combine_station_and_port(df)
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])
        df["SECONDS"] = pd.to_timedelta(df["CYCLETIME"], "s")
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)
        df["END_TIME"] = df["TEST_TIME"] + df["SECONDS"]
        df = df[["MODEL", "TEST_TIME", "END_TIME", "STATION_TYPE", "STATION_PORT", "MSN", "CSN", "CYCLETIME"]]
        df = self.df_rename_column(df=df, rename_dict={"TEST_TIME":"START_TIME"})
        return df

    def cycletime_insert_hourly_amount_pass_fail(self, df, factory_name, table_name):
        df = self.pass_fail_data_combine_stationtype(df)
        df = self.pass_fail_data_combine_station_and_port(df)
        df = self.df_drop_column(df, drop_column_name_list=["STATION_NAME", "PORT"])
        df = df[["TEST_TIME", "MODEL", "STATION_TYPE", "STATION_PORT", "TEST_RESULT"]]
        df["TEST_TIME"] = pd.to_datetime(df["TEST_TIME"]) - pd.Timedelta(hours=8)
        tags_columns = [pd.Grouper(key='TEST_TIME', freq="h"), "MODEL", "STATION_TYPE", "STATION_PORT"]
        df_pass = df[df["TEST_RESULT"]==1].reset_index(drop=True)
        df_pass = df_pass.groupby(tags_columns)["TEST_RESULT"].size()
        df_pass = df_pass.reset_index()
        df_pass = self.df_rename_column(df_pass, {"TEST_RESULT": "PASS"})
        df_fail = df[df["TEST_RESULT"]==0].reset_index(drop=True)
        df_fail = df_fail.groupby(tags_columns)["TEST_RESULT"].size()
        df_fail = df_fail.reset_index()
        df_fail = self.df_rename_column(df_fail, {"TEST_RESULT": "FAIL"})
        df = pd.merge(df_pass, df_fail, how="outer", on=["TEST_TIME", "MODEL", "STATION_TYPE", "STATION_PORT"]).reset_index(drop=True)
        df = df.where(pd.notnull(df), 0)
        json = []
        for index in range(len(df)):
            json_point = {
                "measurement": table_name,
                "time": df["TEST_TIME"][index],
                "tags":{"MODEL": df["MODEL"][index],
                        "STATION_TYPE": df["STATION_TYPE"][index],
                        "STATION_PORT": df["STATION_PORT"][index]},
                "fields":{"PASS":df["PASS"][index],
                          "FAIL":df["FAIL"][index]}
            }
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, None, 10000)

    def cycletime_periodic_wave_plot(self, df):
        start_col = ["MODEL", "START_TIME", "STATION_TYPE", "STATION_PORT", "MSN", "CSN"]
        end_col = ["MODEL", "END_TIME", "STATION_TYPE", "STATION_PORT", "MSN", "CSN"]
        start_onesecond_col = ["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT", "MSN", "CSN"]
        end_onesecond_col = ["MODEL", "ONESECOND", "STATION_TYPE", "STATION_PORT", "MSN", "CSN"]
        df_res = self.cycletime_create_periodic_wave(df, start_col, end_col, start_onesecond_col, end_onesecond_col)
        return df_res

    def cycletime_insert_raw_data(self, factory_name, df_cycletime, measurement_name, pass_or_fail="PASS"):
        df_cycletime["SKU_NAME"] = df_cycletime["MSN"].str.strip().str[-3:]
        df_cycletime = self.df_change_column_type(df_cycletime, col_new_type_dict={"SKU_NAME":"str"})
        sku_name_dict = self.sku_name()
        df_cycletime = df_cycletime.replace({"SKU_NAME":sku_name_dict})
        pass_or_fail = pass_or_fail.upper()
        pass_or_fail_dict = {"PASS": 1, "FAIL": 0}
        json = []
        for index in range(len(df_cycletime)):
            json_point = {
                    "measurement": measurement_name,
                    "time": df_cycletime["START_TIME"][index],
                    "tags": {"MODEL":df_cycletime["MODEL"][index],
                             "STATION_TYPE": df_cycletime["STATION_TYPE"][index],
                             "STATION_PORT": df_cycletime["STATION_PORT"][index],
                             "PASS_FAIL": pass_or_fail_dict[pass_or_fail]},
                    "fields": {"CYCLE_TIME": df_cycletime["CYCLETIME"][index],
                               "MSN": df_cycletime["MSN"][index],
                               "CSN": df_cycletime["CSN"][index],
                               "SKU": df_cycletime["SKU_NAME"][index]}
            }
            json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, None, 10000)

    def cycletime_insert_wave_plot_data(self, factory_name, df, measurement_name, pass_or_fail="PASS"):
        pass_or_fail = pass_or_fail.upper()
        pass_or_fail_dict = {"PASS": 1, "FAIL": 0}
        json = []
        for index in range(len(df)):
            json_point = {
                    "measurement": measurement_name,
                    "time": df["TEST_TIME"][index],
                    "tags": {"MODEL":df["MODEL"][index],
                             "STATION_TYPE": df["STATION_TYPE"][index],
                             "STATION_PORT": df["STATION_PORT"][index],
                             "PASS_FAIL": pass_or_fail_dict[pass_or_fail]},
                    "fields": {"CHECK":df["CHECK"][index]}
            }
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, None, 10000)

    def df_classify_by_work_and_break_time(self, df_origin, period_time_tuple=("00:00", "24:00")):
        df = self.df_get_timerange(df_origin, period_time_tuple)
        df = self.df_time_to_utc_format(df, "TEST_TIME", 8)
        df["SECONDS"] = pd.to_timedelta(df["CYCLETIME"], "s")
        df["START_TIME"] = df["TEST_TIME"] + df["SECONDS"]
        df["END_TIME"] = df["TEST_TIME"].shift(-1)
        df = df.iloc[1:-2, :].reset_index(drop=True)
        df["IDLE_TIME"] = df["END_TIME"] - df["START_TIME"]
        df = self.df_drop_column(df, drop_column_name_list=["CSN", "CYCLETIME", "START_TIME", "TEST_TIME", "SECONDS"])
        df = self.df_rename_column(df, {"END_TIME": "TEST_TIME"})
        df["IDLE_TIME"] = df["IDLE_TIME"].dt.total_seconds()
        df = self.df_change_column_type(df, col_new_type_dict={"IDLE_TIME":"int"})
        if len(df) < 2:
            df = pd.DataFrame(columns=df.columns)
        return df

    def cycletime_create_periodic_wave(self, df, start_col, end_col, start_onesecond_col, end_onesecond_col):
        df_start = df.copy(deep=True)
        df_end = df.copy(deep=True)
        df_start = df_start[start_col]
        df_start = self.df_rename_column(df_start, {"START_TIME":"TEST_TIME"})
        df_end = df_end[end_col]
        df_end = self.df_rename_column(df_end, {"END_TIME":"TEST_TIME"})
        self.display_current_time("Step: Add onesecond before start and onesecond after end dataframe")
        timediff = pd.Timedelta(1, unit='s')
        df_start_one = df_start.copy(deep=True)
        df_end_one = df_end.copy(deep=True)
        df_start_one["ONESECOND"] = df_start_one["TEST_TIME"] - timediff
        df_end_one["ONESECOND"] = df_end_one["TEST_TIME"] + timediff
        df_start_one = df_start_one[start_onesecond_col]
        df_end_one = df_end_one[end_onesecond_col]
        df_start_one = self.df_rename_column(df=df_start_one, rename_dict={"ONESECOND":"TEST_TIME"})
        df_end_one = self.df_rename_column(df=df_end_one, rename_dict={"ONESECOND":"TEST_TIME"})
        self.display_current_time("Step: Give a threshold to display on grafana dashboard")
        df_start["CHECK"] = 1
        df_end["CHECK"] = 1
        df_start_one["CHECK"] = 0
        df_end_one["CHECK"] = 0
        df_res = pd.concat([df_start, df_end, df_start_one, df_end_one], axis=0)
        df_res = self.df_column_sort_value(df=df_res, col_name="TEST_TIME")
        return df_res

    def df_get_timerange(self, df, hour_range=("00:00", "24:00")):
        df_res = df.between_time(hour_range[0], hour_range[1]).reset_index()
        return df_res

    def df_time_to_utc_format(self, df, time_col, utc_time_delta):
        df[time_col] = pd.to_datetime(df[time_col]) - pd.Timedelta(hours=utc_time_delta)
        return df

    def get_cycletime_from_fail_log(self, df):
        cycle_time_text = self.df_column_search_particular_string(df, "RAW_TEXT", "Testing duration")
        cycle_time = cycle_time_text.split(":")[1].lstrip().rstrip()
        allowed_string_format = ["%M Min %S Sec", "%S Sec"]
        for string_format in allowed_string_format:
            try:
                cycle_time = datetime.strptime(cycle_time, string_format)  - datetime(1900,1,1)
                cycle_time = cycle_time.total_seconds()
                return cycle_time
            except:
                pass
        return 0

    def get_fail_log_info(self, df):
        msn_text = self.df_column_search_particular_string(df=df, column_name="RAW_TEXT", target_text="ManufacturingSN")
        msn = msn_text.split(":")[1].lstrip().rstrip()
        station_name_text = self.df_column_search_particular_string(df=df, column_name="RAW_TEXT", target_text="Station Name")
        station_name = station_name_text.split(":")[1].lstrip().rstrip()
        sn_text = self.df_column_search_particular_string(df, "RAW_TEXT", "S/N")
        sn = sn_text.split(" ")[0].split(":")[1].lstrip().rstrip()
        fw = sn_text.split(" ")[1].split(":")[1].lstrip().rstrip()
        mac = sn_text.split(" ")[2].split(":")[1].lstrip().rstrip()
        fail_log = df[df["RAW_TEXT"].str.contains("FAIL", na=False)].reset_index(drop=True).iloc[0]["RAW_TEXT"]
        return msn, station_name, sn, fw, mac, fail_log

    def get_model_from_fail_log(self, df):
        model_name_text = self.df_column_search_particular_string(df=df, column_name="RAW_TEXT", target_text="ENV")
        model_name = model_name_text.split("-")[1].lstrip().rstrip()
        return model_name

    def get_station_type_from_fail_log(self, station_name):
        if "VOICE" in station_name or "Voice" in station_name:
            station_type = "Voice"
        elif "FUNC" in station_name:
            station_type = "Func"
        elif "PREWLAN" in station_name:
            station_type = "PREWLAN"
        elif "WLAN" in station_name:
            station_type = "WLAN"
        else:
            station_type = "Others"
        return station_type

    def create_periodic_wave_of_cycletime(self, df, start_col, end_col, start_onesecond_col, end_onesecond_col):
        df_start = df.copy(deep=True)
        df_end = df.copy(deep=True)
        df_start = df_start[start_col]
        df_start = self.df_rename_column(df=df_start, rename_dict={"START_TIME":"TEST_TIME"})
        df_end = df_end[end_col]
        self.display_current_time("Step: Add onesecond before start and onesecond after end dataframe")
        timediff = pd.Timedelta(1, unit='s')
        df_start_one = df_start.copy(deep=True)
        df_end_one = df_end.copy(deep=True)
        df_start_one["ONESECOND"] = df_start_one["TEST_TIME"] - timediff
        df_end_one["ONESECOND"] = df_end_one["TEST_TIME"] + timediff
        df_start_one = df_start_one[start_onesecond_col]
        df_end_one = df_end_one[end_onesecond_col]
        df_start_one = self.df_rename_column(df=df_start_one, rename_dict={"ONESECOND":"TEST_TIME"})
        df_end_one = self.df_rename_column(df=df_end_one, rename_dict={"ONESECOND":"TEST_TIME"})
        self.display_current_time("Step: Give a threshold to display on grafana dashboard")
        df_start["CHECK"] = 1
        df_end["CHECK"] = 1
        df_start_one["CHECK"] = 0
        df_end_one["CHECK"] = 0
        df_res = pd.concat([df_start, df_end, df_start_one, df_end_one], axis=0)
        df_res = self.df_column_sort_value(df=df_res, col_name="TEST_TIME")
        return df_res

    def sku_name(self):
        sku_name_dict = {"300":"CBN-EU", "301":"CBN-NA", "590":"LGI", "591":"LGI-UPC", "592":"LGI-Telenet", "593":"LGI-VM",
                         "594":"LGI-Ziggo", "660":"Telenet", "038":"Vodafon", "094":"Claro", "096":"Adtran", "101":"Gentek",
                         "150":"CNS", "550":"KBRO", "610":"Mobistar", "670":"Stofa", "800":"CASA", "821":"PYUR", "880":"ONO"}
        return sku_name_dict

class FpyTemplate(FactoryBasicTemplate):
    def __init__(self):
        super(FpyTemplate, self).__init__()

    def fpy_df_pass_fail_count(self, df, pass_fail="PASS"):
        pass_fail_dict = {"PASS": 1, "FAIL": 0}
        df = df[df["TEST_RESULT"]==pass_fail_dict[pass_fail]].reset_index(drop=True)
        df_groupby = df.groupby(["MODEL", "STATION_TYPE", "STATION_NAME"]).agg({"TEST_RESULT":["size"]})
        df_groupby.columns = ["_".join(x) for x in df_groupby.columns.ravel()]
        df_groupby = self.df_rename_column(df_groupby, {"TEST_RESULT_size":pass_fail})
        df_groupby = df_groupby.reset_index()
        return df_groupby

    def df_calc_station_type_yield(self, df):
        df = df.groupby(['STATION_TYPE']).sum()
        df["TOTAL"] = df["PASS"] + df["FAIL"]
        df["YIELD"] = df["PASS"] / (df["PASS"]+df["FAIL"]+0.0001)
        df = df[df["PASS"]!=0]   # Avoid station which FPY=0 display on dashboard
        return df

    def cal_fpy_groupby_stationtype(self, df, factory_name, period_type):
        df = self.df_calc_station_type_yield(df)
        threshold = 0.1*(df["TOTAL"].mean())
        df_cal = df[df["TOTAL"].gt(threshold)]
        df_cal = df_cal[df_cal["TOTAL"].gt(30)]
        if period_type=="Hour":
            measurement = self.table_name_hour_firstpassyield
            time_in_db = self.time_in_db.strftime('%Y-%m-%dT%H:12:00Z')
        elif period_type=="Day":
            measurement = self.table_name_day_firstpassyield
            time_in_db = self.time_in_db.strftime('%Y-%m-%dT00:00:00Z')
        if not df_cal.empty:
            json = [{"measurement": measurement,
                     "time": time_in_db,
                     "fields": {"PASS": df["PASS"].sum(),
                                "FAIL": df["FAIL"].sum(),
                                "FPY": df["YIELD"].mean()}
            }]
            self.print_json_log(json)
            self.insert_into_influxdb(factory_name, json, 'm', 10000)

    def check_this_month_file(self):
        self.month = (datetime.now() - timedelta(days=self.shift_day)).strftime('%m')
        check_thismonth_list = [self.today]
        for day in range(1,31):
            if (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%m') != self.month:
                continue
            this_month_day = (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%Y%m%d')
            check_thismonth_list.append(this_month_day)
        return check_thismonth_list

    def check_this_week_file(self):
        self.week_num = (datetime.now() - timedelta(days=self.shift_day)).isocalendar()[1]
        check_thisweek_list = [self.today]
        for day in range(1,7):
            if (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).isocalendar()[1] != self.week_num:
                continue
            this_week_day = (datetime.now() - timedelta(days=self.shift_day) - timedelta(days=day)).strftime('%Y%m%d')
            check_thisweek_list.append(this_week_day)
        return check_thisweek_list

    def df_pass_fail_empty_df(self):
        df = pd.DataFrame(columns=["MODEL", "MSN", "CSN", "TEST_TIME", "STATION_TYPE", "STATION_NAME", "TEST_RESULT", "CYCLETIME", "PORT"])
        return df

    def df_fpy_week(self):
        check_thisweek_list = self.check_this_week_file()
        df = self.df_pass_fail_empty_df()
        df = self.df_merge_all_week_month_file(df, check_thisweek_list)
        return df

    def df_fpy_month(self):
        check_thismonth_list = self.check_this_month_file()
        df = self.df_pass_fail_empty_df()
        df = self.df_merge_all_week_month_file(df, check_thismonth_list)
        return df

    def df_merge_all_week_month_file(self, df, folder_date_list):
        for day in folder_date_list:
            folder_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(day)
            if not os.path.isdir(folder_path):
                continue
            save_file_name_list = [i for i in os.listdir(folder_path) if self.factory_name in i and "FAIL_result" in i and "!" not in i]
            if save_file_name_list==[]:
                continue
            save_file_name_list = sorted(save_file_name_list, reverse=True)
            df_sub = pd.DataFrame(columns=["MODEL", "MSN", "CSN", "TEST_TIME", "STATION_TYPE", "STATION_NAME", "TEST_RESULT", "CYCLETIME", "PORT"])
            for save_file in save_file_name_list:
                df_daily_sub = pd.read_csv(folder_path+save_file)
                df_sub = pd.concat([df_sub, df_daily_sub], axis=0, ignore_index=True)
            df_sub = df_sub.reset_index(drop=True)
            df = pd.concat([df, df_sub], axis=0, ignore_index=True).reset_index(drop=True)
        df = self.df_drop_column(df, ["MSN", "CSN"])
        return df

    def get_week_or_month_interval_num(self, interval="WEEK"):
        if interval=='WEEK':
            df = self.df_fpy_week()
            interval_num = self.week_num
        elif interval=='MONTH':
            df = self.df_fpy_month()
            interval_num = self.month
        pass_count = len(df[df["TEST_RESULT"]==1])
        fail_count = len(df[df["TEST_RESULT"]==0])
        result_yield = pass_count / (pass_count+fail_count+0.0001)
        if pass_count==0 and fail_count==0:
            pass_count = None
            fail_count = None
            result_yield = None
        return pass_count, fail_count, result_yield, interval_num
