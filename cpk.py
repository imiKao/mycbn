import numpy as np
import pandas as pd
from influxdb import InfluxDBClient
from factory_template import CpkTemplate

class CpkDailyStationType(CpkTemplate):
    def __init__(self):
        super(CpkDailyStationType, self).__init__()
        self.table_name = "CPK_STATIONTYPE_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Get dataframe from csv files")
        df_all = self.get_df_from_cpk_daily_file(factory_name, shift_period)
        if type(df_all) == int:
            return 0

        self.display_current_time("========== Step2: Start preprocessing dataframe")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"
        self.display_current_time("========== Step2-1: Create preprocess columns")
        tags_columns = [pd.Grouper(key='TEST_TIME', freq='D'), "MODEL", "STATION_TYPE", "TEST_ITEM"]
        df = df_all.groupby(tags_columns).agg({"ITEM_VALUE": ["mean", self.std_rbar, "size"], "USL": ["mean"], "LSL": ["mean"]})
        df = df[df["ITEM_VALUE"]["size"].gt(30)]   # Do not calculate Cpk when count less than 30
        df.loc[:, ("CPK","usl_avg")] = (df.loc[:,("USL","mean")] - df.loc[:,("ITEM_VALUE","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","avg_lsl")] = (df.loc[:,("ITEM_VALUE","mean")] - df.loc[:,("LSL","mean")]) / (3 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df.loc[:, ("CPK","usl_lsl")] = (df.loc[:,("USL","mean")] - df.loc[:,("LSL","mean")]) / (6 * df.loc[:, ("ITEM_VALUE","std_rbar")])
        df = self.df_drop_multi_layer_index_by_particular_string(df, ["CYCLETIME", "UPTIME"], index_layer=len(tags_columns))
        self.display_current_time("========== Step2-2: Create Cpk columns")
        df = self.cpk_column(df=df, cpk_col=("CPK","cpk"), usl_avg_col=("CPK","usl_avg"), avg_lsl_col=("CPK","avg_lsl"))
        self.display_current_time("========== Step2-3: Create Cp(precision) columns")
        df = self.cp_column(df=df, cp_col=("CPK","cp"), usl_lsl_col=("CPK","usl_lsl"), usl_avg_col=("CPK","usl_avg"), avg_lsl_col=("CPK","avg_lsl"))
        self.display_current_time("========== Step2-4: Create Ck(1-Accuracy) columns")
        df = self.ck_column(df=df, ck_col=("CPK","ck"), usl_col=("USL","mean"), lsl_col=("LSL","mean"), avg_col=("ITEM_VALUE","mean"))
        self.display_current_time("========== Step2-5: Create Cp & Ck weighted level")
        df = self.level_column_by_cp_ck(df=df, level_col=("CPK","level"), cp_score_col=("CPK","cp_score"), ck_score_col=("CPK","ck_score"), cp_col=("CPK","cp"), ck_col=("CPK","ck"))
        self.display_current_time("========== Step2-6: Final preprocessing")
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl"), ("CPK","cp_score"), ("CPK","ck_score")])
        df =df.reset_index()
        df = self.df_drop_row_by_particular_string(df, "TEST_ITEM", ["_2"])

        self.display_current_time("========== Step3: Start insert data into influxdb")
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index], "CK":df[("CPK","ck")][index], "LEVEL":df[("CPK","level")][index]}
            fields_no_lsl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "CP":df[("CPK","cp")][index], "LEVEL":df[("CPK","level")][index]}
            fields_no_usl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index], "LEVEL":df[("CPK","level")][index]}
            if np.isnan(df[("LSL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_lsl)
            elif np.isnan(df[("USL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_usl)
            else:
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

class CpkDailyStation(CpkTemplate):
    def __init__(self):
        super(CpkDailyStation, self).__init__()
        self.table_name = "CPK_STATION_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Get dataframe from csv files")
        df_all = self.get_df_from_cpk_daily_file(factory_name, shift_period)
        if type(df_all) == int:
            return 0
        self.display_current_time("========== Step2: Start preprocessing dataframe")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"
        """Create preprocess columns"""
        self.display_current_time("========== Step2-1: Create preprocess columns")
        groupby_columns = [pd.Grouper(key='TEST_TIME', freq='D'), "MODEL", "STATION_TYPE", "STATION_NAME", "TEST_ITEM"]
        df = self.get_df_cpk(df=df_all, groupby_columns=groupby_columns)
        self.display_current_time("========== Step2-2: Create Cp(precision) columns")
        df = self.cp_column(df=df, cp_col=("CPK","cp"), usl_lsl_col=("CPK","usl_lsl"), usl_avg_col=("CPK","usl_avg"), avg_lsl_col=("CPK","avg_lsl"))
        self.display_current_time("========== Step2-4: Create Ck(1-Accuracy) columns")
        df = self.ck_column(df=df, ck_col=("CPK","ck"), usl_col=("USL","mean"), lsl_col=("LSL","mean"), avg_col=("ITEM_VALUE","mean"))
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl")])
        df = df.reset_index()
        df = self.df_drop_row_by_particular_string(df, "TEST_ITEM", ["_2"])
        self.display_current_time("========== Step3: Start insert data into influxdb")
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "STATION_NAME": df["STATION_NAME"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index], "CK":df[("CPK","ck")][index]}
            fields_no_lsl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "CP":df[("CPK","cp")][index]}
            fields_no_usl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index]}
            if np.isnan(df[("LSL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_lsl)
            elif np.isnan(df[("USL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_usl)
            else:
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

class CpkDailyStationPort(CpkTemplate):
    def __init__(self):
        super(CpkDailyStationPort, self).__init__()
        self.table_name = "CPK_PORT_DAILY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Get dataframe from csv files")
        df_all = self.get_df_from_cpk_daily_file(factory_name, shift_period)
        if type(df_all) == int:
            return 0

        self.display_current_time("========== Step2: Start preprocessing dataframe")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"
        """Create preprocess columns"""
        groupby_columns = [pd.Grouper(key='TEST_TIME', freq='D'), "MODEL", "STATION_TYPE", "STATION_NAME", "PORT", "TEST_ITEM"]
        df = self.get_df_cpk(df=df_all, groupby_columns=groupby_columns)
        df = self.cp_column(df=df, cp_col=("CPK","cp"), usl_lsl_col=("CPK","usl_lsl"), usl_avg_col=("CPK","usl_avg"), avg_lsl_col=("CPK","avg_lsl"))
        df = self.ck_column(df=df, ck_col=("CPK","ck"), usl_col=("USL","mean"), lsl_col=("LSL","mean"), avg_col=("ITEM_VALUE","mean"))
        df = df.reset_index()
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl")])
        df = self.df_drop_row_by_particular_string(df, "TEST_ITEM", ["_2"])
        df["PORT"] = "#" + df["PORT"].astype(int).astype(str)

        self.display_current_time("========== Step3: Start insert data into influxdb")
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "STATION_NAME": df["STATION_NAME"][index],
                    "PORT": df["PORT"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index], "CK":df[("CPK","ck")][index]}
            fields_no_lsl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "CP":df[("CPK","cp")][index]}
            fields_no_usl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "LSL": df[("LSL","mean")][index], "CP":df[("CPK","cp")][index]}
            if np.isnan(df[("LSL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_lsl)
            elif np.isnan(df[("USL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_usl)
            else:
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

class CpkHourlyStationType(CpkTemplate):
    def __init__(self):
        super(CpkHourlyStationType, self).__init__()
        self.table_name = "CPK_STATIONTYPE_HOURLY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Get dataframe from csv files")
        df_all = self.get_df_from_cpk_hourly_file(factory_name, shift_period)
        if type(df_all) == int:
            return 0

        self.display_current_time("========== Step2: Start preprocessing dataframe =====")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"
        """Create preprocess columns"""
        groupby_columns = [pd.Grouper(key='TEST_TIME', freq='H'), "MODEL", "STATION_TYPE", "TEST_ITEM"]
        df = self.get_df_cpk(df=df_all, groupby_columns=groupby_columns)
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl")])
        df = df.reset_index()
        df = self.df_drop_row_by_particular_string(df, "TEST_ITEM", ["_2"])

        self.display_current_time("========== Step3-1: Start insert data into influxdb(CPK)")
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "LSL": df[("LSL","mean")][index]}
            fields_no_lsl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index]}
            fields_no_usl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "LSL": df[("LSL","mean")][index]}
            if np.isnan(df[("LSL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_lsl)
            elif np.isnan(df[("USL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_usl)
            else:
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)
        self.display_current_time("========== Step3-2: Start insert data into influxdb(data amount < 30)")
        df = df_all.groupby(groupby_columns).agg({"ITEM_VALUE": ["size"], "USL": ["mean"], "LSL": ["mean"]})
        df = df[df.loc[:,("ITEM_VALUE","size")].lt(30)]
        df = df.reset_index()
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"COUNT": df[("ITEM_VALUE","size")][index]}
            json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

class CpkHourlyStation(CpkTemplate):
    def __init__(self):
        super(CpkHourlyStation, self).__init__()
        self.table_name = "CPK_STATION_HOURLY"

    def main(self, factory_name, shift_period=0):
        self.display_current_time("========== Step1: Get dataframe from csv files")
        df_all = self.get_df_from_cpk_hourly_file(factory_name, shift_period)
        if type(df_all) == int:
            return 0

        self.display_current_time("========== Step2: Start preprocessing dataframe =====")
        df_all = df_all.reset_index(drop=True)
        df_all = self.df_change_column_to_numeric(df=df_all, column_name_list=['USL', 'LSL'])
        df_all['TEST_TIME'] = pd.to_datetime(df_all['TEST_TIME']) - pd.Timedelta(hours=8)
        df_all.loc[df_all["STATION_NAME"].str.contains("M200-VOICE7", na=False), "STATION_TYPE"] = "M200-Voice"
        """Create preprocess columns"""
        groupby_columns = [pd.Grouper(key='TEST_TIME', freq='H'), "MODEL", "STATION_TYPE", "STATION_NAME", "TEST_ITEM"]
        df = self.get_df_cpk(df=df_all, groupby_columns=groupby_columns)
        df = self.df_drop_column(df, [("CPK","usl_avg"), ("CPK","avg_lsl")])
        df = df.reset_index()
        df = self.df_drop_row_by_particular_string(df, "TEST_ITEM", ["SNR", "_2"])

        self.display_current_time("========== Step3: Start insert data into influxdb")
        json = []
        for index in range(len(df)):
            tags = {"MODEL": df["MODEL"][index],
                    "STATION_TYPE": df["STATION_TYPE"][index],
                    "STATION_NAME": df["STATION_NAME"][index],
                    "TEST_ITEM": df["TEST_ITEM"][index]}
            fields = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index], "LSL": df[("LSL","mean")][index]}
            fields_no_lsl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "USL": df[("USL","mean")][index]}
            fields_no_usl = {"CPK": df[("CPK","cpk")][index], "STD": df[("ITEM_VALUE","std_rbar")][index], "COUNT": df[("ITEM_VALUE","size")][index], "AVG": df[("ITEM_VALUE","mean")][index], "LSL": df[("LSL","mean")][index]}
            if np.isnan(df[("LSL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_lsl)
            elif np.isnan(df[("USL","mean")][index]):
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields_no_usl)
            else:
                json_point = self.influxdb_json_point(self.table_name, df["TEST_TIME"][index], tags, fields)
            json.append(json_point)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

class CpkDailyStationTypeMA(CpkTemplate):
    def __init__(self):
        super(CpkDailyStationTypeMA, self).__init__()
        self.table_name = "CPK_STATIONTYPE_DAILY_MA"

    def main(self, factory_name, which_day=1):
        client = InfluxDBClient(host=self.host, port=8086, database=factory_name)
        query_text = "select MODEL, STATION_TYPE, TEST_ITEM, CPK from CPK_STATIONTYPE_DAILY where MODEL != 'unknow' and time >= now() - %sd and time < now() - %sd"%(which_day+10, which_day)
        query_result = client.query(query_text)
        df = pd.DataFrame(columns=["time", "MODEL", "STATION_TYPE", "TEST_ITEM", "CPK"])
        for li in query_result:
            for di in li:
                df_temp = pd.DataFrame([di])
                df = pd.concat([df, df_temp], axis=0, ignore_index=True)
        model_list = df["MODEL"].unique()
        json = []
        for model in model_list:
            df_model = df[df["MODEL"]==model]
            station_type_list = df_model["STATION_TYPE"].unique()
            for station_type in station_type_list:
                df_station_type = df_model[df_model["STATION_TYPE"]==station_type].reset_index(drop=True)
                test_item_list = df_station_type["TEST_ITEM"].unique()
                for test_item in test_item_list:
                    df_test_item = df_station_type[df_station_type["TEST_ITEM"]==test_item].reset_index(drop=True)
                    if len(df_test_item)<=3:
                        continue
                    df_test_item["CPK_MA"] = df_test_item["CPK"].rolling(window=3, center=True).mean()
                    df_test_item = df_test_item.dropna(axis=0, how='any').reset_index(drop=True)
                    for index in range(len(df_test_item)):
                        json_point = {
                                "measurement": self.table_name,
                                "time": df_test_item["time"][index],
                                "tags": {"MODEL":df_test_item["MODEL"][index],
                                         "STATION_TYPE": df_test_item["STATION_TYPE"][index],
                                         "TEST_ITEM": df_test_item["TEST_ITEM"][index]},
                                "fields": {"CPK": df_test_item["CPK"][index],
                                           "CPK_MA": df_test_item["CPK_MA"][index]}
                        }
                        json.append(json_point)
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, "m", 10000)

if __name__=="__main__":
    def runCpk(run_class, factory_list, time_period):
        run_cpk = run_class
        for factory_name in factory_list:
            for period in reversed(range(time_period)):
                try:
                    run_cpk.main(factory_name, period)
                except Exception as e:
                    print(e)

    factory_list = ["CDE", "FLEX", "TwoWing"]
    """========== HOURLY CPK =========="""
    hour_range = 8
    runCpk(CpkHourlyStationType(), factory_list, hour_range)
    runCpk(CpkHourlyStation(), factory_list, hour_range)

    """========== DAILY CPK =========="""
    day_range = 2
    runCpk(CpkDailyStationType(), factory_list, day_range)
    runCpk(CpkDailyStation(), factory_list, day_range)
    runCpk(CpkDailyStationPort(), factory_list, day_range)
