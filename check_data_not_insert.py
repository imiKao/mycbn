import os
from factory_template import FactoryBasicTemplate
from datetime import datetime, timedelta

class CheckDataIfInsertOrNot(FactoryBasicTemplate):
    def __init__(self):
        super(CheckDataIfInsertOrNot, self).__init__()
        self.shift_hour = 0

    def check_data_insert(self, factory_name):
        time_in_json = datetime.now() - timedelta(hours=self.shift_hour+self.factory_jet_lag[factory_name]) - timedelta(hours=8)
        folder_path = os.path.dirname(os.getcwd()) + '/Raw_data/' + (datetime.now() - timedelta(hours=self.shift_hour+self.factory_jet_lag[factory_name])).strftime('%Y%m%d')
        this_hour = (datetime.now() - timedelta(hours=self.shift_hour+self.factory_jet_lag[factory_name])).strftime('%Y%m%d-%H')
        file_name_list = [i for i in os.listdir(folder_path) if factory_name in i and "Mac" in i and this_hour in i ]
        if file_name_list!=[]:
            result = 1
        else:
            result = 0
        json = [{
            "measurement": "CHECK_DATA_INSERT_OR_NOT",
            "time":time_in_json.strftime("%Y-%m-%dT%H:12:00Z"),
            "fields":{"check":result}
        }]
        self.print_json_log(json)
        self.insert_into_influxdb(database=factory_name, json_body=json)

if __name__ == '__main__':
    check_insert_data = CheckDataIfInsertOrNot()
    factory_name_list = ["CDE", "FLEX", "TwoWing"]
    for factory in factory_name_list:
        try:
            check_insert_data.check_data_insert(factory)
        except Exception as e:
            print(e)
