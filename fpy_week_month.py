from factory_template import FpyTemplate, CycletimeTemplate
from datetime import datetime, timedelta

class FpyWeekMonth(FpyTemplate, CycletimeTemplate):
    def __init__(self):
        super(FpyWeekMonth, self).__init__()
        self.table_name = "FPY_WEEK_MONTH_YIELD"

    def main(self, factory_name, shift_day):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('host = {}'.format(self.host))
        self.factory_name = factory_name
        self.shift_day = shift_day
        self.today = (datetime.now() - timedelta(days=self.shift_day)).strftime('%Y%m%d')

        self.display_current_time("========== Step2: Weekly FPY")
        pass_count, fail_count, week_yield, interval_num = self.get_week_or_month_interval_num("WEEK")
        json = [{
            "measurement": self.table_name,
            "time": (datetime.now() - timedelta(days=self.shift_day)).strftime('%Y-%m-%dT00:00:00Z'),
            "fields": {
                    "Week_PASS": pass_count,
                    "Week_FAIL": fail_count,
                    "Weekly_fpy": week_yield,
                    "Weeknum": interval_num
                    }
        }]
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

        self.display_current_time("========== Step3: Monthly FPY")
        pass_count, fail_count, month_yield, interval_num = self.get_week_or_month_interval_num("MONTH")
        json = [{
            "measurement": self.table_name,
            "time": (datetime.now() - timedelta(days=self.shift_day)).strftime('%Y-%m-%dT00:00:00Z'),
            "fields": {
                    "Month_PASS": pass_count,
                    "Month_FAIL": fail_count,
                    "Monthly_fpy": month_yield,
                    "Month": interval_num
                    }
        }]
        self.print_json_log(json)
        self.insert_into_influxdb(factory_name, json, 'm', 10000)

if __name__=="__main__":
    fpy_week_month = FpyWeekMonth()
    shift_day = 1
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        try:
            fpy_week_month.main(factory_name=factory_name, shift_day=shift_day)
        except Exception as e:
            print(e)
