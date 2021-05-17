from factory_template import FpyTemplate
from datetime import datetime, timedelta

class FpyWeekMonth(FpyTemplate):
    def __init__(self):
        super(FpyWeekMonth, self).__init__()
        self.table_name = "FPY_WEEK_MONTH"

    def main(self, factory_name, shift_day):
        self.display_current_time("========== Step1: Start setting time & other information")
        print('host = {}'.format(self.host))
        self.factory_name = factory_name
        self.shift_day = shift_day
        self.today = (datetime.now() - timedelta(days=self.shift_day)).strftime('%Y%m%d')

        self.display_current_time("========== Step2: Start adding weekly fpy")
        self.insert_weekly_or_monthly_fpy_to_influxdb(self.factory_name, interval='WEEK')
        
        self.display_current_time("========== Step3: Start adding monthly fpy")
        self.insert_weekly_or_monthly_fpy_to_influxdb(self.factory_name, interval='MONTH')

if __name__=="__main__":
    fpy_week_month = FpyWeekMonth()
    shift_day = 1
    for factory_name in ['CDE', 'FLEX', 'TwoWing']:
        try:
            fpy_week_month.main(factory_name=factory_name, shift_day=shift_day)
        except Exception as e:
            print(e)



