from factory_template import FactoryBasicTemplate
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

class FactoryDate(FactoryBasicTemplate):
    def __init__(self):
        super(FactoryDate, self).__init__()

    def main(self, factory_name, day_period):
        client = InfluxDBClient(host=self.host, port=8086, username="cbn", password="cbn@cbn", database=factory_name)
        today = datetime.now() - timedelta(days=day_period)
        this_week = today.isocalendar()[1]
        json_point = [{
                "measurement": "DATE",
                "time": today.strftime('%Y-%m-%dT00:00:00Z'), 
                "fields":{"date": today.strftime('%Y-%m-%d'),
                          "weeknum": "Week"+str(this_week),
                          "month": today.strftime('%Y-%m')
                }
        }]
        print(json_point)
        client.write_points(json_point, time_precision='m')

if __name__=="__main__":
    date = FactoryDate()
    day_range = 1
    for factory_name in ["CDE", "FLEX", "TwoWing"]:
        for day in reversed(range(day_range)):
            try:
                date.main(factory_name, day)
            except Exception as e:
                print(e)