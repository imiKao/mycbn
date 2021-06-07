from PIL import Image
from factory_report_template import DailyReportTemplate
from datetime import datetime, timedelta
import os

class DailyReportSummary(DailyReportTemplate):
    def __init__(self):
        super(DailyReportSummary, self).__init__()

    def main(self, factory_name, grafana_url_id, which_day=1, cut_width=1366, cut_height=420):
        self.which_day = 3 if (datetime.now()-timedelta(days=which_day)).weekday()==6 else which_day
        url = self.grafana_url + "/" + grafana_url_id + "?orgId=1&kiosk" + "&from=now-{}d%2Fd&to=now-{}d%2Fd".format(self.which_day, self.which_day)
        test_item_list = self.get_min_cpk_from_influxdb(factory_name)
        if test_item_list != []:
            model = test_item_list[0]["MODEL"]
            station_type = test_item_list[0]["STATION_TYPE"]
            test_item = test_item_list[0]["TEST_ITEM"]
            url = url + "&var-MODEL=" + model + "&var-STATION_TYPE=" + station_type + "&var-TEST_ITEM=" + test_item
        else:
            url = url + "&var-TEST_ITEM=NA"
        save_file_name = "daily_report_summary_" + factory_name
        if os.path.isfile(save_file_name+".png"):
            os.remove(save_file_name+".png")

        self.grafana_dashboard_screenshot(url, self.grafana_id, self.grafana_passwd, save_file_name)
        image = Image.open("%s.png"%(save_file_name))
        image = self.image_cut_by_resolution(image, cut_width, cut_height)
        resize_ratio = 0.5
        image = self.image_resize_by_ratio(image, resize_ratio, cut_width, cut_height)
        image.save("%s.png"%(save_file_name))

    def get_min_cpk_from_influxdb(self, factory_name):
        client = self.influxdbClient(self.host, factory_name)
        query_text = "select min(CPK), MODEL, STATION_TYPE, TEST_ITEM from CPK_STATIONTYPE_DAILY where COUNT >= 100 and time >= now() - {}d and time < now() - {}d".format(self.which_day+1, self.which_day)
        query_result = client.query(query_text)
        test_item_list = self.get_daily_low_cpk_item(query_result)
        return test_item_list

    def image_cut_by_resolution(self, image, cut_width, cut_height):
        image_crop = image.crop((0, 0, cut_width, cut_height))
        return image_crop

    def image_resize_by_ratio(self, image, resize_ratio, cut_width, cut_height):
        resize_width = int(cut_width*resize_ratio)
        resize_height = int(cut_height*resize_ratio)
        image_resize = image.resize((resize_width, resize_height), Image.ANTIALIAS)
        return image_resize

if __name__=="__main__":
    daily_report_summary = DailyReportSummary()
    factory_name_list = ["CDE", "FLEX", "TwoWing"]
    grafana_url_id = {"CDE":"aVc9lH9Gz", "FLEX":"0Qo_uNrMk", "TwoWing":"b_qxuN9Gz"}
    which_day = 1
    for factory_name in factory_name_list:
        try:
            daily_report_summary.main(factory_name, grafana_url_id[factory_name], which_day, 1366, 420)
        except Exception as e:
            print(e)
