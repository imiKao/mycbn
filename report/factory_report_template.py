from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from PIL import Image
from PyPDF2 import PdfFileMerger
from influxdb import InfluxDBClient
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import os
import time
import calendar

class FactoryReportTemplate(object):
    def __init__(self):
        self.host = "10.118.251.78"
        self.grafana_url = "http://cbn_factory.compalbn.com:3000/d"
        self.grafana_id = 'cbn'
        self.grafana_passwd = 'cbncbn'
        self.folder_path = os.getcwd() + "/"

    def display_current_time(self, display_name):
        t = time.localtime()
        a = time.strftime("%H:%M:%S", t)
        print("{}: {}".format(display_name, a))

    def influxdbClient(self, host, database):
        client = InfluxDBClient(host=host, port=8086, database=database)
        return client

    def add_group_dashboard_id(self, group_pdf_list, dashboard_id_info, group_name, group_sub_name, which_period, period_type):
        self.display_current_time("----- Add {} Start -----".format(group_sub_name))
        dashboard_id = dashboard_id_info[group_name][group_sub_name]
        url_prefix = "{}/{}?orgId=1".format(self.grafana_url, dashboard_id)
        url_period = "&from=now-{}{}%2F{}&to=now-{}{}%2F{}".format(which_period, period_type, period_type, which_period, period_type, period_type)
        url = url_prefix + url_period + "&kiosk"
        group_sub_pdf = self.sub_pdf_page(url, group_sub_name)
        group_pdf_list.append(group_sub_pdf)
        self.display_current_time("----- Add {} End -----".format(group_sub_name))
        return group_pdf_list

    def get_lower_daily_cpk_item(self, query_result):
        json_list = []
        for result in query_result:
            for res in result:
                json_list.append(res)
        json_list = sorted(json_list, key=lambda k: k["STATION_TYPE"])
        daily_item_list = []
        for item_list in json_list:
            daily_item_dict = {}
            for i in ["MODEL", "STATION_TYPE", "TEST_ITEM"]:
                daily_item_dict[i] = item_list[i]
            daily_item_list.append(daily_item_dict)
        return daily_item_list

    def sub_pdf_page(self, page_url, file_part_name):
        folder_path = self.folder_path
        save_file_name = self.save_file_name
        self.display_current_time("Screenshot start")
        self.grafana_dashboard_screenshot(page_url, self.grafana_id, self.grafana_passwd, save_file_name=save_file_name+file_part_name)
        self.display_current_time("Screenshot end")
        img_png = folder_path + save_file_name + file_part_name + '.png'
        sub_pdf = folder_path + save_file_name + file_part_name + '.pdf'
        self.display_current_time("Convert to pdf start")
        self.output_screenshot_pdf(input_image=img_png, output_pdf=sub_pdf, second_image_list=[])
        self.display_current_time("Convert to pdf end")
        return sub_pdf

    def summary_page(self, summary_page_name, summary_page_url_id, which_period_type, which_period):
        self.display_current_time("Summary page: %s"%(summary_page_name))
        summary_page_url = "{}/{}?orgId=1&from=now-{}{}%2F{}&to=now-{}{}%2F{}&kiosk".format(self.grafana_url, summary_page_url_id, which_period, which_period_type, which_period_type, which_period, which_period_type, which_period_type)
        summary_page_pdf = self.sub_pdf_page(page_url=summary_page_url, file_part_name='_%s'%(summary_page_name))
        return summary_page_pdf

    def web_login(self, drive,username,passwd):
        input = drive.find_element_by_xpath("//input[@name='username']")
        input.send_keys(username)
        input = drive.find_element_by_xpath("//input[@name='password']")
        input.send_keys(passwd)
        input.send_keys(Keys.ENTER)
        drive.implicitly_wait(300)

    def output_screenshot_pdf(self, input_image, output_pdf, second_image_list):
        img1 = Image.open(input_image).convert("RGB")
        img1.save(output_pdf, quality=80, subsampling=0, save_all=True, append_images=second_image_list)
        os.remove(input_image)

    def grafana_dashboard_screenshot(self, url, log_id, log_pw, save_file_name):
        opts = Options()
        opts.set_headless(headless=True)
        assert opts.headless
        driver = webdriver.Firefox(options=opts)
        driver.maximize_window()
        driver.get(url)
        self.web_login(driver, log_id, log_pw)
        time.sleep(4)  # wait until browser loaded success
        driver.save_screenshot('%s.png'%(save_file_name))
        driver.close()
        driver.quit()

    def second_image_list(self, *screenshot_img):
        second_image_list = []
        for img in screenshot_img:
            add_img = Image.open(img).convert("RGB")
            second_image_list.append(add_img)
        return second_image_list

    def merge_pdf(self, output_pdf, pdf_list):
        file_merger = PdfFileMerger()
        for pdf in pdf_list:
            file_merger.append(pdf)
            os.remove(pdf)
        file_merger.write(output_pdf)

class DailyReportTemplate(FactoryReportTemplate):
    def __init__(self):
        super(DailyReportTemplate, self).__init__()

    def check_string_char_digit_index(self, test_item):
        for index, char in enumerate(reversed(test_item)):
            if not char.isdigit():
                break
            digit_index = index+1
        return digit_index

    def cpk_test_item_dashboard_url(self, dashboard_id, model, station_type, test_item, which_day):
        url_prefix = "{}/{}?orgId=1&from=now-{}d%2Fd&to=now-{}d%2Fd".format(self.grafana_url, dashboard_id, which_day, which_day)
        url_var_model = "&var-MODEL=%s"%(model)
        url_var_station_type = "&var-STATION_TYPE=%s"%(station_type)
        url_var_test_item = "&var-TEST_ITEM=%s"%(test_item)
        url = url_prefix+url_var_model+url_var_station_type+url_var_test_item+"&kiosk"
        return url

    def cpk_test_item_type_dashboard_url(self, dashboard_id, model, station_type, test_item_list, which_day):
        url_prefix = "{}/{}?orgId=1&from=now-{}d%2Fd&to=now-{}d%2Fd".format(self.grafana_url, dashboard_id, which_day, which_day)
        url_var_model = "&var-MODEL=%s"%(model)
        url_var_station_type = "&var-STATION_TYPE=%s"%(station_type)
        url = url_prefix + url_var_model + url_var_station_type
        for test_item in test_item_list:
            url_var_test_item = "&var-TEST_ITEM=%s"%(test_item["TEST_ITEM"])
            url += url_var_test_item
        url += "&kiosk"
        return url

    def get_daily_low_cpk_item(self, query_result):
        json_list = []
        for result in query_result:
            for res in result:
                json_list.append(res)
        json_list = sorted(json_list, key=lambda k: k["STATION_TYPE"])
        daily_item_list = []
        for item_list in json_list:
            daily_item_dict = {}
            for i in ["MODEL", "STATION_TYPE", "TEST_ITEM"]:
                daily_item_dict[i] = item_list[i]
            daily_item_list.append(daily_item_dict)
        return daily_item_list

    def get_test_item_from_test_item_type(self, client, which_day, model, station_type, test_item_type):
        query_text = "select MODEL, STATION_TYPE, TEST_ITEM, CPK from PREPROCESS_CPK_DAILY where MODEL = '{}' and STATION_TYPE = '{}' and TEST_ITEM =~ /{}*/ and time >= now() - {}d and time < now() - {}d".format(model, station_type, test_item_type, which_day+1, which_day)
        query_result = client.query(query_text)
        test_item_list = self.get_daily_low_cpk_item(query_result)
        return test_item_list

    def remove_repeat_list_in_list(self, target_list):
        s = set(tuple(l) for l in target_list)
        target_list = [list(t) for t in s]
        return target_list

class WeeklyReportTemplate(FactoryReportTemplate):
    def __init__(self):
        super(WeeklyReportTemplate, self).__init__()

    def get_grafana_target_url(self, target_id, model, station_type, test_item, which_period_type, which_week):
        url_prefix = "{}/{}?orgId=1".format(self.grafana_url, target_id)
        url_period = "&from=now-{}{}%2F{}&to=now-{}{}%2F{}".format(which_week, which_period_type, which_period_type, which_week, which_period_type, which_period_type)
        url_var_model = "&var-MODEL={}".format(model)
        url_var_station_type = "&var-STATION_TYPE={}".format(station_type)
        url_var_test_item = "&var-TEST_ITEM={}".format(test_item)
        url = url_prefix + url_period + url_var_model + url_var_station_type + url_var_test_item + "&kiosk"
        return url

    def get_test_item_freq_dict(self, all_item_list):
        test_item_freq_dict = dict()   # {"model::stationtype::test_item": freq}
        for item in all_item_list:
            model = item["MODEL"]
            station_type = item["STATION_TYPE"]
            test_item = item["TEST_ITEM"]
            key_word = "%s::%s::%s"%(model, station_type, test_item)
            if key_word in test_item_freq_dict.keys():
                test_item_freq_dict[key_word] += 1
            else:
                test_item_freq_dict[key_word] = 1
        return test_item_freq_dict

    def get_item_by_freq_top_ranking(self, top_ranking, choose_dict):
        items = choose_dict.items()
        result_items = [[v[1], v[0]] for v in items]
        result_items = sorted(result_items, reverse=True)
        if len(result_items) > top_ranking:
            result_items = result_items[:top_ranking]
        return result_items

    def get_item_by_freq_ratio(self, choose_threshold, choose_dict):
        items = choose_dict.items()
        result_items = [[v[1], v[0]] for v in items if v[1]>choose_threshold*7.0]
        result_items = sorted(result_items, reverse=True)
        return result_items

    def get_weeknum_first_and_last_day(self, year_week):
        weeknum_day1 = datetime.strptime(year_week + '-1', "%Y-W%W-%w") - timedelta(days=1)
        weeknum_day2 = datetime.strptime(year_week + '-0', "%Y-W%W-%w") - timedelta(days=1)
        first_day = datetime.strftime(weeknum_day1, "%Y-%m-%d")
        last_day = datetime.strftime(weeknum_day2, "%Y-%m-%d")
        return first_day, last_day

class MonthlyReportTemplate(FactoryReportTemplate):
    def __init__(self):
        super(MonthlyReportTemplate, self).__init__()

    def get_month_first_and_last_day(self, which_month):
        month = (date.today() - relativedelta(months=which_month)).strftime('%-m')
        year = (date.today() - relativedelta(months=which_month)).strftime('%Y')
        last_day_num = calendar.monthrange(int(year), int(month))[1]
        first_day = (date.today() - relativedelta(months=which_month)).strftime('%Y-%m-01')
        last_day = (date.today() - relativedelta(months=which_month)).strftime('%Y-%m-{}'.format(last_day_num))
        return first_day, last_day
