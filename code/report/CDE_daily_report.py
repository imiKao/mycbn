from factory_report_template import DailyReportTemplate
from datetime import timedelta, date
import time

class CDEDailyReportCpk(DailyReportTemplate):
    def __init__(self):
        super(CDEDailyReportCpk, self).__init__()

    def main(self, factory_name, which_day, dashboard_id_info):
        time_start = time.time()
        client = self.influxdbClient(host=self.host, database=factory_name)
        self.which_day = which_day
        which_day = self.get_which_day_by_checking_production(client, self.which_day)
        query_text = "select MODEL, STATION_TYPE, TEST_ITEM, CPK from CPK_STATIONTYPE_DAILY where CPK<1 and MODEL != 'unknow' and time >= now() - %sd and time < now() - %sd"%(which_day+1, which_day)
        query_result = client.query(query_text)
        daily_low_cpk_item_list = self.get_daily_low_cpk_item(query_result)
        yesterday = (date.today() - timedelta(days=which_day)).strftime('%Y%m%d')
        self.save_file_name = '%s_CDE_daily_report'%(yesterday)

        """Add dashboard page"""
        pdf_list = []
        self.display_current_time("===== Step1: Summary dashboard =====")
        summary_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "summary", "quality_metrics", which_day, "d")
        summary_pdf_list = self.add_group_dashboard_id(summary_pdf_list, dashboard_id_info, "summary", "production_metrics", which_day, "d")
        pdf_list += summary_pdf_list

        self.display_current_time("===== Step2: Lower CPK Analysis =====")
        """Get grafana dashboard url"""
        dashboard_id = dashboard_id_info["cpk_analysis"]["test_item"]
        daily_low_cpk_url_dict = dict()
        all_test_item_type_list = []
        for daily_low_cpk in daily_low_cpk_item_list:
            model = daily_low_cpk["MODEL"]
            station_type = daily_low_cpk["STATION_TYPE"]
            test_item = daily_low_cpk["TEST_ITEM"]
            """Get Cpk<1 test items' grafana dashboard url"""
            name = model + station_type + test_item
            url = self.cpk_test_item_dashboard_url(dashboard_id, model, station_type, test_item, which_day)
            daily_low_cpk_url_dict[name] = url
            """Get test item type (eg. test item type of 2GPOW2 is 2GPOW) grafana dashboard url"""
            if test_item not in ["WAN-LAN-TPT"]:
                digit_index = self.check_string_char_digit_index(test_item)
                test_item_type = test_item[:-digit_index]
            elif test_item =="WAN-LAN-TPT":
                test_item_type = "WAN-LAN"
            else:
                continue
            test_item_type_list = [model, station_type, test_item_type]
            all_test_item_type_list.append(test_item_type_list)

        """Get sub test item url & Generate pdf for test item type"""
        dashboard_id = dashboard_id_info["cpk_analysis"]["test_item_type"]
        all_test_item_type_list = self.remove_repeat_list_in_list(target_list=all_test_item_type_list)
        all_test_item_type_url_dict = dict()
        for test_item_type in all_test_item_type_list:
            model, station_type, test_item_type = test_item_type[0], test_item_type[1], test_item_type[2]
            test_item_list = self.get_test_item_from_test_item_type(client, which_day, model, station_type, test_item_type)
            test_item_type_url = self.cpk_test_item_type_dashboard_url(dashboard_id, model, station_type, test_item_list, which_day)
            name = model+station_type+test_item_type
            all_test_item_type_url_dict[name] = test_item_type_url

        """Generate pdf for test item type"""
        low_cpk_test_item_type_list = self.generate_pdf_for_test_item_type(all_test_item_type_url_dict)

        """Generate pdf for Cpk<1 test item"""
        low_cpk_test_item_list = self.generate_pdf_for_test_item(daily_low_cpk_url_dict)
        temp_list = low_cpk_test_item_type_list + low_cpk_test_item_list
        temp_list = sorted(temp_list)
        pdf_list += temp_list

        """Merge all sub pdf into one pdf"""
        output_pdf = self.folder_path + self.save_file_name + '.pdf'
        print("Output PDF name: {}".format(output_pdf))
        self.merge_pdf(output_pdf, pdf_list)
        time_end = time.time()
        time_c = time_end - time_start
        print("time cost:", time_c, "s")

if __name__=="__main__":
    get_report = CDEDailyReportCpk()
    which_period = 1
    factory_list = ["CDE"]
    dashboard_id_dict = {
            "summary": {"quality_metrics": "NFU-UdnGz",
                        "production_metrics": "83ip3R_Gk"},
            "cpk_analysis": {"test_item": "vTdxhqfMz",
                             "test_item_type": "IA9ZaZYMz"}
    }
    try:
        get_report.main(factory_name=factory_list[0],
                        which_day=which_period,
                        dashboard_id_info=dashboard_id_dict)
    except Exception as e:
        print(e)
