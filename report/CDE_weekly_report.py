from factory_report_template import WeeklyReportTemplate
from datetime import timedelta, date
import time

class CDEWeeklyReport(WeeklyReportTemplate):
    def __init__(self):
        super(CDEWeeklyReport, self).__init__()

    def main(self, factory_name, which_week, dashboard_id_info):
        time_start = time.time()
        last_week = (date.today() - timedelta(weeks=which_week)).strftime('%V')
        year = (date.today() - timedelta(weeks=which_week)).strftime('%Y')
        self.save_file_name = '{}W{}_{}_weekly_report'.format(year, last_week, factory_name)
        print("save_file_name: %s"%(self.save_file_name))

        pdf_list=[]
        """Summary page for Production, FPY & CPK"""
        self.display_current_time("===== Step1: Summary page =====")
        summary_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "summary", "summary_page", which_week, "w")
        pdf_list += summary_pdf_list

        """WIP information"""
        self.display_current_time("===== Step2: WIP information =====")
        wip_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "summary", "wip_page", which_week, "w")
        pdf_list += wip_pdf_list

        """Lower CPK summary"""
        self.display_current_time("===== Step3: Lower CPK test item Summary dashboard =====")
        lower_cpk_summary_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "cpk_analysis", "lower_cpk_summary", which_week, "w")
        lower_cpk_summary_pdf_list = self.add_group_dashboard_id(lower_cpk_summary_pdf_list, dashboard_id_info, "cpk_analysis", "normal_cpk_lower_accuracy", which_week, "w")
#        lower_cpk_summary_pdf_list = self.add_group_dashboard_id(lower_cpk_summary_pdf_list, dashboard_id_info, "cpk_analysis", "higher_cpk", which_week, "w")
        pdf_list += lower_cpk_summary_pdf_list

        """Merge all sub pdf into one pdf"""
        self.display_current_time("=====Step3: Merge all pdf into one pdf=====")
        output_pdf = self.folder_path + self.save_file_name + '.pdf'
        self.merge_pdf(output_pdf, pdf_list)
        time_end = time.time()
        time_c = time_end - time_start
        print("time cost:", time_c, "s")

if __name__=="__main__":
    get_report = CDEWeeklyReport()
    which_period = 1
    factory_list = ["CDE"]
    dashboard_id_dict = {
            "summary": {"summary_page": "jrq425VMk",
                        "wip_page": "itF2NPSGz"},
            "cpk_analysis": {"lower_cpk_summary": "t6Sd-nEGk",
                             "test_item": "DQ2C9-LGz",
                             "normal_cpk_lower_accuracy": "RLYhneEGz",
                             "higher_cpk": "sPBCGvyGz"}
    }
    try:
        get_report.main(factory_name=factory_list[0],
                        which_week=which_period,
                        dashboard_id_info=dashboard_id_dict)
    except:
        print("Skip")
