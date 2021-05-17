from factory_report_template import MonthlyReportTemplate
from datetime import date
from dateutil.relativedelta import relativedelta
import time

class TwoWingMonthlyReport(MonthlyReportTemplate):
    def __init__(self):
        super(TwoWingMonthlyReport, self).__init__()

    def main(self, factory_name, which_month, dashboard_id_info):
        time_start = time.time()
        last_month = (date.today() - relativedelta(months=which_month)).strftime('%Y-%b')
        self.save_file_name = '{}_{}_monthly_report'.format(last_month, factory_name)
        print(self.save_file_name)

        pdf_list=[]
        """Summary page for Production, FPY & CPK"""
        self.display_current_time("===Step1: Summary page===")
        summary_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "summary", "summary_page", which_month, "M")
        pdf_list += summary_pdf_list

        """CPK Analysis"""
        self.display_current_time("===Step3: CPK analysis===")
        cpk_analysis_pdf_list = self.add_group_dashboard_id([], dashboard_id_info, "cpk_analysis", "lower_cpk_summary", which_month, "M")
        cpk_analysis_pdf_list = self.add_group_dashboard_id(cpk_analysis_pdf_list, dashboard_id_info, "cpk_analysis", "normal_cpk_lower_accuracy", which_month, "M")
        cpk_analysis_pdf_list = self.add_group_dashboard_id(cpk_analysis_pdf_list, dashboard_id_info, "cpk_analysis", "higher_cpk", which_month, "M")
        pdf_list += cpk_analysis_pdf_list

        """Merge all sub pdf into one pdf"""
        self.display_current_time("===Step3: Merge all pdf into one pdf===")
        output_pdf = self.folder_path + self.save_file_name + '.pdf'
        self.merge_pdf(output_pdf, pdf_list)
        time_end = time.time()
        time_c = time_end - time_start
        print("time cost:", time_c, "s")

if __name__=="__main__":
    month_report = TwoWingMonthlyReport()
    which_period = 1
    factory_list = ["TwoWing"]
    dashboard_id_dict = {
            "summary": {"summary_page": "kFdGbUSMk"},
            "cpk_analysis": {"lower_cpk_summary": "YS9DLrPMz",
                             "normal_cpk_lower_accuracy": "UW6eA6PGz",
                             "higher_cpk": "gIa6DvsMz"}
    }
    try:
        month_report.main(factory_name=factory_list[0],
                          which_month=which_period,
                          dashboard_id_info=dashboard_id_dict)
    except Exception as e:
        print(e)
