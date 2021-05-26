import os
from datetime import datetime, timedelta
from ftplib import FTP

class catchDataFromFtp:
    def __init__(self):
        self.factory_jet_lag = {"CDE": 0, "FLEX": 12, "TwoWing": 0}

    def main(self, factory_name, shift_hour=0):
        start_time = datetime.now()
        print('start_time: ', start_time)
        shift_hour = shift_hour + self.factory_jet_lag[factory_name]
        self.catch_file_from_ftp(shift_hour=shift_hour)
        end_time = datetime.now()
        print("end_time: ", end_time)

    def catch_file_from_ftp(self, shift_hour):
        ftp = self.ftp_login()
        filenamelist = self.ftp_get_filename_list(ftp=ftp)
        file_timestamp_day, file_timestamp_hour = self.file_timestamp(shift_hour=shift_hour)
        local_path = os.path.dirname(os.getcwd()) + "/" + "Raw_data/%s/"%(file_timestamp_day)
        if not os.path.exists(local_path):
            os.mkdir(local_path)
        today_file_list = self.ftp_check_file(filenamelist=filenamelist, file_timestamp_hour=file_timestamp_hour)
        if today_file_list!=[]:
            self.ftp_copy_file_to_local(ftp=ftp, today_file_list=today_file_list, local_path=local_path)
            self.ftp_delete_file(ftp=ftp, today_file_list=today_file_list)
        else:
            print("File does not exist.")
        ftp.set_debuglevel(0)
        ftp.quit()

    def ftp_login(self, host="ftp2.compalbn.com", user_name="cbn_grafana", password="HGB7Z3mf"):
        ftp=FTP()
        ftp.set_debuglevel(2)
        ftp.connect(host)
        ftp.login(user_name, password)
        return ftp

    def ftp_get_filename_list(self, ftp):
        filelist = []
        ftp.retrlines("LIST", filelist.append)
        filenamelist = []
        for item in filelist:
            words = item.split(None, 8)
            filename = words[-1].lstrip()
            filenamelist.append(filename)
        return filenamelist

    def file_timestamp(self, shift_hour):
        file_timestamp = datetime.now() - timedelta(hours=shift_hour)
        file_timestamp_day = file_timestamp.strftime('%Y%m%d')
        file_timestamp_hour = file_timestamp.strftime('%Y%m%d-%H')
        return file_timestamp_day, file_timestamp_hour

    def ftp_check_file(self, filenamelist, file_timestamp_hour):
        today_file_list = [item for item in filenamelist if file_timestamp_hour in item]
        return today_file_list

    def ftp_copy_file_to_local(self, ftp, today_file_list, local_path):
        bufsize=1024
        for item in today_file_list:
            ftp.retrbinary("RETR %s"%(item), open(local_path+item, 'wb').write, bufsize)

    def ftp_delete_file(self, ftp, today_file_list):
        for item in today_file_list:
            ftp.delete(item)

if __name__=="__main__":
    ftp_data = catchDataFromFtp()
    factory_name_list = ["CDE", "TwoWing", "FLEX"]
    hour_range = 8
    for factory_name in factory_name_list:
        for shift_hour in reversed(range(hour_range)):
            try:
                ftp_data.main(factory_name=factory_name, shift_hour=shift_hour)
            except Exception as e:
                print(e)
