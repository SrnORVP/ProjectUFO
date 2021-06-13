
from datetime import datetime as dt


def Get_Formatted_Date():
    return dt.now().strftime("%d%b%y")


def Get_Detailed_Time():
    # temp = '-' + str(dt.now().hour * 360 + dt.now().minute * 6 + int(dt.now().second/10))
    return dt.now().strftime("%y'%m%d'%H%M")

