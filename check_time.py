from datetime import datetime, time


def time_in_range(start, end, x):
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end



def check_time_now(time_check):
    start = time(8, 45, 0)
    end = time(11, 31, 0)
    start_afternoon = time(13, 0, 0)
    end_afternoon = time(15, 0, 0)
    if (time_in_range(start, end, time_check) == True or time_in_range(start_afternoon, end_afternoon, time_check)==True):
        return True
    else:
        return False
