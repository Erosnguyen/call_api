from datetime import datetime, time


def time_in_range(start, end, x):
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end



def check_time_now():
    start = time(8, 40, 0)
    end = time(15, 0, 0)
    if (time_in_range(start, end, datetime.now().time()) == True):
        return True
    else:
        return False
