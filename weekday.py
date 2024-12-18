from datetime import datetime, timedelta

def get_timedelta():
    hoje = datetime.now().weekday()
    if hoje in (0,1,2,3):
        return 14
    elif hoje == 4:
        return 12
    else:
        pass

def get_start_date():
    current_date = datetime.now()
    start_date = current_date - timedelta(days=get_timedelta())
    return start_date.strftime('%Y-%m-%d')