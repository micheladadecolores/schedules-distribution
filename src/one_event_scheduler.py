import pandas as pd
import pytz

from datetime import datetime, timedelta

from intervals import intervals
from onsale_intervals import on_sale_intervals
from scheduler import (
    get_event_date_utc,
    get_on_sale_utc,
    calculate_interval,
    calculate_onsale_interval,
    EXECUTIONS_PER_SECOND
)

OUTPUT_FILE = 'New Schedule Distribution.xlsx'

def run(new_event_id: int, event_date: datetime, onsale: datetime):             
    current_date = datetime.now(pytz.utc) 
    on_sale_interval = on_sale_intervals[0]
    
    schedules_list = []
    msg = None
        
    try:
            
        if not event_date or event_date.year > 2099:
            msg = f'Event has no date {new_event_id}'
            print(msg) 
            return msg
            
        event_date_utc = get_event_date_utc(event_date)
        days_remaining = (event_date_utc - current_date).days
                    
        if days_remaining < 0:
            msg = f'Event already happened {new_event_id}'
            print(msg)
            return msg
        
        on_sale_utc = get_on_sale_utc(onsale)
                    
        if on_sale_utc and event_date_utc <= on_sale_utc:
            msg = f'Cannot have an event occur before/on the onsale {new_event_id}'
            print(msg)
            return msg
                    
        interval = calculate_interval(intervals, days_remaining)
        create_event_daily_schedule(current_date, interval, schedules_list, new_event_id,
                                    on_sale_utc, on_sale_interval)
    except Exception as e:
        msg = f"Unable to generate schedules for event {new_event_id}. Error: {e}"
        print(msg)     
    
    if len(schedules_list) > 0:          
        schedules_df = pd.DataFrame(schedules_list)
        schedules_df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
    else:
        msg = "There were no schedules generated"
        print(msg)
        


def create_event_daily_schedule(current_date, interval, created_schedules, skybox_event_id, on_sale, on_sale_interval):
    schedule = get_open_schedule(current_date)
    tomorrow = current_date + timedelta(days=1)
            
    has_on_sale = False
    if not pd.isna(on_sale):
        has_on_sale = True
        os_low_day, os_high_day, os_interval = calculate_onsale_interval(on_sale, on_sale_interval)
                    
    while schedule < tomorrow:   
        if (has_on_sale and os_low_day <= schedule <= os_high_day):
            current_interval = os_interval
        else:
            current_interval = interval
            
        created_schedules.append({'event': skybox_event_id, 'datetime': schedule.strftime("%Y-%m-%d %H:%M:%S"), 'interval': current_interval})
        interval_schedule = schedule + timedelta(minutes=current_interval)
        schedule = get_open_schedule(interval_schedule)
        

def get_open_schedule(schedule):    
    schedule_str = schedule.strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT datetime as schedule
        FROM ##YOURTABLE##
        WHERE `datetime` >= '{schedule_str}'
        GROUP BY datetime
        HAVING COUNT(*) < {EXECUTIONS_PER_SECOND}
        ORDER BY datetime
        LIMIT 1
    """ 
    #RUN QUERY and change tu UTC .astimezone(pytz.utc)
        
    return query


if __name__ == '__main__':
    run()