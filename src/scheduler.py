import pandas as pd
import pytz

from datetime import datetime, timedelta
from intervals import intervals
from onsale_intervals import on_sale_intervals

                
TIMEZONE = 'America/Los_Angeles'
EXECUTIONS_PER_SECOND = 9 
MAX_MINUTE_RANGE = 105
SECOND_SET = {i: 0 for i in range(MAX_MINUTE_RANGE*60)}  
        
OUTPUT_FILE = 'Schedules Distribution.xlsx'
   
def run():
    csv_file_path = 'events.csv'
    df = pd.read_csv(csv_file_path)
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df['onsale'] = pd.to_datetime(df['onsale'], errors='coerce')
    
    current_date = datetime.now(pytz.utc) 
    on_sale_interval = on_sale_intervals[0]
    
    df_sorted = sort_events_df(current_date, on_sale_interval, df)
    
    scheduled_datetimes = {}
    schedules_list = []
    msg = None
    
    for index, row in df_sorted.iterrows():
        event = row['skybox_event_id']
        
        try:
            event_date = row['datetime']
            
            if not event_date or event_date.year > 2099:
                print(f'Event has no date {event}') 
                continue
            
            event_date_utc = get_event_date_utc(event_date)
            days_remaining = (event_date_utc - current_date).days
                    
            if days_remaining < 0:
                continue
            
            on_sale_utc = get_on_sale_utc(row['onsale'])
                    
            if on_sale_utc and event_date_utc <= on_sale_utc:
                print(f'Cannot have an event occur before/on the onsale {event}') 
                continue
                    
            interval = calculate_interval(intervals, days_remaining)
            create_daily_schedules(current_date, interval, scheduled_datetimes, 
                                   schedules_list, event, on_sale_utc, on_sale_interval)
        except Exception as e:
            msg = f"Unable to generate schedules for event {event}. Error: {e}"
            print(msg)     
    
    if len(schedules_list) > 0:
        schedules_df = pd.DataFrame(schedules_list)
        schedules_df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
    else:
        msg = "There were no schedules generated"
        print(msg)


def sort_events_df(current_date, on_sale_interval, df):
    def adjust_sort_date(row):
        if not pd.isna(row['onsale']):
            on_sale = row['onsale']
            on_sale_local = localize(on_sale)
            on_sale_utc = on_sale_local.astimezone(pytz.utc)
            on_sale_low_day = on_sale_utc + timedelta(days=on_sale_interval['low_day'])
            on_sale_high_day = on_sale_utc + timedelta(days=on_sale_interval['high_day'])

            if on_sale_low_day <= current_date <= on_sale_high_day:
                if on_sale_utc.date() < current_date.date():
                    return current_date.replace(tzinfo=None,hour=0,minute=0,microsecond=0)
                else:
                    return row['onsale']
        return row['datetime']

    df['sort_date'] = df.apply(adjust_sort_date, axis=1)
    return df.sort_values(by=['sort_date', 'onsale', 'datetime'])


def get_event_date_utc(event_date):
    event_datetime_local = localize(event_date)
    return event_datetime_local.astimezone(pytz.utc)


def get_on_sale_utc(on_sale):
    on_sale_utc = None
                    
    if not pd.isna(on_sale) and on_sale.year < 2099:
        on_sale_local = localize(on_sale)
        on_sale_utc = on_sale_local.astimezone(pytz.utc)
        
    return on_sale_utc
    
    
def localize(dt: datetime):
    """ makes datetime objects tz-aware as UTC """
    return pytz.timezone(TIMEZONE).localize(dt)
        
        
def calculate_interval(intervals, number):
    low, high = 0, len(intervals) - 1

    while low <= high:
        mid = (low + high) // 2
        current_interval = intervals[mid]
            
        if current_interval['high_day'] == None:
            current_interval['high_day'] = 99999

        if int(current_interval['low_day']) <= number <= int(current_interval['high_day']):
            return int(current_interval['autoprice_interval'])
        elif number < int(current_interval['low_day']):
            high = mid - 1
        else:
            low = mid + 1

    return None 


def create_daily_schedules(current_date, interval, scheduled_datetimes, created_schedules, event, on_sale, on_sale_interval):
    first_day, first_hour, first_minute, first_second = distribute_first_schedule(current_date)  
    schedule = current_date.replace(hour=first_hour, minute=first_minute, second=first_second, microsecond=0)
    tomorrow = current_date + timedelta(days=1)
            
    has_on_sale = False
    if not pd.isna(on_sale):
        has_on_sale = True
        os_low_day, os_high_day, os_interval = calculate_onsale_interval(on_sale, on_sale_interval)
                    
    while schedule < tomorrow:                
        if schedule not in scheduled_datetimes or scheduled_datetimes[schedule] < EXECUTIONS_PER_SECOND:
            if schedule not in scheduled_datetimes:
                scheduled_datetimes[schedule] = 1
            else:
                scheduled_datetimes[schedule] += 1
                
            if (has_on_sale and os_low_day <= schedule <= os_high_day):
                current_interval = os_interval
            else:
                current_interval = interval
            
            created_schedules.append({'event': event, 'datetime': schedule.strftime("%Y-%m-%d %H:%M:%S"), 'interval': current_interval})
            adjust_second_distribution(schedule, first_day, current_date.hour)
            schedule += timedelta(minutes=current_interval)
        else:
            schedule += timedelta(seconds=1)
            
                        
def calculate_onsale_interval(on_sale, on_sale_interval):
    on_sale_low_day = on_sale + timedelta(days=on_sale_interval['low_day'])
    on_sale_high_day = on_sale + timedelta(days=on_sale_interval['high_day'])
    os_interval = on_sale_interval['autoprice_interval']
    
    return on_sale_low_day, on_sale_high_day, os_interval    


def adjust_second_distribution(schedule, first_day, first_hour):
    seconds_in_minute = 60 * 60
    second_hour_minutes = MAX_MINUTE_RANGE - 60
    
    if (schedule.day == first_day and 
        (schedule.hour == first_hour or
         (schedule.hour == first_hour + 1 and schedule.minute < second_hour_minutes))):
        seconds = schedule.minute * 60 + schedule.second
        
        if schedule.hour > first_hour:
            seconds += seconds_in_minute
        
        SECOND_SET[seconds] += 1
        
    
def distribute_first_schedule(current_date):
    min_second = min(SECOND_SET, key=SECOND_SET.get)
    hours = min_second // 3600
    minutes = (min_second % 3600) // 60
    seconds = min_second % 60
    return current_date.day, current_date.hour+hours, minutes, seconds


if __name__ == '__main__':
    run()