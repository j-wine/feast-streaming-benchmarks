import datetime
import time

def my_function(time: datetime.datetime):
    print("Function is executed at:", time.strftime("%Y-%m-%d %H:%M:%S.%f"))

def schedule_function(target_second,f):
    now = datetime.datetime.now()
    # Set target time with 0 microseconds
    target_time = now.replace(second=target_second, microsecond=0)

    # If the target time is earlier in the current minute, shift to next minute
    if target_time <= now:
        target_time += datetime.timedelta(minutes=1)

    # Calculate total wait time
    wait_time = (target_time - now).total_seconds()

    # Sleep until just before the target time (with small margin)
    if wait_time > 0.5:
        time.sleep(wait_time - 0.5)

    # High-precision wait loop
    while True:
        now = datetime.datetime.now()
        if now >= target_time:
            break
        # Very short sleep to yield CPU, allows for fine timing
        time.sleep(0.0001)

    f(now)

schedule_function(30,my_function)
