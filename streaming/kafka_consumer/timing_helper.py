import time
import datetime


def wait_until(target_ts):
    """Wait until the precise float timestamp."""
    sleep_time = target_ts - time.time()
    if sleep_time > 0.002:
        time.sleep(sleep_time - 0.001)

    while time.time() < target_ts:
        pass


def schedule_function(target_second, f, arg):
    """Run once at the next occurrence of the given second (0–59)."""
    now = datetime.datetime.now()
    target = now.replace(second=target_second, microsecond=0)

    if target <= now:
        target += datetime.timedelta(minutes=1)

    target_ts = target.timestamp()
    wait_until(target_ts)

    now = datetime.datetime.now()
    print("Function is executed at:", now.strftime("%Y-%m-%d %H:%M:%S.%f"), flush=True)
    f(arg)


def schedule_repeating_function(start_second, interval, f, arg):
    """Run first at start_second, then repeat every `interval` seconds."""
    now = datetime.datetime.now()
    target = now.replace(second=start_second, microsecond=0)

    if target <= now:
        target += datetime.timedelta(minutes=1)

    next_ts = target.timestamp()

    while True:
        wait_until(next_ts)

        now = datetime.datetime.now()
        print("Function is executed at:", now.strftime("%Y-%m-%d %H:%M:%S.%f"), flush=True)
        f(arg)

        next_ts += interval

