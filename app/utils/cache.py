from datetime import datetime, timedelta

cache = {}
CACHE_EXPIRY = 60 * 24  # Cache duration in minutes

def set_to_cache(key, value):
    expiry_time = datetime.now() + timedelta(minutes=CACHE_EXPIRY)
    cache[key] = (value, expiry_time)

def get_from_cache(key):
    if key in cache:
        value, expiry_time = cache[key]
        if datetime.now() < expiry_time:
            return value
        else:
            del cache[key]
    return None