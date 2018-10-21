from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim


def get_random_location(query):
    try:
        geo = Nominatim()
        location = geo.geocode(query)
        lat1, lat2, long1, long2 = map(float, location.raw['boundingbox'])
        latitude, longitude = random.uniform(lat1, lat2), random.uniform(long1, long2)
        # location2 = geo.reverse('%f, %f' % (latitude, longitude))
        # print(location2)
        return latitude, longitude
    except GeocoderTimedOut as e:
        log(str(e))
        return 55.9320842394594, 37.5631882631847


def location_to_name(latitude, longitude):
    result = ''
    try:
        result = Nominatim().reverse('%f, %f' % (latitude, longitude))
    except:
        result = ''
    return result