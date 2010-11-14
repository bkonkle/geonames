import re

from geonames.models import Geoname, GeonameAlternateName


us_address_re = re.compile(r'(?:(?P<number>\d+)\s+(?P<street>[\w\s]+),?\s+)?(?P<city>[\w\s]+),?\s+(?P<state>[A-Z]{2}|[\w\s]*)')


def geocode(query, first=True):
    """
    A simple geocoding function which tries to understand the query passed to
    it, and then look for a Geoname object to match it.  By default, it returns
    the first result.  If first is False, however, it returns a list of all
    results.
    """
    # Check for a US Address or 'City, ST'
    match = us_address_re.match(query)
    if match:
        number = match.groupdict()['number']
        street = match.groupdict()['street']
        city = match.groupdict()['city']
        state = match.groupdict()['state']
        
        if city and state:
            filters = {}
            if number and street:
                # TODO
                pass
            else:
                filters.update({'name__iexact': city})
                if len(state) == 2:
                    filters.update({'admin1__code__iexact': state})
                else:
                    filters.update({'admin1__name__iexact': state})
            results = Geoname.objects.filter(**filters)
            if first and results:
                return results[0]
            return results

def reverse_geocode(lat, lng):
    """
    A simple reverse geocoder that returns the Geoname closest to the given
    coordinates.
    """
    results = Geoname.objects.near_point(lat, lng)
    if results:
        return results[0]
