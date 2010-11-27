import re

from geonames.models import Geoname, GeonameAlternateName, Country

us_state_abbrs = 'AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|MD|MA|MI|MN|MS|MO|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY'
us_states = 'Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|District of Columbia|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming'
us_address_re = re.compile(r'(?:(?P<number>\d+)\s+(?P<street>[\w\s]+),?\s+)?(?P<city>[\w\s]+),?\s+(?P<state>%s|%s)' % (us_states, us_state_abbrs), re.I)

can_prov_abbrs = 'AB|BC|MB|NB|NL|NT|NS|NU|ON|PE|QC|SK|YT'
can_provinces = 'Alberta|British Columbia|Manitoba|New Brunswick|Newfoundland and Labrador|Northwest Territories|Nova Scotia|Nunavut|Ontario|Prince Edward Island|Quebec|Saskatchewan|Yukon'
can_city_prov_re = re.compile(r'(?P<city>[\w\s]+),?\s+(?P<province>%s|%s)' % (can_provinces, can_prov_abbrs), re.I)

city_country_re = re.compile(r'(?P<city>[\w\s]+?),?\s+(?P<country>[\w\s]+)', re.I)


def geocode(query, first=True):
    """
    A simple geocoding function which tries to understand the query passed to
    it, and then look for a Geoname object to match it.  By default, it returns
    the first result.  If first is False, however, it returns a list of all
    results.
    """
    # Check for a US Address or 'City, State'
    match = us_address_re.match(query)
    if match:
        number = match.groupdict()['number']
        street = match.groupdict()['street']
        city = match.groupdict()['city']
        state = match.groupdict()['state']
        if city and state:
            filters = {}
            filters.update({'name__iexact': city})
            if len(state) == 2:
                filters.update({'admin1__code__iexact': state})
            else:
                filters.update({'admin1__name__iexact': state})
            if number and street:
                # The geonames database doesn't actually include street and
                # street number, so they can be ignored.
                pass
            results = Geoname.objects.filter(**filters)
            if first and results:
                return results[0]
            return results
    
    # Check for Canadian 'City, Province'
    match = can_city_prov_re.match(query)
    if match:
        city = match.groupdict()['city']
        province = match.groupdict()['province']
        if city and province:
            filters = {'name__iexact': city}
            if len(province) == 2:
                filters.update({'admin1__code__iexact': province})
            else:
                filters.update({'admin1__name__iexact': province})
            results = Geoname.objects.filter(**filters)
            if first and results:
                return results[0]
            return results
    
    # Check for 'City, Country'
    match = city_country_re.match(query)
    if match:
        city = match.groupdict()['city']
        country = match.groupdict()['country']
        if city and country:
            filters = {'name__iexact': city}
            if len(country) == 2:
                if country.lower() == 'uk':
                    country = 'GB'
                filters.update({'country__iso_alpha2__iexact': country})
            elif len(country) == 3:
                filters.update({'country__iso_alpha3__iexact': country})
            else:
                filters.update({'country__name__iexact': country})
            results = Geoname.objects.filter(**filters)
            if first and results:
                return results[0]
            return results
    
    # Try the name directly, and sort the results by population
    results = Geoname.objects.filter(
        name__iexact=query
    ).order_by('-population')
    if first and results:
        return results[0]
    return results


def reverse_geocode(lat, lng, cities=False):
    """
    A simple reverse geocoder that returns the Geoname closest to the given
    coordinates.  Will optionally search only cities.
    """
    if -180.0 < float(lat) < 180.0 and -180.0 < float(lng) < 180.0:
        return Geoname.objects.closest_to_point(lat, lng, cities=cities)
    raise ValueError('The latitude and longitude must be between -180 and 180')
