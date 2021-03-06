import sys

from django.core.management.base import NoArgsCommand
from django.db import connections

from geonames.models import Country, Geoname

"""
Generates geonames for all first-order administrative divisions, such as US
states and Canadia provinces. To run this command, first download the
allCountries.zip file::

    wget http://download.geonames.org/export/dump/allCountries.zip

After unzipping the allCountries.zip file and placing both files in the project
directory, you can run the command.
"""

class Command(NoArgsCommand):
    help = "Generates the countries geonames objects in JSON format"
    
    def handle_noargs(self, **options):
        print "Importing Geoname objects for the AMD1 geonmes in countryInfo.txt"
        cursor = connections['default'].cursor()
        cursor.execute('BEGIN')
        with open('allCountries.txt') as fd:
            i = 0
            for line in fd:
                i += 1
                if i % 100000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()

                fields = line.split('\t')
                geoname_id, name, ascii_name = fields[:3]
                latitude, longitude, fclass, fcode, country_id, cc2 = fields[4:10]
                
                if not fcode == 'ADM1':
                    continue
                
                population, elevation, gtopo30 = fields[14:17]
                moddate = fields[18]
                if elevation == '':
                    elevation = 0

                timezone_id = None
                name = unicode(name,'utf-8')
                admin1 = fields[10]
                admin2 = fields[11]
                admin3 = fields[12]
                admin4 = fields[13]
                admin1_id, admin2_id, admin3_id, admin4_id = [None] * 4
                
                try:
                    # Delete existing entries first, then insert
                    cursor.execute(u"DELETE FROM geoname WHERE id = %s", (geoname_id,))
                    cursor.execute(u"INSERT INTO geoname (id, name, ascii_name, point, fclass, fcode, country_id, cc2, admin1_id, admin2_id, admin3_id, admin4_id, population, elevation, gtopo30, timezone_id, moddate) VALUES (%s, %s, %s, GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (geoname_id, name, ascii_name, 'POINT(%s %s)' % (longitude, latitude), fclass, fcode, country_id, cc2, admin1_id, admin2_id, admin3_id, admin4_id, population, elevation, gtopo30, timezone_id, moddate))
                except Exception, e:
                    print 'Error: %s' % e
                    continue
        
        print 'Done!'
        cursor.execute('COMMIT')
        
        print "Complete! States and Provinces should now be available for querying."
