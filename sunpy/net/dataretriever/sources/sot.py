from sunpy.net.dataretriever.client import GenericClient
from bs4 import BeautifulSoup
import urllib.request
import re
from sunpy.time import TimeRange, parse_time


class SPClient(GenericClient):
    def _get_url_for_timerange(self, timerange, **kwargs):
        base_url = 'http://www.lmsal.com/solarsoft/hinode/level2hao/'
        start = int(timerange.start.strftime('%Y%m%d%H%M%S'))
        end = int(timerange.end.strftime('%Y%m%d%H%M%S'))
        range_start = int(timerange.start.strftime('%Y%m%d'))
        range_end = int(timerange.end.strftime('%Y%m%d'))
        result = list()

        for date in range(range_start, range_end + 1):
            date = str(date)
            url = (base_url + date[:4] + '/' + date[4:6] + '/' + date[6:8] +
                   '/SP3D/')
            resp = urllib.request.urlopen(url)
            soup = BeautifulSoup(resp)

            for link in soup.find_all('a'):
                link = link.get('href')
                if (
                    re.compile(r'^' + date).match(link) and
                    int(link[-16:-8] + link[-7:-1]) <= end and
                    int(link[-16:-8] + link[-7:-1]) >= start
                   ):
                    result.append(url + link + link[:-1] + '.fits')

        return result

    def _makeimap(self):
        self.map_['source'] = 'hao'
        self.map_['instrument'] = 'sot'
        self.map_['physobs'] = 'stokes_parameters'
        self.map_['provider'] = 'csac'

    def _can_handle_query(cls, *query):
        """
        Answers whether client can service the query.

        Parameters
        ----------
        query : list of query objects

        Returns
        -------
        boolean
            answer as to whether client can service the query
        """

        for x in query:
            if x.__class__.__name__ == 'Time':
                start = (
                         x.start.start().strftime('%Y%m%d%H%M%S') if isinstance(x.start, TimeRange)
                         else parse_time(x.start).strftime('%Y%m%d%H%M%S')
                        )
                if int(start) < 20061026161012:
                    return False
            if x.__class__.__name__ == 'Instrument' and x.value.lower() != 'sp':
                return False
            if x.__class__.__name__ == 'Level' and x.value != '2' and x.value != float(2):
                return False
        return True
