from sunpy.net.dataretriever.client import GenericClient
from bs4 import BeautifulSoup
import urllib.request
import re


class SOTClient(GenericClient):
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
        self.map_['physobs'] = 'potato'
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
        chkattr = ['Time', 'Instrument', 'Level']
        chklist = [x.__class__.__name__ in chkattr for x in query]
        for x in query:
            if (
                x.__class__.__name__ == 'Instrument' and
                x.value.lower() == 'sot'
               ):
                return all(chklist)
        return False
