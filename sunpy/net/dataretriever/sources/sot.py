from sunpy.net.dataretriever.client import GenericClient
from bs4 import BeautifulSoup
import urllib.request
import re
from sunpy.time import TimeRange, parse_time


class SPClient(GenericClient):
    """
    Provides access to Level 2 SOT SP fits files
    `archive <http://www.lmsal.com/solarsoft/hinode/level2hao/>`__ hosted
    by the `Lockheed Martin Solar and Astrophysics Lab <http://sot.lmsal.com/>`__
    and mirrored from the `Community Spectro-polarimetric Analysis Center <https://www2.hao.ucar.edu/csac>`__.

    Examples
    --------

    >>> from sunpy.net import Fido, attrs as a
    >>> results = Fido.search(a.Time('2019/06/09 00:00', '2019/06/11 23:59'),
    ...                       a.Instrument('sotsp'),
    ...                       a.Level('2'))
    >>> results  #doctest: +REMOTE_DATA +ELLIPSIS
    <sunpy.net.fido_factory.UnifiedResponse object at ...>
    Results from 1 Provider:
    <BLANKLINE>
    18 Results from the SPClient:
     Start Time           End Time      Source Instrument Wavelength
       str19               str19         str3     str3       str3
    ------------------- ------------------- ------ ---------- ----------
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
                    ...                 ...    ...        ...        ...
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    2019-06-09 00:00:00 2019-06-11 23:59:00    hao        sot        nan
    <BLANKLINE>
    <BLANKLINE>

    """

    def _get_url_for_timerange(self, timerange, **kwargs):
        """
        Returns a list of URLs to the SOT SP data for the specified time range.

        Parameters
        ----------
        timerange: sunpy.time.TimeRange
            time range for which data is to be downloaded.

        Returns
        -------
        urls : list
            list of URLs corresponding to the requested time range
        """

        start = int(timerange.start.strftime('%Y%m%d%H%M%S'))
        end = int(timerange.end.strftime('%Y%m%d%H%M%S'))
        range_start = int(timerange.start.strftime('%Y%m%d'))
        range_end = int(timerange.end.strftime('%Y%m%d'))
        result = list()

        for date in range(range_start, range_end + 1):
            result += self._get_url_for_date(str(date), start, end)

        return result

    def _get_url_for_date(self, date, start, end):
        """
        Return URLs for corresponding date.

        Parameters
        ----------
        date : str
        start : str
            Start of queried time range.
        end : str
            End of queried time range.

        Returns
        -------
        list
            List of URLs for the corresponding date.
        """

        base_url = 'http://www.lmsal.com/solarsoft/hinode/level2hao/'
        url = (base_url + date[:4] + '/' + date[4:6] + '/' + date[6:8] +
               '/SP3D/')
        resp = urllib.request.urlopen(url)
        soup = BeautifulSoup(resp)
        results = list()

        for link in soup.find_all('a'):
            link = link.get('href')
            if (
                re.compile(r'^' + date).match(link) and
                int(link[-16:-8] + link[-7:-1]) <= end and
                int(link[-16:-8] + link[-7:-1]) >= start
               ):
                results.append(url + link + link[:-1] + '.fits')
        return results

    def _makeimap(self):
        """
        Helper function used to hold information about source.
        """
        self.map_['source'] = 'hao'
        self.map_['instrument'] = 'sot'
        self.map_['physobs'] = 'stokes_inversions'
        self.map_['provider'] = 'csac'

    @classmethod
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
            if (
                x.__class__.__name__ == 'Instrument' and
                x.value.lower() != 'sotsp'
               ):
                return False
            elif (
                x.__class__.__name__ == 'Level' and x.value != '2' and
                x.value != float(2)
               ):
                return False
            elif x.__class__.__name__ == 'Time':
                start = (
                         x.start.start().strftime('%Y%m%d%H%M%S')
                         if isinstance(x.start, TimeRange)
                         else parse_time(x.start).strftime('%Y%m%d%H%M%S')
                        )
                if int(start) < 20061026161012:
                    return False
        return True
