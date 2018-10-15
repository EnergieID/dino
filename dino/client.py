import requests
import pandas as pd

__title__ = "dino"
__version__ = "0.0.4"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://www.connetcontrolcenter.com/testjson/jsonanswerdino.php'


class RawDinoClient:
    """API Client that returns JSON data from the server"""
    def __init__(self, client_id, client_secret, username, serial, timeout=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.serial = serial

        self.session = requests.Session()
        # self.session.headers.update({
        #    "Accept": "application/json",
        #    "Content-Type": "application/json"
        #})
        self.timeout = timeout

    def get_data(self, start, end):
        """
        Parameters
        ----------
        start : datetime.datetime
        end : datetime.datetime
        Returns
        -------
        dict
        """
        data = {
            'userName': self.username,
            'seriale': self.serial,
            'datada': start.strftime('%d/%m/%Y'),
            'dataa': end.strftime('%d/%m/%Y'),
            'OEMusername': self.client_id,
            'OEMpassword': self.client_secret
        }

        self.session.verify = False
        r = self.session.post(url=URL, json=data, timeout=self.timeout)
        r.raise_for_status()
        j = r.json()
        return j


class PandasDinoClient(RawDinoClient):
    """API Client that parses data in a Pandas DataFrame"""
    def get_data(self, start, end, columns=None):
        """
        Get data and return as a parsed Pandas DataFrame

        Possible column values:
            - "E0":[] produced energy as counter (i.e. [32036140,1518908534] first value is the counter, second value is unix time timestamp)
            - "DE0":[] produced energy as difference with the previous sample
            - "E1":[],"DE1":[] energy sold
            - "E2":[],"DE2":[] energy bought
            - "ET0":[],"DET0":[],"ET1":[],"DET1":[],"ET2":[],"DET2":[], energy bought in the various timeslots
            - "P0":[],"DP0":[], power bought
            - "PE":[],"DPE":[], energy bought the previous period
            - "R":[],"DR":[] reactive energy

            - DailyDE0, etc.: daily difference for the various measures
            - HourlyDE0 etc.: hourly difference for the various measures

        Parameters
        ----------
        start : datetime.datetime
        end : datetime.datetime
        columns : [str], optional
                select only specific entries to use

        Returns
        -------
        pd.DataFrame
        """
        d = super(PandasDinoClient, self).get_data(start=start, end=end)

        if columns is not None:
            d = {key: d[key] for key in d if key in columns}

        series = self._dict_to_series(d)
        df = pd.concat(series, axis=1)

        return df

    @staticmethod
    def _dict_to_series(d):
        """
        Generates a Pandas Series for each element in the dict

        Parameters
        ----------
        d : dict

        Yields
        -------
        pd.Series
        """
        for key, val in d.items():
            try:
                ts = pd.DataFrame(val)
            except ValueError:
                continue
            if ts.empty:
                continue
            else:
                ts.set_index(1, drop=True, inplace=True)
                ts = ts[0]  # go from dataframe to series
                ts.index = pd.to_datetime(ts.index, unit='s', utc=True)

                ts.index.name = None
                ts.name = key

                yield ts
