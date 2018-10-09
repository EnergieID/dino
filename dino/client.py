import requests
import pandas as pd

__title__ = "dino"
__version__ = "0.0.1"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://www.connetcontrolcenter.com/testjson/jsonanswerdino.php'


class DinoClient:
    def __init__(self, client_id, client_secret, username, serial):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.serial = serial

        self.session = requests.Session()
        # self.session.headers.update({
        #    "Accept": "application/json",
        #    "Content-Type": "application/json"
        #})

    def get_data_raw(self, start, end):
        """
        Get data and return the response as dict

        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
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
        r = self.session.post(url=URL, json=data)
        r.raise_for_status()
        j = r.json()
        return j

    def get_data(self, start, end, columns=None):
        """
        Get data and return as a parsed Pandas DataFrame

        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        columns : [str], optional
                select only specific entries to use

        Returns
        -------
        pd.DataFrame
        """
        j = self.get_data_raw(start=start, end=end)
        df = self.dict_to_df(d=j, columns=columns)
        return df

    @staticmethod
    def dict_to_df(d, columns=None):
        """
        Parse dict to Pandas DataFrame

        Possible values:
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
        d : dict
            dict with the values described above
        columns : [str], optional
                select only specific entries to use

        Returns
        -------
        pd.DataFrame
        """
        def gen_series(_d):
            for key, val in _d.items():
                try:
                    ts = pd.DataFrame(val)
                except ValueError:
                    continue
                if ts.empty:
                    yield pd.Series(name=key)
                else:
                    ts.set_index(1, drop=True, inplace=True)
                    ts = ts[0]  # go from dataframe to series
                    ts.index = ts.index.map(lambda v: pd.to_datetime(v, unit='s'))

                    ts.index.name = None
                    ts.name = key

                    yield ts

        if columns is not None:
            d = {key: d[key] for key in d if key in columns}

        df = pd.concat(gen_series(_d=d), axis=1)

        return df
