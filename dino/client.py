import requests
import pandas as pd

__title__ = "dino"
__version__ = "0.0.1"
__author__ = "EnergieID.be"
__license__ = "MIT"

URL = 'https://www.connetcontrolcenter.com/testjson/jsonanswerdino.php'


class DinoClient:
    def __init__(self, client_id=None, client_secret=None, username=None, serial=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.serial = serial

        self.session = requests.Session()
        # self.session.headers.update({
        #    "Accept": "application/json",
        #    "Content-Type": "application/json"
        #})

    def get_data(self, start, end):
        """
        Get data and parse them into a flat DataFrame
        Parameters
        ----------
        start : pd.Timestamp
        end : pd.Timestamp
        Returns
        -------
        pd.DataFrame
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

        return self.map_json(r.json())


    def map_json(self, json):
        """
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

        :param json: dict with the values described above
        :return: pd.DataFrame
        """
        columns = ['production', 'timestamp']
        production = pd.DataFrame(json['DailyDE0'], columns=columns).set_index('timestamp')
        production.index = production.index.map(lambda v: pd.to_datetime(v, unit='s'))

        # injection
        columns = ['injection', 'timestamp']
        injection = pd.DataFrame(json['DailyDE1'], columns=columns).set_index('timestamp')
        injection.index = injection.index.map(lambda v: pd.to_datetime(v, unit='s'))

        # consumption F1
        columns = ['consumption_F1', 'timestamp']
        consumption_F1 = pd.DataFrame(json['DailyDET0'], columns=columns).set_index('timestamp')
        consumption_F1.index = consumption_F1.index.map(lambda v: pd.to_datetime(v, unit='s'))
        # consumption F2
        columns = ['consumption_F2', 'timestamp']
        consumption_F2 = pd.DataFrame(json['DailyDET1'], columns=columns).set_index('timestamp')
        consumption_F2.index = consumption_F2.index.map(lambda v: pd.to_datetime(v, unit='s'))
        # consumption F3
        columns = ['consumption_F3', 'timestamp']
        consumption_F3 = pd.DataFrame(json['DailyDET2'], columns=columns).set_index('timestamp')
        consumption_F3.index = consumption_F3.index.map(lambda v: pd.to_datetime(v, unit='s'))

        return pd.concat([production, injection, consumption_F1, consumption_F2, consumption_F3], axis=1)