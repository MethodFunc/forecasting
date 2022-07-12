import xml.etree.ElementTree as ET
from collections import OrderedDict

import pandas as pd
import requests


class Asos:
    def __init__(self, start, end, args):
        self.start = start
        self.end = end
        self.args = args
        self.response = None
        self.data_dict = OrderedDict()

        self.response_()

    def call(self):
        if self.args.data_type == 'JSON':
            df = self.json_parser()
        elif self.args.data_type == 'XML':
            df = self.xml_parser()

        return df

    def response_(self):
        url = r'http://apis.data.go.kr/1360000/AsosHourlyInfoService/' \
              r'getWthrDataList'

        if '-' in self.start:
            start = self.start.replace('-', '')
        if '-' in self.end:
            end = self.end.replace('-', '')
            if self.args.one_day:
                start = end

        params = {
            'serviceKey': self.args.asos_key,
            'pageNo': 1,
            'numOfRows': 72,
            'dataType': self.args.data_type,
            'dataCd': 'ASOS',
            'dateCd': 'HR',
            'startDt': end,
            'endDt': start,
            'startHh': '00',
            'endHh': '23',
            'stnIds': str(self.args.sticks)

        }

        self.response = requests.get(url, params=params).content

    def json_parser(self):
        self.create_dict()
        load_json = json.loads(self.response)
        data_json = load_json['response']['body']['items']['item']

        for post in data_json:
            self.data_dict['DT'].append(post['tm'])
            self.data_dict['TEMP'].append(post['ta'])
            self.data_dict['PA'].append(post['pa'])
            self.data_dict['WS'].append(post['ws'])
            self.data_dict['WD'].append(post['wd'])

        dataframe = pd.DataFrame(self.data_dict)

        return dataframe

    def xml_parser(self):
        self.create_dict()
        load_xml = ET.fromstring(self.response)
        for post in load_xml.iter():
            try:
                self.data_dict['DT'].append(post.find('tm').text)
                self.data_dict['TEMP'].append(post.find('ta').text)
                self.data_dict['PA'].append(post.find('pa').text)
                self.data_dict['WS'].append(post.find('ws').text)
                self.data_dict['WD'].append(post.find('wd').text)
            except AttributeError:
                continue
        df = pd.DataFrame(self.data_dict)
        return df

    def create_dict(self):
        self.data_dict['DT'] = list()
        self.data_dict['TEMP'] = list()
        self.data_dict['PA'] = list()
        self.data_dict['WS'] = list()
        self.data_dict['WD'] = list()
