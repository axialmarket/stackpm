'''stackpm_config_link.py -- link to read setup info from a config file.

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''
### STANDARD LIBRARY IMPORTS
import heapq
from datetime import datetime

class Connector(object):
    def __init__(self, config=None):
        '''Initialize the config_link connector. config_link provides an
           extremely simple way to load holidays and vacations (and eventually
           tasks, iterations and users) from a betterconfig config file.'''
        self.config = config or {}
        self.__holidays = config.get('holidays', {})
        self.__vacations = {}
        for key,vacation in config.iteritems():
            if '@' in key:
                self.__vacations[key] = vacation

    def __repr__(self):
        return '<JiraLink to {}>'.format(self.config.get('url'))

    def __vacaterator(self, map_, filter_key):
        '''Given a vacation or holiday map and a key to filter on (year or
           email), generate items (tuple of len 2) with the a key (year or
           email) and a date string.'''
        iter_map = map_.iteritems()
        if filter_key is not None:
            iter_map = [(filter_key, map_.get(filter_key, {}))]
        for key,iter_ in iter_map:
            if isinstance(iter_, dict):
                iter_ = iter_.itervalues()
            for val in iter_:
                yield (key, val)

    def holidays(self, year=None):
        '''return a list of holidays in descending order for ``year``. if
           ``year`` is none, holidays will return holidays from all years'''
        holidays = []
        year = year if year is None else str(year)
        fmt = ''.join(['%Y', self.config.get('holiday_fmt', '%d/%m')])
        for year,day in self.__vacaterator(self.__holidays, year):
            holidays.append({
                'date': datetime.strptime(''.join([year, day]), fmt)})

        holidays.sort(key=lambda x: x['date'], reverse=True)
        return holidays

    def vacations(self, email=None):
        '''Return a list of vacations in descending order for ``email``.
           If ``email`` is None, return vacations for all emails.'''
        vacations = []
        fmt = self.config.get('vacation_fmt', '%d/%m/%Y')
        for email,day in self.__vacaterator(self.__vacations, email):
            vacations.append({ 'user': { 'email': email },
                               'date': datetime.strptime(day, fmt)})

        vacations.sort(key=lambda x: x['date'], reverse=True)
        return vacations

    def get(self, meth, *args, **kwargs):
        '''Provide access to the raw vacations and holidays'''
        meths = {'vacations': self.__vacations,
                 'holidays': self.__holidays}
        got = meths.get(meth)
        for arg in args:
            if got is None:
                break
            got = got.get(arg)
        return got
