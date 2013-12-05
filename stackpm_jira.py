'''stackpm_jira.py -- jira connection extension for stackpm

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''
import requests
from datetime import datetime

class Connector(object):
    '''Jira Connector Ojbect'''
    ### MAGIC METHODS
    def __init__(self, config=None):
        self.config = config or {}
        self.__field_cache = None
        self.__iteration_map_cache = None

        # map of the form:
        #   JIRA name => models.Iteration name
        self._iteration_map = {
            'key': 'ext_id',
            'Summary': 'name',
            'Created': 'created_on',
            'Updated': 'updated_on',
            'Assignee.name': 'user_id',
            'Project.key': 'project',
            self.config['effort_estimate_field']: 'effort_est',
            self.config['value_estimate_field']: 'value_est',
        }

    def __repr__(self):
        return '<JiraLink to {}>'.format(self.config.get('url'))

    ### INTERNAL METHODS
    def __auth(self):
        '''Produce a tuple username, password'''
        return ( self.config['username'], self.config['password'], )

    def __url(self, method):
        '''Produce a full url for a REST method'''
        return 'https://{}/rest/api/2/{}'.format(self.config['url'], method)

    def __jql(self, jql, res_filters, since=None):
        '''Combine base jql, resolution filters, and updated since'''
        if res_filters:
            res_filters = '(resolution is empty or resolution not in'\
                          ' ( {} ))'.format(",".join([
                '"{}"'.format(res) for res in res_filters
            ]))
            jql = (' AND ' if jql else '').join([ jql, res_filters, ])
        if since:
            since_filter = 'Updated >= "{}"'.format(
                               since.strftime(self.config['jql_time_fmt']))
            jql = (' AND ' if jql else '').join([ jql, since_filter, ])

        return jql

    def __field_key(self, name):
        '''Take a human recognizable field name and return the internal key'''
        if self.__field_cache is None:
            cache = {}
            for field in self.get('field').json():
                cache[field['name']] = field['id']
            self.__field_cache = cache

        return self.__field_cache.get(name, name)

    def __fmt_item(self, item, field_map):
        '''Traverse potentially nested JIRA keys and return a dictionary with
           stackpm recognizable names'''
        stackpm_item = {}
        for jira_key,stack_key in field_map.iteritems():
            val = item.get(jira_key)
            if val is None:
                val = item.get('fields', {})[jira_key]
            while isinstance(stack_key, tuple):
                jira_key,stack_key = stack_key
                if val is not None:
                    new_val = val.get(jira_key)
                    if new_val is None:
                        new_val = val.get('fields', {})[jira_key]
                    val = new_val
            #TODO: get more intelligent about this
            if isinstance(val, basestring):
                try:
                    val = datetime.strptime(val, self.config['time_fmt'])
                except ValueError:
                    pass
            #END TODO
            stackpm_item[stack_key] = val
        return stackpm_item

    def __full_search(self, jql, fields, field_map, expands="", limit=None):
        '''Generator to perform a full search to limit, regardless of Jira
           pagination limits'''
        total, seen = limit or -1, 0
        params = { 'jql': jql, 'maxResults': 200, }
        if fields:
            params['fields'] = ','.join(fields)
        if expands:
            params['expands'] = expands

        while 0 > total or total > seen:
            params['startAt'] = seen
            res = self.get('search', params=params).json()
            for issue in res['issues']:
                yield self.__fmt_item(issue, field_map)
                seen += 1
                if limit and seen >= limit:
                    break
            if limit:
                total = min(res['total'], limit)
            else:
                total = res['total']

    ### EXPOSED API
    @property
    def iteration_map(self):
        '''Proper jira => models map cache for models.Iteration'''
        if self.__iteration_map_cache is None:
            cache = {}
            for k,v in self._iteration_map.iteritems():
                key_layers= k.split('.')
                # provide depth by nesting tuples
                for sub_key in key_layers[:0:-1]:
                    v = (self.__field_key(sub_key), v)
                cache[self.__field_key(key_layers[0])] = v
            self.__iteration_map_cache = cache

        return self.__iteration_map_cache

    def iterations(self, since=None, limit=None):
        '''Return a list of iteration dicts, capable of being sent to
           models.Iteration'''
        fields = self.iteration_map.keys()
        jql = self.__jql(self.config.get('epic_jql', ''),
                         self.config.get('discard_resolutions', []),
                         since=since)
        return self.__full_search(jql, [ f.split('.')[0] for f in fields ],
                                  self.iteration_map, limit=limit)

    def tasks(self):
        '''Return a list of iteration dicts, capable of being sent to
           models.Iteration'''
        return self.__full_search(self.__jql(self.config.get('work_jql', ''),
                                  self.config.get('discard_resolutions', [])))

    def get(self, method, **kwargs):
        '''REST get method with auth and method builder helpers'''
        return requests.get(self.__url(method), auth=self.__auth(), **kwargs)
