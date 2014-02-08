'''stackpm_jira.py -- jira connection extension for stackpm

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''
import requests
from datetime import datetime

# default sentinel
null = object()

def _make_map(*maps):
    '''Overlay a series of maps without modifying the originals'''
    made_map = {}
    for map_ in maps:
        made_map.update(map_)
    return made_map

class JiraLinkError(Exception):
    '''An exception for errors returned by Jira'''
    def __init__(self, code, messages):
        self.code = code
        return super(Exception, self).__init__('\n'.join(messages))

class Connector(object):
    '''Jira Connector Ojbect'''
    ### MAGIC METHODS
    def __init__(self, config=None):
        self.config = config or {}

        # initialize status map
        self.status_map = {
            self.config.get('started_status'): 'started_on',
            self.config.get('dev_done_status'): 'dev_done_on',
            self.config.get('prod_done_status'): 'prod_done_on'
        }

        # initialize @property-accessed caches
        self.__field_cache = None
        self.__iteration_map_cache = None
        self.__task_map_cache = None

        # maps of the form:
        #   JIRA name => models.Iteration name
        self.__base_map = {
            'key': 'ext_id',
            'Summary': 'name',
            'Created': 'created_on',
            'Updated': 'updated_on',
            'Assignee.name': 'user_id',
            self.config['effort_estimate_field']: 'effort_est',
        }
        self.__iteration_map = _make_map(self.__base_map, {
            'Project.key': 'project',
            self.config['value_estimate_field']: 'value_est',
        })
        self.__task_map = _make_map(self.__base_map, {
            'changelog.histories': 'changelog',
            self.config['started_override_field']: 'started_on',
            self.config['dev_done_override_field']: 'dev_done_on',
            self.config['prod_done_override_field']: 'prod_done_on',
        })

    def __repr__(self):
        return '<JiraLink to {}>'.format(self.config.get('url'))

    ### INTERNAL METHODS
    def __auth(self):
        '''Produce a tuple username, password'''
        return ( self.config['username'], self.config['password'], )

    def __url(self, method):
        '''Produce a full url for a REST method'''
        return 'https://{}/rest/api/2/{}'.format(self.config['url'],
                                                 method.lstrip('/'))

    def __quote(self, val):
        return '"{}"'.format(val.replace('"', r'\"'))

    def __jql(self, jql, discard_res=null, since=None):
        '''Combine base jql, resolution filters, and updated since'''
        if discard_res is null:
            discard_res = self.config.get('discard_resolutions')

        if discard_res:
            res_jql = '(resolution is empty or resolution not in'\
                      ' ({}))'.format(",".join([
                          self.__quote(res) for res in discard_res
                      ]))
            jql = (' AND ' if jql else '').join([ jql, res_jql, ])

        if since:
            since_filter = 'Updated >= "{}"'.format(
                               since.strftime(self.config['jql_time_fmt']))
            jql = (' AND ' if jql else '').join([ jql, since_filter, ])

        return jql

    def __field_key(self, name):
        '''Take a human recognizable field name and return the internal key'''
        if self.__field_cache is None:
            cache = {}
            for field in self.get('field'):
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
                    #KLUDGE: Jira appends microtime and tzinfo, neither of
                    # which python is capable of handling, so rtrim 9
                    val = datetime.strptime(val[:-9], self.config['time_fmt'])
                except ValueError:
                    pass
            #END TODO
            stackpm_item[stack_key] = val
        return stackpm_item

    def __full_search(self, jql, field_map, expand=None, limit=None):
        '''Generator to perform a full search to limit, regardless of Jira
           pagination limits'''
        total, seen = limit or -1, 0
        params = { 'jql': jql, 'maxResults': 200, }
        fields = ','.join([field.split('.')[0] for field in field_map.keys()])
        expand = [expand] if isinstance(expand, basestring) else expand
        if expand:
            params['expand'] = ",".join(expand)

        while 0 > total or total > seen:
            params['startAt'] = seen
            res = self.get('search', params=params)
            for issue in res['issues']:
                yield self.__fmt_item(issue, field_map)
                seen += 1
                if limit and seen >= limit:
                    break
            if limit:
                total = min(res['total'], limit)
            else:
                total = res['total']

    def __jira_map(self, our_map):
        '''Create a map suitable for caching from our field names to jira's'''
        made_map = {}
        for k,v in our_map.iteritems():
            key_layers = k.split('.')
            # provide depth by nesting tuples
            for sub_key in key_layers[:0:-1]:
                v = (self.__field_key(sub_key), v)
            made_map[self.__field_key(key_layers[0])] = v

        return made_map

    ### EXPOSED API
    @property
    def iteration_map(self):
        '''Proper jira => models map cache for models.Iteration'''
        if self.__iteration_map_cache is None:
            self.__iteration_map_cache = self.__jira_map(self.__iteration_map)
        return self.__iteration_map_cache

    @property
    def task_map(self):
        '''Proper jira => models map cache for models.Task'''
        if self.__task_map_cache is None:
            self.__task_map_cache = self.__jira_map(self.__task_map)
        return self.__task_map_cache

    def iterations(self, since=None, limit=None):
        '''Return a list of iteration dicts, capable of being sent to
           models.Iteration'''
        return self.__full_search(self.__jql(self.config.get('epic_jql', ''),
                                             since=since),
                                  self.iteration_map, limit=limit)

    def tasks(self, since=None, limit=None):
        '''Return a list of task dicts, capable of being sent to
           models.Iteration'''
        for task in self.__full_search(
                self.__jql(self.config.get('work_jql', ''), since=since),
                self.task_map, expand='changelog', limit=limit):
            # mixin transition dates
            transitions = {
                'started_on': task.get('started_on'),
                'dev_done_on': task.get('dev_done_on'),
                'prod_done_on': task.get('prod_done_on'),
                'added_to_iteration_on': None
            }
            for change in task.get('changelog', {}):
                for item in change.get('items', []):
                    if item['field'].strip() == 'status':
                        field_name = self.status_map.get(item['toString'])
                        if field_name and task[field_name] is None:
                            task[field_name] = datetime.strptime(
                                change['created'][:-9],
                                self.config['time_fmt'])
                    elif item['field'].strip() == 'Epic Link':
                        #TODO: implement
                        pass

            del task['changelog']
            yield task


    def get(self, method, **kwargs):
        '''REST get method with auth and method builder helpers'''
        resp = requests.get(self.__url(method), auth=self.__auth(), **kwargs)
        if resp.status_code != 200:
            raise JiraLinkError(resp.status_code, resp.json()['errorMessages'])
        return resp.json()

__all__ = ['Connector']
