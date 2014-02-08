'''stackpm_jira.py -- jira connection extension for stackpm

   classes: Connector
   @author: Matthew Story <matt.story@axial.net>
'''
import requests
from datetime import datetime

### INTERNAL METHODS AND ATTRIBUTES

# default sentinel
_null = object()

def _make_map(*maps):
    '''Overlay a series of maps without modifying the originals'''
    made_map = {}
    for map_ in maps:
        made_map.update(map_)
    return made_map

def _strip_val(val):
    '''Jira is inconsistent in regards to internal custom ids versus names,
       strip the '.value' suffix from any fields as needed.'''
    return val[:-6] if val.endswith('.value') else val

### EXPOSED CLASSES
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


        # inverse maps for fields affecting events
        self.__effort_est_field = self.config['effort_estimate_field']
        self.__iteration_ext_id_field = self.config['iteration_link_field']

        # maps of the form:
        #   JIRA name => models.Iteration name
        self.__base_map = {
            'ext_id': 'key',
            'name': 'Summary',
            'created_on': 'Created',
            'updated_on': 'Updated',
            'effort_est': self.config['effort_estimate_field']
        }
        self.__iteration_map_raw = _make_map(self.__base_map, {
            'project': 'Project.key',
            'value_est': self.config['value_estimate_field'],
        })
        self.__task_map_raw = _make_map(self.__base_map, {
            'changelog': 'changelog.histories',
            'user_pm_name': 'Assignee.name',
            'user_email': 'Assignee.emailAddress',
            'iteration_ext_id': self.config['iteration_link_field'],
            'started_on': self.config['started_override_field'],
            'dev_done_on': self.config['dev_done_override_field'],
            'prod_done_on': self.config['prod_done_override_field']
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

    def __jql(self, jql, discard_res=_null, since=None):
        '''Combine base jql, resolution filters, and updated since'''
        if discard_res is _null:
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
        for stack_key,jira_keys in field_map.iteritems():
            cur_scope = item
            for jira_key in jira_keys:
                val = cur_scope.get(jira_key)
                if val is None:
                    val = cur_scope.get('fields', {})[jira_key]
                cur_scope = val

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

    def __fmt_task(self, task, since=None):
        '''Additional formatting for tasks. Assumes __fmt_item has been called
           on task already.'''
        # add depth for user
        task['user'] = {}
        for key in ('pm_name', 'email'):
            task['user'][key] = task.pop('user_{}'.format(key), None)

        # there is a simplifying assumption here that changes are time-ordered
        current_iteration = task.get('iteration_ext_id')
        events = []

        iter_field = _strip_val(self.__task_map_raw['iteration_ext_id'])
        est_field = _strip_val(self.__task_map_raw['effort_est'])

        for change in task.get('changelog', {}):
            for item in change.get('items', []):
                # KLUDGE: same microtime/tzinfo hack here
                occured = datetime.strptime(change['created'][:-9],
                                            self.config['time_fmt'])

                if item['field'].strip() == 'status':
                    field_name = self.status_map.get(item['toString'])
                    if field_name and (task[field_name] is None or \
                                       field_name == 'prod_done_on'):
                        task[field_name] = occured
                # else, if the event is recent enough, append it
                elif since is None or occured > since:
                    if item['field'].strip() == iter_field:
                        current_iteration = item['toString']
                        for ext_id in (item['fromString'], item['toString']):
                            events.append({
                                'type': 'iteration-change',
                                'iteration_ext_id': ext_id,
                                'occured_on': occured
                            })
                    elif item['field'].strip() == est_field:
                        events.append({
                            'type': 'estimate-change',
                            'iteration_ext_id': current_iteration,
                            'occured_on': occured,
                            'effort_est_from': item['fromString'] or None,
                            'effort_est_to': item['toString'] or None
                        })

        del task['changelog']
        task['events'] = events
        return task

    def __full_search(self, jql, field_map, expand=None, limit=None):
        '''Generator to perform a full search to limit, regardless of Jira
           pagination limits'''
        total, seen = limit or -1, 0
        params = { 'jql': jql, 'maxResults': 200, }
        fields = ','.join([field[0] for field in field_map.values()])
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
        for stack_key,jira_key in our_map.iteritems():
            made_map[stack_key] = [
                self.__field_key(k) for k in jira_key.split('.')
            ]
        return made_map

    @property
    def __iteration_map(self):
        '''Proper jira => models map cache for models.Iteration'''
        if self.__iteration_map_cache is None:
            self.__iteration_map_cache = \
                self.__jira_map(self.__iteration_map_raw)
        return self.__iteration_map_cache

    @property
    def __task_map(self):
        '''Proper jira => models map cache for models.Task'''
        if self.__task_map_cache is None:
            self.__task_map_cache = self.__jira_map(self.__task_map_raw)
        return self.__task_map_cache

    ### EXPOSED API
    def iterations(self, since=None, limit=None):
        '''Return a list of iteration dicts, capable of being sent to
           models.Iteration'''
        return self.__full_search(self.__jql(self.config.get('epic_jql', ''),
                                             since=since),
                                  self.__iteration_map, limit=limit)
    def iteration(self, ext_id):
        '''Return an iteration dict, capable of being sent to
           models.Iteration'''
        return self.__fmt_item(self.get('issue/{}'.format(ext_id)),
                               self.__iteration_map)

    def tasks(self, since=None, limit=None):
        '''Return a list of task dicts, capable of being sent to
           models.Task'''
        for task in self.__full_search(
                self.__jql(self.config.get('work_jql', ''), since=since),
                self.__task_map, expand='changelog', limit=limit):
            yield self.__fmt_task(task, since=since)

    def task(self, ext_id, since=None):
        '''Return an task dict, capable of being sent to models.Task'''
        return self.__fmt_task(self.__fmt_item(
                self.get('issue/{}'.format(ext_id),
                         params={'expand': 'changelog'}), self.__task_map),
                since=since)


    def get(self, method, **kwargs):
        '''REST get method with auth and method builder helpers'''
        resp = requests.get(self.__url(method), auth=self.__auth(), **kwargs)
        if resp.status_code != 200:
            raise JiraLinkError(resp.status_code,
                                resp.json()['errorMessages'])
        return resp.json()

__all__ = ['Connector', 'JiraLinkError']
