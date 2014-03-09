'''stackpm/stats.py -- API for computing stats

   functions: make_stats
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### STANDARD LIBRARY IMPORTS
from datetime import datetime, timedelta
import math

### 3RD PARTY IMPORTS
import numpy

### INTERNAL IMPORTS
from . import null, db, config
from .models import Task, Stat

### INTERNAL METHODS
def _default_stat(user, est, as_of):
    '''Return a default stat dict'''
    #TODO: there is a not-so-nice implicit name dependency here
    stat = {'as_of': as_of, 'effort_est': est, 'user': user,
            'failure_rate': None}
    empty = _ewma_stats([], [])
    for kind in ('dev_done', 'prod_done', 'round_trips'):
        stat.update({'_'.join([kind, k]):v for k,v in empty.iteritems()})
    return stat

def _ewma_stats(vals, weights):
    '''Return weighted mean, stddev, stderr, median and mode for correlated
       vals and weights vectors.

       http://stackoverflow.com/a/2415343'''
    weighted_len = sum(weights)
    average = numpy.average(vals, weights=weights or None)
    stddev = math.sqrt(numpy.average((vals-average)**2,
                                     weights=weights or None))
    stderr = None if weighted_len == 0 else stddev / math.sqrt(weighted_len)
    median, mode, median_weight, modes = None, None, 0, {}
    for ind in range(len(vals)):
        val, weight = vals[ind], weights[ind]
        median_weight += weight

        # compute weighted median if needed
        if median is None:
            if median_weight > weighted_len / 2:
                median = val
            elif median_weight == weighted_len / 2:
                try:
                    median = numpy.average([val, vals[ind+1]],
                                           weights=[weight, weights[ind+1]])
                except IndexError:
                    median = val

        # setup for mode
        modes[val] = modes.get(val, 0) + weight

    if modes:
        mode = max(modes.iteritems(), key=lambda (x,y): y)[0]

    stats = {'mean': average, 'stddev': stddev, 'stderr': stderr,
             'median': median, 'mode': mode, 'sample_size': len(vals),
             'conf_int': (numpy.nan if stderr is None else stderr) * 1.96}
    return {k:None if v is None or numpy.isnan(v) else v for k,v in stats.iteritems()}

def _sequence_ewma(items, for_day, halflife):
    '''Take an iterable of tuples of length 2, each tuple a pair a datetime
       and values, a day to measure from, and a halflife (in days) and return
       a tuple of length 2 with correlated lists of dates and weights for
       those dates.'''
    evidence, weights = [], []
    for dt,val in items:
        if dt > for_day:
            break
        evidence.append(val)
        weights.append(0.5**((for_day - dt).days/halflife))
    return (evidence, weights)


### EXPOSED METHODS
def make_stats(user, est, since=null, until=null):
    '''Return an iterable of Stat objects for ``user``/``est`` for every day
       since ``since``. If ``since`` is not passed, return Stat objects for
       all time.'''
    dones = {'dev_done': [], 'prod_done': [], 'round_trips': [],
             'failures': []}
    until = datetime.now() if until is null else until
    halflife = float(config.get('forecast', {}).get('halflife', 30))
    discard_resolutions = config.get('tasks', {}).get('discard_resolutions')
    failure_res = config.get('tasks', {}).get('failure_resolution')
    tasks = Task.query.filter(db.and_(Task.user == user,
                                      Task.effort_est == est, db.or_(
                                        Task.dev_done_on, Task.prod_done_on
                                      )))

    # filter-out resolutions we don't count for stats
    if discard_resolutions:
        tasks = tasks.filter(db.or_(Task.resolution == None,
                                    ~Task.resolution.in_(discard_resolutions)))
    # unpack tasks
    for task in tasks.all():
        if task.dev_done_on and task.dev_done_workdays:
            dones['dev_done'].append((task.dev_done_on,
                                       task.dev_done_workdays))
        if task.prod_done_on and task.prod_done_workdays:
            dones['prod_done'].append((task.prod_done_on,
                                       task.prod_done_workdays))
            dones['round_trips'].append((task.prod_done_on,
                                         task.round_trips or 1))
            dones['failures'].append((task.prod_done_on,
                                      int(bool(task.resolution == failure_res))))
    # time sort
    lows = []
    for k in dones:
        dones[k].sort(key=lambda x:x[0])
        if len(dones[k]):
            lows.append(dones[k][0][0])

    # short-circuit if we have no evidence
    if lows:
        since = min(lows) if since in (null, None) else since

        # iterate all days in range
        for day in xrange((until - since).days + 1):
            day = since + timedelta(days=day)
            stat = _default_stat(user, est, day)

            # compute failure rate
            fails, fail_weights = _sequence_ewma(dones['failures'], day,
                                                 halflife)
            if fails:
                stat['failure_rate'] = numpy.average(fails,
                                                     weights=fail_weights)

            # compute total stats for all other types
            for items,kind in ((dones['dev_done'], 'dev_done'),
                               (dones['prod_done'], 'prod_done'),
                               (dones['round_trips'], 'round_trips')):

                evidence, weights = _sequence_ewma(items, day, halflife)
                if not evidence:
                    continue

                kind_stats = _ewma_stats(evidence, weights)

                # mixin stats
                stat.update({'_'.join([kind, k]):v for k,v in kind_stats.iteritems()})

            yield stat
