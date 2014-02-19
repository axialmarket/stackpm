'''stackpm/stats.py -- API for computing stats

   classes: User, Holiday, Vacation, Iteration, Task, Stat, Event,
            Simulation, Sync
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

def _weighted_mean_stddev(vals, weights):
    '''Return weighted mean and stddev of a list of vals

       http://stackoverflow.com/a/2415343'''
    average = numpy.average(vals, weights=weights)
    variance = numpy.average((vals-average)**2, weights=weights)
    return (average, math.sqrt(variance))

def lead_time_stats(user, est, since=null, until=null):
    '''Return an iterable of Stat objects for ``user``/``est`` for every day
       since ``since``. If ``since`` is not passed, return Stat objects for
       all time.'''
    dones = { 'dev_done': [], 'prod_done': [] }
    until = datetime.now() if until is null else until
    halflife = float(config.get('forecast', {}).get('halflife', 45))
    tasks = Task.query.filter(db.and_(Task.user == user,
                                      Task.effort_est == est, db.or_(
                                        Task.dev_done_on, Task.prod_done_on
                                      ))).all()
    # unpack tasks
    for task in tasks:
        if task.dev_done_on and task.dev_done_workdays:
            dones['dev_done'].append((task.dev_done_on,
                                       task.dev_done_workdays))
        if task.prod_done_on and task.prod_done_workdays:
            dones['prod_done'].append((task.prod_done_on,
                                       task.prod_done_workdays))
    # time sort
    lows = []
    for k in dones:
        dones[k].sort(key=lambda x:x[0])
        if len(dones[k]):
            lows.append(dones[k][0][0])

    # short-circuit if we have no evidence
    if not lows:
        return []

    since = min(lows) if since in (null, None) else since

    # iterate all days in range
    stats = []
    for day in xrange((until - since).days):
        stat = {}
        day = since + timedelta(days=day)

        for items,prefix in ((dones['dev_done'], 'dev_done'),
                             (dones['prod_done'], 'prod_done')):
            evidence, weights = [], []
            for dt,workdays in items:
                if dt > day:
                    break
                evidence.append(workdays)
                weights.append(0.5**((day - dt).days/halflife))

            if not evidence:
                continue

            mean, stddev = _weighted_mean_stddev(evidence, weights=weights)
            stderr = stddev / math.sqrt(len(evidence))
            stat.update({
                '{}_sample_size'.format(prefix): len(evidence),
                '{}_mean'.format(prefix): mean,
                '{}_stddev'.format(prefix): stddev,
                '{}_median'.format(prefix): numpy.median(evidence),
                '{}_stderr'.format(prefix): stderr,
                '{}_conf_int'.format(prefix): stderr * 1.96,
            })

        # if we found anything, append it to the stats
        if stat:
            stat['as_of'] = day
            stat['effort_est'] = est
            stats.append(stat)

    return stats
