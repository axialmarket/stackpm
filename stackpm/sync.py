'''stackpm/sync.py -- Sync local database with links

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### STANDARD LIBRARY IMPORTS
from datetime import datetime, date

### INTERNAL IMPORTS
from . import db, null
from .links import project_manager as pm, calendar as cal
from .stats import make_stats
from .estimates import task_efforts
from .models import Sync, Iteration, User, Task, Event, Holiday, Vacation, Stat

### GLOBALS
SYNC_BATCH = 100

### INTERNAL METHODS
def _complex_key(obj, columns):
    '''Return a hashable key made from the values of columns on obj.'''
    if isinstance(columns, basestring):
        return getattr(obj, columns)
    return tuple([getattr(obj, i) for i in columns])

def _complex_query(values, model, columns):
    '''Return a query clause, suitable for use with filter from a single:

        column IN (*values)

       or multi-column constraint:

        (column[0] == values[key0][0] AND column[1] == values[key0][1])
          OR
        (column[0] == values[key1][1] AND column[1] == values[key1][1])'''
    if isinstance(columns, basestring):
        return getattr(model, columns).in_(values)
    else:
        ors = []
        for key in values:
            ands = []
            for i,part in enumerate(key):
                ands.append(getattr(model, columns[i]) == part)
            ors.append(db.and_(*ands))
        return db.or_(*ors)

def _batch_sync(most_recent_update, batch, model, ident,
                updated_on='updated_on', task_changes=None):
    '''Sync a batch of models with the database, inserting/updating as needed,
       and return a dict of objects that were created, and the most recent
       updated_on time

       NB: batch and task_changes are modified by side-effect, this behavior
           is relied on.
       TODO: Consider changing this.
    '''
    # if no items were sent, bail
    if not batch:
        return {}, most_recent_update

    # setup complex ident keys, and force at least 1
    if not ident:
        raise ValueError('Must specify at least 1 ident')

    # filter on multi-column
    for obj in model.query.filter(_complex_query(batch.keys(), model,
                                                 ident)).all():
        key = _complex_key(obj, ident)
        if task_changes not in (None, null) and model is Task:
            task_changes = _log_task_change(task_changes, obj, batch[key])
        for k,v in batch[key].iteritems():
            # tasks have side-effects for both stats and simulations
            # log date-deltas here if asked to
            setattr(obj, k, v)
        batch[key] = obj

    # now save all fetched, modifying batch as we go
    created = {}
    for key,obj in batch.iteritems():
        if not isinstance(obj, model):
            if task_changes not in (None, null) and model is Task:
                task_changes = _log_task_change(task_changes, None, obj)
            obj = model(**obj)
            db.session.add(obj)
            batch[key] = created[key] = obj

        obj_updated_on = None
        if updated_on is not None:
            obj_updated_on = getattr(obj, updated_on)

        if most_recent_update in (None, null):
            most_recent_update = obj_updated_on
        elif obj_updated_on is not None:
            most_recent_update = max(most_recent_update, obj_updated_on)

    # finalize
    db.session.commit()
    return created, most_recent_update

def _sync_since(type_):
    '''Return the datetime of the last updated timestamp from the last sync
       of ``type_``'''
    last_sync = Sync.query.filter_by(type=type_)\
                          .order_by(Sync.last_seen_update.desc()).first()
    if last_sync:
        return last_sync.last_seen_update

    return None

def _record_sync(type_, last_seen):
    '''Record that a sincy of ``type_`` occured, and that the most recently
       updated record of ``type_`` was updated at ``last_seen``'''
    if not last_seen:
        return None
    try:
        record = Sync(last_seen_update=last_seen, type=type_)
        db.session.add(record)
        db.session.commit()
        return record
    except Exception:
        db.session.rollback()
        raise

def _batch_sync_tasks(since, batch, users, iter_ext_ids, events,
                      task_changes):
    '''Task sync'ing requires sycning users, iterations and events, it's
       enough complexity to warrant a helper function.'''
    # first sync all iterations, then grab the iterations:
    iters = {}
    if len(iter_ext_ids):
        sync_iterations(ids=iter_ext_ids)
        for iter_ in Iteration.query.filter(
                Iteration.ext_id.in_(iter_ext_ids)).all():
            iters[iter_.ext_id] = iter_

    # then sync users -- NB: _batch_sync modifies users
    _batch_sync(None, users, User, 'email')

    # then add iterations and users to tasks
    for k,(task, email, iter_ext_id) in batch.iteritems():
        task['user'] = users[email]
        task['iteration'] = iters.get(iter_ext_id)
        batch[k] = task

    # then sync the tasks themselves -- store this sync, it's the one we want
    _, task_sync = _batch_sync(since, batch, Task, 'ext_id',
                               task_changes=task_changes)

    # forceably reset task workdays cache
    for task in batch.itervalues():
        task.cache_workdays(force=True)

    # and finally the events related to the tasks
    new_events = {}
    for key,(task_ext_id,iter_ext_id,from_iter_ext_id,ev) in events.iteritems():
        iter_, from_iter = iters.get(iter_ext_id), iters.get(from_iter_ext_id)
        ev['iteration'] = iter_
        ev['from_iteration'] = from_iter
        ev['task'] = batch[task_ext_id]
        new_key = (key[0], key[1], ev['task'].id)
        new_events[new_key] = ev

    # sync events
    _batch_sync(None, new_events, Event, ['type', 'occured_on', 'task_id'],
                updated_on='occured_on')

    return task_sync, task_changes

def _batch_sync_vacations(vacations, users, updated_dates, all_):
    '''Batch sync holidays and vacations. Update and return a set of new dates
       and a dict of all seen dates, to determine what should be deleted
       dates'''
    _batch_sync(None, users, User, 'email')
    for date, email in vacations.keys():
        user = users[email]
        key = (date, user.id)
        vacation = vacations.pop((date, email))
        vacation['user_id'] = user.id
        vacation['user'] = user
        vacations[key] = vacation
        all_.add(key)
    created, _ = _batch_sync(None, vacations, Vacation, ['date', 'user_id'],
                                updated_on=None)
    updated_dates |= set(created.keys())
    return updated_dates, all_


def _delete_datish(keep, model, ext_id, date_attr='date'):
    '''Delete old dateish objects.'''
    deletes = set()
    for delete in model.query.filter(db.not_(_complex_query(keep, model,
                                             ext_id))).all():
        db.session.delete(delete)
        deletes.add(_complex_key(delete, ext_id))

    db.session.commit()
    return deletes


def _update_task_net_workdays(*args):
    '''Update a task net_workdays by date/user or just date'''
    query = Task.query
    ors = []
    task_changes = _task_change_log()
    for arg in args:
        date, user_id = arg, None
        if isinstance(arg, tuple):
            date, user_id = arg

        ands = [Task.started_on != None, Task.started_on <= date,
                       db.or_(Task.prod_done_on == None,
                              Task.prod_done_on >= date)]
        if user_id is not None:
            ands.append(Task.user_id == user_id)
        ors.append(db.and_(*ands))

    if ors:
        for task in Task.query.filter(db.or_(*ors)):
            task.cache_workdays(force=True)
            task_changes = _log_task_change(task_changes, task, None)

    db.session.commit()
    return task_changes

def _task_change_log():
    '''Create an empty task change log dictionary'''
    return {'stats': {}, 'iterations': {}}

def _log_task_change(task_log, old, new):
    '''Log dates for user-specifica and iteration-specific changes to a task
       log dictionary'''
    old = old.__dict__ if isinstance(old, Task) else (old or {})
    new = new.__dict__ if isinstance(new, Task) else (new or {})
    changes, users, iterations, effort_ests = [], [], [], []
    for key in set(old.keys())|set(new.keys()):
        for d in (old, new):
            val = d.get(key)
            # we need to re-run stats/sims for both sides of any user change
            if key == 'user':
                users.append(val.id if val is not None else None)
            # we need to re-run stats/sims for both sides of any est change
            elif key == 'effort_est':
                effort_ests.append(val)
            # we need to re-run sims for both sides of any iteration change
            elif key == 'iteration':
                iterations.append(val.id if val is not None else None)
            # setup a minable list
            elif isinstance(val, date):
                changes.append(val)

    change_since = min(changes) if changes else None
    if change_since:
        for user in users:
            user_log = task_log['stats'].setdefault(user, {})
            for est in effort_ests:
                user_log[est] = min(change_since,
                                    user_log.get(est, change_since))
        for iter_ in iterations:
            iters = task_log['iterations']
            iters[iter_] = min(change_since, iters.get(iter_, change_since))

    return task_log

def _update_stats_and_sims(task_log):
    '''Update stats and simulations based on logged task changes'''
    # map users we need to lookup in DB
    user_lookup, stats = {}, task_log['stats']
    if stats:
        for u in User.query.filter(User.id.in_([
                u for u in stats.keys() if u is not None])).all():
            user_lookup[u.id] = u

    # update stats for users
    for user_id,tasks in stats.iteritems():
        for effort_est,since in tasks.iteritems():
            users = [user_lookup[user_id]] if user_id else null
            if users is not null:
                sync_stats(since=since, users=users, efforts=[effort_est],
                           record=False)

    # TODO: update sims by iteration part of map

### EXPOSED METHODS
def sync():
    '''Sync everything.

       If ``since`` is passed, sinc only objects updated more recently than
       ``since``.'''
    last_sync = []
    for meth in ('sync_holidays', 'sync_vacations', 'sync_iterations',
                 'sync_tasks', 'sync_stats'):
        sync_res = globals()[meth]()
        if sync_res:
            last_sync.append(sync_res.last_seen_update)

    since = max([dt for dt in last_sync]) if last_sync else None
    return _record_sync('full', since)

def sync_iterations(since=null, ids=null, record=null):
    '''Sync iterations from remote project_manager link to the local database.

       If ``since`` is passed, sync only iterations updated more recently
       than ``since``, else only sync iterations updated more recently than
       the last iteration sync.'''
    since = _sync_since('iteration') if since is null else since
    record = record if record is not null else (ids is null)
    try:
        batch = {}
        for iteration in pm.iterations(since=since, ids=ids):
            batch[iteration['ext_id']] = iteration
            if len(batch) == SYNC_BATCH:
                _, since = _batch_sync(since, batch, Iteration, 'ext_id')
                batch = {}

        if len(batch):
            _, since = _batch_sync(since, batch, Iteration, 'ext_id')
    except Exception:
        db.session.rollback()
        raise
    if record:
        return _record_sync('iteration', since)
    return None

def sync_tasks(since=null, ids=null, record=null):
    '''Sync tasks from remote project_manager link to the local database.

       If ``since`` is passed, sync only tasks updated more recently than
       ``since``, else only sync tasks updated more recently than the last
       task sync.'''
    #TODO: networkdays
    since = _sync_since('task') if since is null else since
    record = record if record is not null else (ids is null)
    task_changes = _task_change_log()
    try:
        batch, users, events, iter_ext_ids = {}, {}, {}, set()
        for task in pm.tasks(since=since, ids=ids):
            # setup events
            for ev in task.pop('events', []):
                event_iter_ext_id = ev.pop('iteration_ext_id', None)
                event_from_iter_ext_id = ev.pop('from_iteration_ext_id', None)
                for i in (event_iter_ext_id, event_from_iter_ext_id):
                    if i is not None:
                        iter_ext_ids.add(i)
                key = (
                    ev['type'], ev['occured_on'], task['ext_id']
                )
                events[key] = (task['ext_id'], event_iter_ext_id,
                               event_from_iter_ext_id, ev)

            # setup iterations
            iter_ext_id = task.pop('iteration_ext_id')
            if iter_ext_id is not None:
                iter_ext_ids.add(iter_ext_id)

            # setup users
            email = task['user']['email'].strip()
            users.setdefault(email, task.pop('user'))

            # NB: we have not associated the iteration yet
            batch[task['ext_id']] = (task, email, iter_ext_id)
            if len(batch) == SYNC_BATCH:
                since, task_changes = _batch_sync_tasks(since, batch, users,
                                                        iter_ext_ids, events,
                                                        task_changes)
                batch, users, events, iter_ext_ids = {}, {}, {}, set()

        if len(batch):
            since, task_changes = _batch_sync_tasks(since, batch, users,
                                                    iter_ext_ids, events,
                                                    task_changes)
        _update_stats_and_sims(task_changes)
    except Exception:
        db.session.rollback()
        raise

    if record:
        return _record_sync('task', since)
    return None

def sync_holidays(year=None, record=True):
    '''Sync holidays in ``year`` from remote calendar link to the local
       database. If ``year`` is None, sync holidays from all years.

       sync_holidays supports no ``since`` argument, as updates are not
       generally available from calendars, the entire calendar is sync'ed with
       the database every time.'''
    holidays, updated_dates, all_ = {}, set(), set()
    try:
        for holiday in cal.holidays(year=year):
            key = holiday['date']
            holidays[key] = holiday
            all_.add(key)
            if len(holidays) == SYNC_BATCH:
                created, _ = _batch_sync(None, holidays, Holiday, 'date',
                                            updated_on=None)
                updated_dates |= set(created.keys())
                holidays = {}

        if len(holidays):
            created, _ = _batch_sync(None, holidays, Holiday, 'date',
                                        updated_on=None)
            updated_dates |= set(created.keys())
            holidays = {}

        # delete old holidays
        updated_dates |= _delete_datish(all_, Holiday, 'date')
        # update tasks
        _update_stats_and_sims(_update_task_net_workdays(*updated_dates))
    except Exception:
        db.session.rollback()
        raise

    if record:
        return _record_sync('holiday', datetime.now())
    return None

def sync_vacations(email=None, record=True):
    '''Sync vacations for user associated with ``email`` from remote calendar
       link to the local database. If ``email`` is None, sync vacations for
       all users.

       sync_vacations supports no ``since`` argument, as updates are not
       generally available from calendars, the entire calendar is sync'ed with
       the database every time.'''

    vacations, users, updated_dates, all_ = {}, {}, set(), set()
    try:
        for vacation in cal.vacations(email=email):
            user = vacation.pop('user')
            users[user['email']] = user
            key = (vacation['date'], user['email'])
            vacations[key] = vacation

            if len(vacations) == SYNC_BATCH:
                updated_dates, all_ = _batch_sync_vacations(vacations, users,
                                                            updated_dates, all_)
                vacations, users = {}, {}

        if len(vacations):
            updated_dates, all_ = _batch_sync_vacations(vacations, users,
                                                        updated_dates, all_)

        # delete old vacations
        updated_dates |= _delete_datish(all_, Vacation, ['date','user_id'])
        # update tasks
        _update_stats_and_sims(_update_task_net_workdays(*updated_dates))
    except Exception:
        db.session.rollback()
        raise

    # update tasks
    if record:
        return _record_sync('vacation', datetime.now())
    return None

def sync_stats(since=null, users=null, efforts=null, record=True):
    '''Sync stats for user (``users``) and effort (``efforts``) combinations,
       day-over-day since ``since``.'''
    efforts = task_efforts(user=users) if efforts is null else efforts
    users = User.query.all() if users is null else users
    since = _sync_since('task') if since is null else since
    try:
        stats = {}
        for user in users:
            for effort in efforts:
                for stat in make_stats(user, effort, since=since):
                    stat['user_id'] = stat['user'].id
                    key = (stat['user_id'], stat['effort_est'], stat['as_of'])
                    stats[key] = stat

                    if len(stats) == SYNC_BATCH:
                        _, sync = _batch_sync(since, stats, Stat,
                                              ['user_id', 'effort_est', 'as_of'],
                                              updated_on=None)
                        stats = {}
        if len(stats):
            _, sync = _batch_sync(since, stats, Stat,
                                  ['user_id', 'effort_est', 'as_of'],
                                  updated_on=None)
            stats = {}
    except Exception:
        db.session.rollback()
        raise

    if record:
        return _record_sync('vacation', since)
    return None

__all__ = ['SYNC_BATCH', 'sync', 'sync_iterations', 'sync_tasks',
           'sync_holidays', 'sync_vacations', 'sync_stats']
