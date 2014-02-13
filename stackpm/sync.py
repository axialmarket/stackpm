'''stackpm/sync.py -- Sync local database with links

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### TODO: Need to be able to remove items that were resolved with
###       "invalid resolution statuses", or that were deleted on the remote

### STANDARD LIBRARY IMPORTS
from datetime import datetime

### INTERNAL IMPORTS
from . import db, null
from .links import project_manager as pm, calendar as cal
from .models import Sync, Iteration, User, Task, Event, Holiday, Vacation

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

def _batch_sync(most_recent_update, batch, model, ext_id='ext_id',
                updated_on='updated_on'):
    '''Sync a batch of models with the database, inserting/updating as needed,
       and return a dict of objects that were created, and the most recent
       updated_on time'''
    # if no items were sent, bail
    if not batch:
        return {}, most_recent_update

    # setup complex ext_id keys, and force at least 1
    if not ext_id:
        raise ValueError('Must specify at least 1 ext_id')

    # filter on multi-column
    for obj in model.query.filter(_complex_query(batch.keys(), model,
                                                 ext_id)).all():
        key = _complex_key(obj, ext_id)
        for k,v in batch[key].iteritems():
            setattr(obj, k, v)
        batch[key] = obj

    # now save all fetched, modifying batch as we go
    created = {}
    for key,obj in batch.iteritems():
        if not isinstance(obj, model):
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

def _batch_sync_tasks(since, batch, users, iter_ext_ids, events):
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
    _batch_sync(None, users, User, ext_id='email')

    # then add iterations and users to tasks
    for k,(task, email, iter_ext_id) in batch.iteritems():
        task['user'] = users[email]
        task['iteration'] = iters.get(iter_ext_id)
        batch[k] = task

    # then sync the tasks themselves -- store this sync, it's the one we want
    _, task_sync = _batch_sync(since, batch, Task)

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
    _batch_sync(None, new_events, Event, updated_on='occured_on',
                ext_id=['type', 'occured_on', 'task_id'])

    return task_sync

def _batch_sync_vacations(vacations, users, updated_dates, all_):
    '''Batch sync holidays and vacations. Update and return a set of new dates
       and a dict of all seen dates, to determine what should be deleted
       dates'''
    _batch_sync(None, users, User, ext_id='email')
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

    for task in Task.query.filter(db.or_(*ors)):
        task.cache_workdays(force=True)

    db.session.commit()


### EXPOSED METHODS
def sync():
    '''Sync everything.

       If ``since`` is passed, sinc only objects updated more recently than
       ``since``.'''
    last_sync = []
    for meth in ('sync_holidays', 'sync_vacations', 'sync_iterations',
                 'sync_tasks'):
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
                _, since = _batch_sync(since, batch, Iteration)
                batch = {}

        if len(batch):
            _, since = _batch_sync(since, batch, Iteration)
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
                since = _batch_sync_tasks(since, batch, users, iter_ext_ids,
                                          events)
                batch, users, events, iter_ext_ids = {}, {}, {}, set()

        if len(batch):
            since = _batch_sync_tasks(since, batch, users, iter_ext_ids,
                                      events)
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
        _update_task_net_workdays(*updated_dates)
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
        _update_task_net_workdays(*updated_dates)
    except Exception:
        db.session.rollback()
        raise

    # update tasks
    if record:
        return _record_sync('vacation', datetime.now())
    return None

__all__ = ['SYNC_BATCH', 'sync', 'sync_iterations', 'sync_tasks',
           'sync_holidays', 'sync_vacations']
