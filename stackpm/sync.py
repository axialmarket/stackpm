'''stackpm/sync.py -- Sync local database with links

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### TODO: Need to be able to remove items that were resolved with
###       "invalid resolution statuses", or that were deleted on the remote

### STANDARD LIBRARY IMPORTS
from datetime import datetime

### INTERNAL IMPORTS
from . import db, null
from .links import project_manager as pm
from .models import Sync, Iteration, User, Task, Event

### GLOBALS
SYNC_BATCH = 100

### INTERNAL METHODS
def _batch_sync(most_recent_update, batch, model, ext_id='ext_id',
                updated_on='updated_on'):
    '''Sync a batch of models with the database, inserting/updating as needed,
       and return the most recent updated_on time'''
    # if no items were sent, bail
    if not batch:
        return most_recent_update

    # setup complex ext_id keys, and force at least 1
    ext_id = (ext_id,) if isinstance(ext_id, basestring) else ext_id
    if not ext_id:
        raise ValueError('Must specify at least 1 ext_id')

    # setup the query for simple or complex keys
    query = model.query
    if len(ext_id) == 1:
        query = query.filter(getattr(model, ext_id[0]).in_(batch.keys()))
    else:
        # (ext_id[0] == batch[key0][0] AND ext_id[1] == batch[key0[1])
        #  OR (ext_id[0] == batch[key1][1] AND ext_id[1] == batch[key1][1])
        # etc ...
        ors = []
        for key in batch.keys():
            ands = []
            for i,part in enumerate(key):
                ands.append(getattr(model, ext_id[i]) == part)
            ors.append(db.and_(*ands))
        query = query.filter(db.or_(*ors))

    # execute
    for obj in query.all():
        key = None
        if len(ext_id) == 1:
            key = getattr(obj, ext_id[0])
        else:
            key = tuple([getattr(obj, i) for i in ext_id])
        for k,v in batch[key].iteritems():
            setattr(obj, k, v)
        batch[key] = obj

    # now save all fetched, modifying batch as we go
    for key,obj in batch.iteritems():
        if not isinstance(obj, model):
            obj = model(**obj)
            db.session.add(obj)
            batch[key] = obj
        obj_updated_on = getattr(obj, updated_on)
        if most_recent_update in (None, null):
            most_recent_update = obj_updated_on
        elif obj_updated_on is not None:
            most_recent_update = max(most_recent_update, obj_updated_on)

    # finalize
    db.session.commit()
    return most_recent_update

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
    task_sync = _batch_sync(since, batch, Task)

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


### EXPOSED METHODS
def sync(since=null):
    '''Sync everything.

       If ``since`` is passed, sinc only objects updated more recently than
       ``since``.'''
    last_sync = []
    for meth in ('sync_iterations','sync_tasks'):
        sync_res = globals()[meth](since=since)
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
                since = _batch_sync(since, batch, Iteration)
                batch = {}

        if len(batch):
            since = _batch_sync(since, batch, Iteration)
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


__all__ = ['SYNC_BATCH', 'sync', 'sync_iterations', 'sync_tasks']
