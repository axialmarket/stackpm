'''stackpm/sync.py -- Sync local database with links

   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

from datetime import datetime

from . import db
from .links import project_manager as pm
from .models import Sync, Iteration

SYNC_BATCH = 100

def _batch_sync_iterations(most_recent_update, batch, ext_ids):
    # fetch all existing iterations by ext_id
    updates = {}
    for iteration in Iteration.query\
                              .filter(Iteration.ext_id.in_(ext_ids)).all():
        updates[iteration.ext_id] = iteration

    # now save all fetched
    db.session.expunge_all()
    for iteration in batch:
        try:
            iteration.id = updates[iteration.ext_id].id
            db.session.merge(iteration)
        except KeyError:
            db.session.add(iteration)

        if most_recent_update is None:
            most_recent_update = iteration.updated_on
        else:
            most_recent_update = max(most_recent_update, iteration.updated_on)

    # finalize
    db.session.commit()
    return most_recent_update

def sync_iterations(since=None):
    if since is None:
        last_sync = Sync.query.filter_by(type='iteration')\
                              .order_by(Sync.last_seen_update.desc()).first()
        if last_sync:
            since = last_sync.last_seen_update

    try:
        batch,ext_ids = [], []
        for iteration in pm.iterations(since=since):
            batch.append(Iteration(**iteration))
            ext_ids.append(iteration['ext_id'])
            if len(batch) == SYNC_BATCH:
                since = _batch_sync_iterations(since, batch, ext_ids)
                batch, ext_ids = [], []

        if len(batch):
            since = _batch_sync_iterations(since, batch, ext_ids)
    except Exception:
        db.session.rollback()
        raise

    try:
        if since is not None:
            db.session.add(Sync(last_seen_update=since, type="iteration"))

        # commit
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

__all__ = ['SYNC_BATCH', 'sync_iterations']
