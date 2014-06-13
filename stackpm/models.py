'''stackpm/models.py -- Database API for stackpm

   classes: User, Holiday, Vacation, Iteration, Task, Stat, Event,
            Simulation, Sync
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### STANDARD LIBRARY IMPORTS
import copy
from datetime import datetime

### 3RD PARTY IMPORTS
from workdays import networkdays

### INTERNAL IMPORTS
from . import db
from .fields import JSONField

def _copy_as_of(obj, dels=tuple(), relateds=tuple()):
    '''Return a dict copy of `obj`, removing `dels` and making sure `relateds`
       objects are fully copied'''
    cp = copy.deepcopy(obj.__dict__)
    for del_ in tuple(dels) + tuple(['_sa_instance_state']):
        del cp[del_]
    for related in relateds:
        related_obj = getattr(obj, related)
        cp['_'.join([related, 'id'])] = related_obj.id
        cp[related] = related_obj

    return cp

### EXPOSED CLASSES
class User(db.Model):
    '''Model to map local usage to 3rd party tool like Jira

       NB: JIRA does not seem to require unique emails ... but we're not as
           stupid as they are ... you will break stackpm if you are silly
           enough to duplicate emails in JIRA.'''
    id = db.Column(db.Integer, primary_key=True)
    created_on = db.Column(db.DateTime, nullable=False,
                           server_default=db.func.now())
    updated_on = db.Column(db.DateTime, nullable=False,
                           server_default=db.func.now(),
                           onupdate=db.func.current_timestamp())
    email = db.Column(db.String(255), unique=True, nullable=False)

    pm_name = db.Column(db.String(255), unique=True, nullable=True)
    cal_name = db.Column(db.String(255), unique=True, nullable=True)

    def __init__(self, email, pm_name=None):
        self.email = email
        self.pm_name = pm_name

    def __repr__(self):
        return '<User {}>'.format(self.email)

class Holiday(db.Model):
    '''Model to store vacations'''
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False, unique=True)

    def __repr__(self):
        return '<Holiday {}>'.format(self.date)

class Vacation(db.Model):
    '''Model to store vacations'''
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    user = db.relationship('User', backref=db.backref('vacation',
                                        order_by=db.desc('Vacation.date')))
    db.Index('user_id_date', user_id, date, unique=True)

    def __repr__(self):
        return '<Vacation {} for {}>'.format(self.date, self.user)

class Iteration(db.Model):
    '''Model for iterations, which will typically be 'Epics' in the upstream
       PM software (e.g. Jira)

       TODO: How to track negative changes over time?'''
    id = db.Column(db.Integer, primary_key=True)
    ext_id = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    created_on = db.Column(db.DateTime, nullable=False)
    updated_on = db.Column(db.DateTime, nullable=False)

    rank = db.Column(db.Integer, nullable=False)
    effort_est = db.Column(db.String(50), nullable=True)
    value_est = db.Column(db.String(50), nullable=True)

    team = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return '<Iteration {}>'.format(self.name)

    def as_of(self, dt=None):
        '''Return a dictionary representation of self as it existed on date
           `dt`.

            TODO: Consider returning an object, rather than a dict
        '''
        # if we're asking for a date before the iteration was created, return None
        dt = dt or datetime.now()
        if dt < self.created_on:
            return None

        # hack to get around copying list bug with SQLA
        del self.events, self.tasks
        cp = _copy_as_of(self)
        cp['tasks'] = []
        change_map = {'estimate-change': 'effort_est'}
        for e in self.events:
            if e.occured_on >= dt:
                changed = change_map.get(e.type)
                if changed and e.task is None:
                    cp[changed] = getattr(e, '_'.join(['from', changed]))

        # re-roll tasks
        for task in Task.query.join(Event.task).filter(db.or_(
                Task.iteration == self, Event.from_iteration == self,
                Event.iteration == self)).group_by(Task).all():
            task_on_dt = task.as_of(dt)
            if task_on_dt and task_on_dt['iteration'].id == self.id:
                cp['tasks'].append(task_on_dt)

        return cp

# m2m self-join through table for dependency tracking between tasks
task_dependencies = db.Table('task_dependency', db.metadata,
                             db.Column('blocks_id', db.Integer,
                                       db.ForeignKey('task.id')),
                             db.Column('blocked_id', db.Integer,
                                       db.ForeignKey('task.id')))

class Task(db.Model):
    '''Model for execution items, which will typically be 'Stories' or 'Cards'
       in the upstream PM software (e.g. Jira).'''
    id = db.Column(db.Integer, primary_key=True)
    ext_id = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    created_on = db.Column(db.DateTime, nullable=False)
    updated_on = db.Column(db.DateTime, nullable=False)

    iteration_id = db.Column(db.Integer, db.ForeignKey('iteration.id'),
                             nullable=True)
    iteration = db.relationship('Iteration', backref='tasks')

    blocks = db.relationship("Task", secondary=task_dependencies,
                             primaryjoin=id==task_dependencies.c.blocked_id,
                             secondaryjoin=id==task_dependencies.c.blocks_id,
                             backref="blocked_by")

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', backref='tasks')

    started_on = db.Column(db.DateTime, nullable=True)
    dev_done_on = db.Column(db.DateTime, nullable=True)
    prod_done_on = db.Column(db.DateTime, nullable=True)
    effort_est = db.Column(db.String(50), nullable=True)

    # computed and cached information, will change with vacations
    dev_done_workdays = db.Column(db.Integer, nullable=True)
    prod_done_workdays = db.Column(db.Integer, nullable=True)

    # TODO: days are not granular enough, populate this later
    dev_done_workseconds = db.Column(db.Integer, nullable=True)
    prod_done_workseconds = db.Column(db.Integer, nullable=True)

    resolution = db.Column(db.String(50), nullable=True)
    rank = db.Column(db.Integer, nullable=True)

    # number of times through "testing" status
    round_trips = db.Column(db.Integer, nullable=True)

    def __init__(self, **kwargs):
        super(Task, self).__init__(**kwargs)
        self.cache_workdays()

    def __repr__(self):
        return '<Task {}>'.format(self.name.encode('utf8', errors='ignore'))

    def cache_workdays(self, force=False):
        '''Compute the number of work-days between started_on and dev_done_on
           and prod_done_on, and store on self.'''
        if force or (self.started_on and ((
                self.dev_done_on and self.dev_done_workdays is None) or (
                self.prod_done_on and self.prod_done_workdays is None))):
            days_off = set([v.date for v in self.user.vacation])
            days_off |= set([h.date for h in Holiday.query.all()])
            for stop,cache in (('dev_done_on', 'dev_done_workdays'),
                               ('prod_done_on', 'prod_done_workdays')):
                stop = getattr(self, stop)
                if self.started_on is not None and stop is not None:
                    setattr(self, cache, networkdays(self.started_on, stop,
                                                     holidays=days_off))
                elif force:
                    setattr(self, cache, None)
        return self.dev_done_workdays, self.prod_done_workdays

    def as_of(self, dt=None):
        '''Return a dictionary representation of self as it existed on date
           `dt`.

            TODO: Consider returning an object, rather than a dict
        '''
        # if we're asking for a date before the card was created, return None
        dt = dt or datetime.now()
        if dt < self.created_on:
            return None

        # hack to get around copying list bug with SQLA
        del self.events
        cp = _copy_as_of(self, relateds=['iteration', 'user'])

        # update date fields
        if cp['started_on'] and dt > cp['started_on']:
            cp['started_on'] = None
        for key in ('dev_done', 'prod_done'):
            on = cp['_'.join([key, 'on'])]
            if on and dt < on:
                cp['_'.join([key, 'on'])] = None
                cp['_'.join([key, 'workdays'])] = None

        # iterate events
        change_map = {'iteration-change': 'iteration',
                      'estimate-change': 'effort_est',
                      'user-change': 'user'}
        for e in self.events:
            if e.occured_on >= dt:
                changed = change_map.get(e.type)
                if changed:
                    cp[changed] = getattr(e, '_'.join(['from', changed]))
        return cp

class Stat(db.Model):
    '''Model of cached statistics about deliveries by estimate

       TODO: May by somewhat useless, or misleading, might want to aggregate
             trailing N-day averages, and look at it over time.'''
    id = db.Column(db.Integer, primary_key=True)
    dev_done_sample_size = db.Column(db.Integer, nullable=False, default=0)
    dev_done_mean = db.Column(db.Float, nullable=True)
    dev_done_median = db.Column(db.Float, nullable=True)
    dev_done_mode = db.Column(db.Float, nullable=True)
    dev_done_stddev = db.Column(db.Float, nullable=True)
    dev_done_stderr = db.Column(db.Float, nullable=True)
    dev_done_conf_int = db.Column(db.Float, nullable=True) # 95% conf int

    prod_done_sample_size = db.Column(db.Integer, nullable=False, default=0)
    prod_done_mean = db.Column(db.Float, nullable=True)
    prod_done_median = db.Column(db.Float, nullable=True)
    prod_done_mode = db.Column(db.Float, nullable=True)
    prod_done_stddev = db.Column(db.Float, nullable=True)
    prod_done_stderr = db.Column(db.Float, nullable=True)
    prod_done_conf_int = db.Column(db.Float, nullable=True) # 95% conf int

    round_trips_sample_size = db.Column(db.Integer, nullable=False, default=0)
    round_trips_mean = db.Column(db.Float, nullable=True)
    round_trips_median = db.Column(db.Float, nullable=True)
    round_trips_mode = db.Column(db.Float, nullable=True)
    round_trips_stddev = db.Column(db.Float, nullable=True)
    round_trips_stderr = db.Column(db.Float, nullable=True)
    round_trips_conf_int = db.Column(db.Float, nullable=True) # 95% conf int

    failure_rate = db.Column(db.Float, nullable=True)

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', backref='stats')

    as_of = db.Column(db.DateTime, nullable=False)
    effort_est = db.Column(db.String(50), nullable=True)
    db.Index('user_id_as_of_effort_est', user_id, as_of, effort_est,
             unique=True)

    def __repr__(self):
        return '<Stat for {} at {} est>'.format(self.user, self.effort_est)

#TODO: rank changes matter too
class Event(db.Model):
    '''Model for observed events that require notification'''
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.Enum('iteration-change', 'estimate-change',
                             'user-change', 'outlier'),
                     nullable=False)
    occured_on = db.Column(db.DateTime, nullable=False)

    # iteration associated with the change, when there is an iteration change
    # this column is the iteration after the change
    iteration_id = db.Column(db.Integer, db.ForeignKey('iteration.id'),
                             nullable=True)
    iteration = db.relationship(
        'Iteration', backref=db.backref('events',
                                        order_by=db.desc('Event.occured_on')),
        primaryjoin='Event.iteration_id == Iteration.id')

    # when there is an iteration change this column is the iteration before
    # the change
    from_iteration_id = db.Column(db.Integer, db.ForeignKey('iteration.id'),
                                  nullable=True)
    from_iteration = db.relationship(
        'Iteration', backref=db.backref('old_task_events',
                                        order_by=db.desc('Event.occured_on')),
        primaryjoin='Event.from_iteration_id == Iteration.id')

    task_id = db.Column(db.Integer, db.ForeignKey('task.id'), nullable=False)
    task = db.relationship('Task', backref=db.backref('events',
                           order_by=db.desc('Event.occured_on')))

    # additional info for user-change events
    from_user_id = db.Column(db.Integer, db.ForeignKey('user.id'),
                             nullable=True)
    from_user = db.relationship(
        'User', backref='old_task_events',
        primaryjoin='Event.from_user_id == User.id')

    to_user_id = db.Column(db.Integer, db.ForeignKey('user.id'),
                           nullable=True)
    to_user = db.relationship(
        'User', backref='new_task_events',
        primaryjoin='Event.to_user_id == User.id')

    # additional info for estimate-change events
    from_effort_est = db.Column(db.String(50), nullable=True)
    to_effort_est = db.Column(db.String(50), nullable=True)

    db.Index('task_id_type_occured_on', task_id, type, occured_on,
             unique=True)

    def __repr__(self):
        return '<Event {} on "{}" id: {}>'.format(self.type, self.task.name,
                                                  self.id)

# many-to-many for users <> simulation
simulation_users = db.Table('simulation_users', db.metadata,
    db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
    db.Column('simulation_id', db.Integer, db.ForeignKey('simulation.id')))

class Simulation(db.Model):
    '''Model to group all data-points in a simulation.

       simuation_on is the date from which the simulation was run, against
       progress. E.g. even if the simulation was run on day 3, if it was run
       as though it was run on day 2, simulation_on would be day 2.'''
    id = db.Column(db.Integer, primary_key=True)
    simulation_on = db.Column(db.DateTime, nullable=False)
    iteration_id = db.Column(db.Integer, db.ForeignKey('iteration.id'),
                             nullable=False)
    iteration = db.relationship('Iteration', backref='simulations')
    users = db.relationship("User", secondary=simulation_users,
                            backref="simulations")

    algorithm = db.Column(db.String(50), nullable=False)
    plays = db.Column(db.Integer, nullable=False)
    earliest_date = db.Column(db.DateTime, nullable=True)
    latest_date = db.Column(db.DateTime, nullable=True)

    data = db.Column(JSONField, nullable=True)
    errors = db.Column(JSONField, nullable=True)

    def __repr__(self):
        return '<Simulation of {} from {}>'.format(self.iteration,
                                                   self.simulation_on)

class Sync(db.Model):
    '''Model to store sync's that have been run'''
    id = db.Column(db.Integer, primary_key=True)
    synced_on = db.Column(db.DateTime, nullable=False,
                          server_default=db.func.now())
    last_seen_update = db.Column(db.DateTime, nullable=False)

    type = db.Column(db.Enum('full', 'iteration', 'task', 'holiday',
                             'vacation'), nullable=False)

    notes = db.Column(JSONField, nullable=True)

    def __repr__(self):
        return "<Sync'ed {} on {}>".format(self.type, self.synced_on)

__all__ = ['User', 'Holiday', 'Vacation', 'Iteration', 'Task', 'Stat',
           'Event', 'Simulation', 'Sync']
