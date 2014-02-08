'''stackpm/models.py -- Database API for stackpm

   classes: User, Vacation, Iteration, Task, Stat, Event, Simulation
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

from . import db
from .fields import JSONField

class User(db.Model):
    '''Model to map local usage to 3rd party tool like Jira

       NB: JIRA does not seem to require unique emails ... but we're not as
           stupid as they are ... you will break stackpm if you are silly
           enough to duplicate emails in JIRA.'''
    id = db.Column(db.Integer, primary_key=True)
    pm_name = db.Column(db.String(255), unique=True, nullable=True)
    email = db.Column(db.String(255), unique=True, nullable=False)

    def __init__(self, email, pm_name=None):
        self.email = email
        self.pm_name = pm_name

    def __repr__(self):
        return '<User {}>'.format(self.email)

class Vacation(db.Model):
    '''Model to store vacations and holidays, holidays are nullable on user_id
       field'''
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    user = db.relationship('User', backref='vacations')

    def __init__(self, date, user=None):
        self.date = date
        self.user = user

    def __repr__(self):
        return '<Vacation {} for {}>'.format(self.date, self.user or 'All')

class Iteration(db.Model):
    '''Model for iterations, which will typically be 'Epics' in the upstream
       PM software (e.g. Jira)

       TODO: How to track negative changes over time?'''
    id = db.Column(db.Integer, primary_key=True)
    ext_id = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    created_on = db.Column(db.DateTime, nullable=False)
    updated_on = db.Column(db.DateTime, nullable=False)

    effort_est = db.Column(db.String(50), nullable=True)
    value_est = db.Column(db.String(50), nullable=True)
    project = db.Column(db.String(255), nullable=True) #TODO: ref?

    def __init__(self, name, **kwargs):
        self.name = name
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return '<Iteration {}>'.format(self.name)

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
    added_to_iteration_on = db.Column(db.DateTime, nullable=False)
    effort_est = db.Column(db.String(50), nullable=True)

    # computed and cached information, will change with vacations
    # TODO: hours? days will not be granular enough for all cases
    dev_done_workdays = db.Column(db.Integer, nullable=True)
    prod_done_workdays = db.Column(db.Integer, nullable=True)

    def __init__(self, name, **kwargs):
        self.name = name
        iteration_ext_id = kwargs.pop('iteration_ext_id', None)
        if iteration_ext_id is not None:
            kwargs['iteration'] = \
                Iteration.query.filter_by(ext_id=iteration_ext_id).first()
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return '<Execution Task {}>'.format(self.name)

class Stat(db.Model):
    '''Model of cached statistics about deliveries by estimate

       TODO: May by somewhat useless, or misleading, might want to aggregate
             trailing N-day averages, and look at it over time.'''
    id = db.Column(db.Integer, primary_key=True)
    sample_size = db.Column(db.Integer, nullable=False, default=0)
    dev_done_mean = db.Column(db.Float, nullable=False)
    dev_done_median = db.Column(db.Integer, nullable=False)
    dev_done_stddev = db.Column(db.Float, nullable=False)
    dev_done_stderr = db.Column(db.Float, nullable=False)
    dev_done_conf_int = db.Column(db.Float, nullable=False) # 95% conf int
    prod_done_mean = db.Column(db.Float, nullable=False)
    prod_done_median = db.Column(db.Float, nullable=False)
    prod_done_stddev = db.Column(db.Float, nullable=False)
    prod_done_stderr = db.Column(db.Float, nullable=False)
    prod_done_conf_int = db.Column(db.Float, nullable=False) # 95% conf int

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', backref='stats')

    effort_est = db.Column(db.String(50), nullable=True)

    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return '<Stat for {} at {} est>'.format(self.user, self.effort_est)

class Event(db.Model):
    '''Model for observed events that require notification'''
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.Enum('iteration-change', 'estimate-change',
                             'outlier'),
                     nullable=False)
    occured_on = db.Column(db.DateTime, nullable=False)

    # iteration associated with the change, when there is an iteration change
    # one record should be created for each iteration
    iteration_id = db.Column(db.Integer, db.ForeignKey('iteration.id'),
                             nullable=True)
    iteration = db.relationship('Iteration', backref='events')

    task_id = db.Column(db.Integer, db.ForeignKey('task.id'), nullable=False)
    task = db.relationship('Task', backref='events')

    # additional info for estimate-change events
    effort_est_from = db.Column(db.String(50), nullable=True)
    effort_est_to = db.Column(db.String(50), nullable=True)

    def __init__(self, type_, task):
        self.type = type_
        self.task = task

    def __repr__(self):
        return '<Event {} on task {}>'.format(self.type, self.task)

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

    data = db.Column(JSONField, nullable=True)

    def __init__(self, simulation_on, iteration, data):
        self.simulation_on = simulation_on
        self.iteration = iteration
        self.data = data

    def __repr__(self):
        return '<Simulation of {} from {}>'.format(self.iteration,
                                                   self.simulation_on)
