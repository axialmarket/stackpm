'''stackpm/models.py -- Database API for stackpm

   functions: task_efforts, iteration_efforts, iteration_values
   @author: Matthew Story <matt.story@axial.net>
   @license: BSD 3-Clause (see LICENSE.txt)'''

### INTERNAL IMPORTS
from . import db, null
from .models import Task, Iteration, User

### INTERNAL METHODS
def _est(col, xtra_filter):
    '''Helper to return unique estimates with a generic filter'''
    query = db.session.query(col)
    if xtra_filter:
        query = query.filter(xtra_filter)

    return [row[0] for row in query.group_by(col).all()]

def _iter_est(col, team):
    '''Return either value_est or effort_est for an Iteration'''
    xtra_filter = None
    if team is not null:
        if team is None or isinstance(team, basestring):
            xtra_filter = Iteration.team == team
        else:
            query = Iteration.team.in_(team)

    return _est(col, xtra_filter)

### EXPOSED METHODS
def task_efforts(user=null):
    '''Return an iterable of unique task effort_est values, possibly
       constrained by a user.'''
    xtra_filter = None
    if user is not null:
        if user is None or isinstance(user, User):
            xtra_filter = Task.user == user
        else:
            xtra_filter = Task.user.in_(user)
    return _est(Task.effort_est, xtra_filter)

def iteration_efforts(team=null):
    '''Return an iterable of unique iteration effort_est values, possibly
       constrained by a team.'''
    return _iter_est(Iteration.effort_est, team)

def iteration_values(team=null):
    '''Return an iterable of unique iteration value_est values, possibly
       constrained by a team.'''
    return _iter_est(Iteration.value_est, team)
