#!/usr/bin/env python
'''
stackpm/workdays.py -- module providing work-day calculation functions for the
                       axial stack project management forecasting system.
'''

### 3RD PARY IMPORTS
from dateutil import rrule

### INTERNAL IMPORTS
from . import config

### GLOBALS
DEFAULT_WEEK = ("MO", "TU", "WE", "TH", "FR")

def _ruleset(workdays=None):
    '''Return a datetime.rrule.rruleset object that excludes weekends.'''
    workdays = workdays or config.get('work', {}).get('days', DEFAULT_WEEK)
    dates = rrule.rruleset()
    # exclude weekends
    dates.rrule(rrule.rrule(rrule.DAILY, byweekday=[
        getattr(rrule, workday) for workday in workdays ]))
    return dates

def workday_calendar(start=None, stop=None, excludes=None,
                     workday_calendar=None):
    '''
    Return a dateutil.rrule.rruleset object describing a work calendar.

    By default the returned rruleset object excludes weekends.

    Optional Arguments
    ------------------

    start
        Start date for calendar, default today.
    stop
        Stop date for calendar, default None.
    excludes
        An iterable of datetime.datetime objects to exclude in addition to
        weekends from the returned rruleset object. excludes is intended to
        provide support for both a holiday calendar and individual vacation
        schedules. It is recommended that excludes not be used for sick days,
        as schedule variance due to illness should affect your forecasting.
    workday_calendar
        A dateutil.rrule.rruleset object representing the base work calendar.
        This argument is useful when your normal schedule does not align with
        the default assumption that MO - FR are work-days.
    '''
    dates = _ruleset(workday_calendar)
    if start:
        dates.rrule(rrule.rrule(rrule.DAILY, dtstart=start))
    if stop:
        dates.rrule(rrule.rrule(rrule.DAILY, until=stop))
    if excludes:
        for exclude in excludes:
            dates.exdate(exclude)
    return dates

def net_workdays(start, stop, **kwargs):
    '''
    Return the number of work days between start and stop, less excludes.

    net_workdays computes the number of workdays less holidays and vacation
    (passed via the excludes argument) between 2 dates. net_workdays is meant
    to be functionaly equivalent to the Excel macro NETWORKDAYS.

    Required Arguments
    ------------------

    start
        A datetime.datetime object representing the beginning of the calendar
        period.
    stop
        A datetime.datetime object representing the end of the calendar
        period.

    Optional Arguments
    ------------------

    excludes
        An iterable of datetime.datetime objects to exclude from the calendar.
        For more information see `help(workday_calendar)`.
    workday_calendar
        A dateutil.rrule.rruleset object representing the base work calendar.
        For more information see `help(workday_calendar)`.
    '''
    kwargs['start'] = start
    kwargs['stop'] = stop
    dates = workday_calendar(**kwargs)
    return len(dates.between(start, stop, inc=True)) or 1

def workday(start, days, **kwargs):
    '''
    Return a datetime.datetime object `days` workdays from `start`.

    workday computes a datetime.datetime object `days` from `start` plus
    holidays and vacation (passed via the excludes argument) on a work
    calendar. workday is meant to be functionaly equivalent to the Excel macro
    WORKDAY.

    Required Arguments
    ------------------

    start
        A datetime.datetime object representing the beginning of the calendar
        period.
    days
        A numbers.Real type object (int, float, etc) representing the number
        of work days from `start` should be returned.

    Optional Arguments
    ------------------

    excludes
        An iterable of datetime.datetime objects to exclude from the calendar.
        For more information see `help(workday_calendar)`.
    workday_calendar
        A dateutil.rrule.rruleset object representing the base work calendar.
        For more information see `help(workday_calendar)`.
    '''
    kwargs['start'] = start
    dates = workday_calendar(**kwargs)
    return dates[max(int(days) - 1, 0)]

__all__ = ['workday_calendar', 'workday', 'net_workdays']
