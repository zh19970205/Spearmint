# -*- coding: utf-8 -*-

import sys
import optparse
import importlib
import time
import os

import numpy as np

try: import simplejson as json
except ImportError: import json

from collections import OrderedDict

from spearmint.utils.database.mongodb import MongoDB
from spearmint.tasks.task_group       import TaskGroup

from spearmint.resources.resource import parse_resources_from_config
from spearmint.resources.resource import print_resources_status

from spearmint.utils.parsing import parse_db_address

def get_options():
    parser = optparse.OptionParser(usage="usage: %prog [options] directory")

    parser.add_option("--config", dest="config_file",
                      help="Configuration file name.",
                      type="string", default="config.json")

    (commandline_kwargs, args) = parser.parse_args()

    # Read in the config file
    expt_dir  = os.path.realpath(os.path.expanduser(args[0]))
    if not os.path.isdir(expt_dir):
        raise Exception("Cannot find directory %s" % expt_dir)
    expt_file = os.path.join(expt_dir, commandline_kwargs.config_file)

    try:
        with open(expt_file, 'r') as f:
            options = json.load(f, object_pairs_hook=OrderedDict)
    except:
        raise Exception("config.json did not load properly. Perhaps a spurious comma?")
    options["config"]  = commandline_kwargs.config_file


    # Set sensible defaults for options
    options['chooser']  = options.get('chooser', 'default_chooser')
    if 'tasks' not in options:
        options['tasks'] = {'main' : {'type' : 'OBJECTIVE', 'likelihood' : options.get('likelihood', 'GAUSSIAN')}}

    # Set DB address
    db_address = parse_db_address(options)
    if 'database' not in options:
        options['database'] = {'name': 'spearmint', 'address': db_address}
    else:
        options['database']['address'] = db_address

    if not os.path.exists(expt_dir):
        sys.stderr.write("Cannot find experiment directory '%s'. "
                         "Aborting.\n" % (expt_dir))
        sys.exit(-1)

    return options, expt_dir

def main():
    options, expt_dir = get_options()

    resources = parse_resources_from_config(options)

    # Load up the chooser.
    chooser_module = importlib.import_module('spearmint.choosers.' + options['chooser'])
    chooser = chooser_module.init(options)
    experiment_name     = options.get("experiment-name", 'unnamed-experiment')

    # Connect to the database
    db_address = options['database']['address']
    sys.stderr.write('Using database at %s.\n' % db_address)        
    db         = MongoDB(database_address=db_address)
    
    while True:

        for resource_name, resource in resources.iteritems():

            jobs = load_jobs(db, experiment_name)
            # resource.printStatus(jobs)

            # If the resource is currently accepting more jobs
            # TODO: here cost will eventually also be considered: even if the 
            #       resource is not full, we might wait because of cost incurred
            # Note: I chose to fill up one resource and them move on to the next
            # You could also do it the other way, by changing "while" to "if" here

            while resource.acceptingJobs(jobs):

                # Load jobs from DB 
                # (move out of one or both loops?) would need to pass into load_tasks
                jobs = load_jobs(db, experiment_name)
                
                # Remove any broken jobs from pending.
                remove_broken_jobs(db, jobs, experiment_name, resources)

                # Get a suggestion for the next job
                suggested_job = get_suggestion(chooser, resource.tasks, db, expt_dir, options, resource_name)
    
                # Submit the job to the appropriate resource
                process_id = resource.attemptDispatch(experiment_name, suggested_job, db_address, expt_dir)

                # Set the status of the job appropriately (successfully submitted or not)
                if process_id is None:
                    suggested_job['status'] = 'broken'
                    save_job(suggested_job, db, experiment_name)
                else:
                    suggested_job['status'] = 'pending'
                    suggested_job['proc_id'] = process_id
                    save_job(suggested_job, db, experiment_name)

                jobs = load_jobs(db, experiment_name)

                # Print out the status of the resources
                # resource.printStatus(jobs)
                print_resources_status(resources.values(), jobs)

        # If no resources are accepting jobs, sleep
        # (they might be accepting if suggest takes a while and so some jobs already finished by the time this point is reached)
        if tired(db, experiment_name, resources):
            time.sleep(options.get('polling-time', 5))

def tired(db, experiment_name, resources):
    """
    return True if no resources are accepting jobs
    """
    jobs = load_jobs(db, experiment_name)
    for resource_name, resource in resources.iteritems():
        if resource.acceptingJobs(jobs):
            return False
    return True

def remove_broken_jobs(db, jobs, experiment_name, resources):
    """
    Look through jobs and for those that are pending but not alive, set
    their status to 'broken'
    """
    if jobs:
        for job in jobs:
            if job['status'] == 'pending':
                if not resources[job['resource']].isJobAlive(job):
                    sys.stderr.write('Broken job %s detected.\n' % job['id'])
                    job['status'] = 'broken'
                    save_job(job, db, experiment_name)

# TODO: support decoupling i.e. task_names containing more than one task,
#       and the chooser must choose between them in addition to choosing X
def get_suggestion(chooser, task_names, db, expt_dir, options, resource_name):

    if len(task_names) == 0:
        raise Exception("Error: trying to obtain suggestion for 0 tasks ")

    experiment_name = options['experiment-name']

    # We are only interested in the tasks in task_names
    task_options = { task: options["tasks"][task] for task in task_names }
    # For now we aren't doing any multi-task, so the below is simpler
    # task_options = options["tasks"]

    # Load the tasks from the database -- only those in task_names!
    task_group = load_task_group(db, options, task_names)

    # Load the model hypers from the database.
    hypers = load_hypers(db, experiment_name)

    # "Fit" the chooser - give the chooser data and let it fit the model.
    hypers = chooser.fit(task_group, hypers, task_options)

    # Save the hyperparameters to the database.
    save_hypers(hypers, db, experiment_name)

    # Ask the chooser to actually pick one.
    suggested_input = chooser.suggest()

    # TODO: implelent this
    suggested_task = task_names[0]  

    # Parse out the name of the main file (TODO: move this elsewhere)
    if "main-file" in task_options[suggested_task]:
        main_file = task_options[suggested_task]["main-file"]
    elif "main-file" in options:
        main_file = options['main-file']
    else:
        raise Exception("main-file not specified for task %s" % suggested_task)

    if "language" in task_options[suggested_task]:
        language = task_options[suggested_task]["language"]
    elif "language" in options:
        language = options['language']
    else:
        raise Exception("language not specified for task %s" % suggested_task)


    jobs = load_jobs(db, experiment_name)

    job_id = len(jobs) + 1

    job = {
        'id'          : job_id,
        'params'      : task_group.paramify(suggested_input),
        'expt_dir'    : expt_dir,
        'tasks'       : task_names,
        'resource'    : resource_name,
        'main-file'   : main_file,
        'language'    : language,
        'status'      : 'new',
        'submit time' : time.time(),
        'start time'  : None,
        'end time'    : None
    }

    save_job(job, db, experiment_name)

    return job

def save_hypers(hypers, db, experiment_name):
    if hypers:
        db.save(hypers, experiment_name, 'hypers')

def load_hypers(db, experiment_name):
    return db.load(experiment_name, 'hypers')

def load_jobs(db, experiment_name):
    """load the jobs from the database
    
    Returns
    -------
    jobs : list
        a list of jobs or an empty list
    """
    jobs = db.load(experiment_name, 'jobs')

    if jobs is None:
        jobs = []
    if isinstance(jobs, dict):
        jobs = [jobs]

    return jobs

def save_job(job, db, experiment_name):
    """save a job to the database"""
    db.save(job, experiment_name, 'jobs', {'id' : job['id']})

def load_task_group(db, options, task_names=None):
    if task_names is None:
        task_names = options['tasks'].keys()
    task_options = { task: options["tasks"][task] for task in task_names }

    jobs = load_jobs(db, options['experiment-name'])

    task_group = TaskGroup(task_options, options['variables'])

    if jobs:
        task_group.inputs  = np.array([task_group.vectorify(job['params'])
                for job in jobs if job['status'] == 'complete'])

        task_group.pending = np.array([task_group.vectorify(job['params'])
                for job in jobs if job['status'] == 'pending'])

        task_group.values  = {task : np.array([job['values'][task]
                for job in jobs if job['status'] == 'complete'])
                    for task in task_names}

        task_group.add_nan_task_if_nans()

        # TODO: record costs

    return task_group


# BROKEN
def print_diagnostics(chooser):
    sys.stderr.write("Optimizing over %d dimensions\n" % (expt_grid.vmap.cardinality))
    best_val   = None
    best_job   = None
    best_input = None
    if task.has_data():
        best_input, best_val = chooser.get_best()
        best_job = db.load(experiment_name, 'jobs', {'input' : best_input})

        if best_job:
            best_job = best_job[0]
        else:
            best_job 
            raise Warning('Job ID of best input/value pair not recorded.')

    # Track the time series of optimization. This should eventually go into a diagnostics module.
    trace_fh = open(os.path.join(expt_dir, 'trace.csv'), 'a')
    trace_fh.write("%d,%f,%d,%d,%d,%d\n"
                   % (time.time(), best_val, best_job,
                      tasks.pending.shape[0], tasks.invalid.shape[0], tasks.data.shape[0]))
    trace_fh.close()

    # Print out the best job results
    best_job_fh = open(os.path.join(expt_dir, 'best_job_and_result.txt'), 'w')
    best_job_fh.write("Best result: %f\nJob-id: %d\nParameters: \n" % 
                      (best_val, best_job))

    if best_input:
        for name, params in task.get_params(best_input):
            best_job_fh.write('%s: %s\n' % (name, params))

    best_job_fh.close()

if __name__ == '__main__':
    main()
