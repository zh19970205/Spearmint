# -*- coding: utf-8 -*-

import os
import sys
import time
import optparse
import numpy as np

from spearmint.utils.database.mongodb import MongoDB

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options]")
    parser.add_option("--experiment-name", dest="experiment_name",
                      help="The name of the experiment in the database.",
                      type="string")

    parser.add_option("--database-address", dest="db_address",
                      help="The address where the database is located.",
                      type="string")

    parser.add_option("--job-id", dest="job_id",
                      help="The id number of the job to launch in the database.",
                      type="int")

    (options, args) = parser.parse_args()

    if not options.experiment_name:
        parser.error('Experiment name must be given.')

    if not options.db_address:
        parser.error('Database address must be given.')

    if not options.job_id:
        parser.error('Job ID not given or an ID of 0 was used.')

    launch(options.db_address, options.experiment_name, options.job_id)

def launch(db_address, experiment_name, job_id):
    """
    Launches a job from on a given id.
    """

    db  = MongoDB(database_address=db_address)
    job = db.load(experiment_name, 'jobs', {'id' : job_id})
    job.pop("_id")

    job['start time'] = time.time()
    db.save(job, experiment_name, 'jobs', {'id' : job_id})

    try:
        sys.path.append(os.path.realpath(job['expt_dir']))
        os.chdir(job['expt_dir'])
        params = {}
        for name, param in job['params'].iteritems():
            vals = param['values']
            if param['type'].lower() == 'float':
                params[name] = np.array(vals)
            else:
                raise Exception("Unknown parameter type.")
        main_file = job['main-file']
        if main_file[-3:] == '.py':
            main_file = main_file[:-3]
        sys.stderr.write('Importing %s.py\n' % main_file)
        module  = __import__(main_file)
        sys.stderr.write('Running %s.main()\n' % main_file)
        raw_result = module.main(job['id'], params)
        os.chdir('..')
        sys.stderr.write("Got result %s\n" % (raw_result))

        if isinstance(raw_result,list):
            result = {'main' : raw_result[0][-1]}
        else:
            result = {'main' : raw_result}

        job['values']   = result
        job['status']   = 'complete'
        job['end time'] = time.time()

        sys.stderr.write("Saving sample : %s\n"%job)
        db.save(job, experiment_name, 'jobs', {'id' : job_id})

        if isinstance(raw_result, list):
            for i in range(len(raw_result)):
                if i == 0:
                    continue
                try:
                    job['values'] = {'main' : raw_result[i][-1]}
                    for j,k in raw_result[i][0].iteritems():
                        job['params'][j]["values"][0] = k
                    jobs = db.load(experiment_name, 'jobs')
                    if jobs is None:
                        jobs = []
                    elif isinstance(jobs,dict):
                        jobs = [jobs]
                    job["id"]=len(jobs)+1
                    sys.stderr.write("Saving extra sample : %s\n"%job)
                    db.save(job, experiment_name, 'jobs', {'id' : job["id"]})
                except:
                    sys.stderr.write("Error extra sample : %s\n"%raw_result[i])

    except:
        import traceback
        traceback.print_exc()
        sys.stderr.write("Problem executing the function\n")
        print sys.exc_info()

        job['status']   = 'broken'
        job['end time'] = time.time()

        db.save(job, experiment_name, 'jobs', {'id' : job_id})

if __name__ == '__main__':
    main()
