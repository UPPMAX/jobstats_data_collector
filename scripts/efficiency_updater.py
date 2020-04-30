#!/bin/env python3.6.7
# -*- coding: utf-8 -*-

# A script to summarize the efficiency of jobstats files
#
# Usage: python script.py <path to collected_jobstats folder> <path to slurm_accounting.sqlite> <path to efficiency folder>





import sqlite3
import sys
import os
from datetime import datetime
import shutil
import re
import time
import zlib 
import base64
import itertools
import glob
import argparse
import statistics
from multiprocessing import Pool
from functools import partial


from IPython.core.debugger import Tracer



# save the time the run started
updateStart = time.time()




import math  
import functools  
  
def percentile(N, percent, key=lambda x:x):  
    """  
    Find the percentile of a list of values.  
  
    @parameter N - is a list of values. Note N MUST BE already sorted.  
    @parameter percent - a float value from 0.0 to 1.0.  
    @parameter key - optional key function to compute value from each element of N.  
  
    @return - the percentile of the values  
    """  
    # Tracer()()
    N.sort()
    if not N:  
        return None  
    k = (len(N)-1) * percent  
    f = math.floor(k)  
    c = math.ceil(k)  
    if f == c:  
        return key(N[int(k)])  
    d0 = key(N[int(f)]) * (c-k)  
    d1 = key(N[int(c)]) * (k-f)  
    return d0+d1  
    





def chunk(xs, n):
    '''Split the list, xs, into n chunks'''
    L = len(xs)
    assert 0 < n <= L
    s = L//n
    return [xs[p:p+s] for p in range(0, L, s)]






def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result








# returns the connection to the database
def connectDB(db, type, cluster):

    # # check if the file exists and has a size > 0
    if not os.path.isfile("{}/{}.sqlite".format(db[type]['root'], cluster)):
    #     # if os.stat("{}/{}.sqlite".format(db['db_root'], cluster)).st_size > 0:
    #     #     # try repairing it if it does
    #     #     print("Trying to repair {}/{}.sqlite".format(db['db_root'], cluster))
    #     #     subprocess.call(["{}/repair_sqlite3db.sh".format(db['db_root']), "{}/{}.sqlite".format(db['db_root'], cluster)])
    #     # else:
    #     #     replace_db(db['db_root'], cluster)
    # # else:
        create_db(db, type, cluster)

    # first running
    if type not in db:
        db[type] = {}

    db[type][cluster] = {}
    db[type][cluster]['db'] = sqlite3.connect("{}/{}.sqlite".format(db[type]['root'], cluster))
    db[type][cluster]['db'].row_factory = sqlite3.Row
    db[type][cluster]['cur'] = db[type][cluster]['db'].cursor()
    # db[cluster]['db'].set_trace_callback(print)
    return db






# def replace_db(db_root, cluster):
#     # the db has to be replaced
#     print("Moving {0}/{1}.sqlite to {0}/{1}.sqlite.{2}.corrupt to avoid losing data.".format(db_root, cluster, datetime.now().strftime('%Y-%m-%d_%s')))
#     shutil.move("{}/{}.sqlite".format(db_root, cluster), "{}/{}.sqlite.{}.corrupt".format(db_root, cluster, ))
#     create_db(db_root, cluster)




def create_db(db, type, cluster):

    print("Creating new db: {}/{}.sqlite".format(os.path.normpath(db[type]['root']), cluster))
    # shutil.move("{}/{}".format(db_root, cluster), "{}/{}.corrupt".format(db_root, cluster))
    shutil.copy("{}/empty_shard".format(db[type]['root']), "{}/{}.sqlite".format(db[type]['root'], cluster))





def get_last_updated(db, type, cluster):

    # connect to the db if needed
    if cluster not in db:
        db = connectDB(db, type, cluster)

    # Tracer()()

    # get the timestamp
    try:
        res = db[type][cluster]['cur'].execute('SELECT date FROM updated').fetchone()
    except sqlite3.OperationalError:
        res = None

    # if it's the first time the database is updated
    if not res:
        return db, 0
    
    return db, res[0]





# @profile
def get_new_jobs(db, type, cluster, last_updated):

    # init
    new_jobs = {}

    # Tracer()()

    # query db for new jobs since last update
    db[type][cluster]['cur'].execute('SELECT * FROM slurm_accounting WHERE end>=? AND cluster=?', [last_updated, cluster])
    # db[type][cluster]['cur'].execute('SELECT * FROM slurm_accounting WHERE job_id=647373 AND cluster=?', ['rackham'])

    # go through the jobs
    print("Fetching new {} jobs".format(cluster))
    for i,job in enumerate(ResultIter(db[type][cluster]['cur'])):

        # store the slurm info
        new_jobs[job['job_id']] = dict(job)

        # print progress
        if i % 10000 == 0:
            print(i)

        # Tracer()()

    return new_jobs







def get_jobstats_files(db, cluster, new_jobs):

    # Tracer()()

    # fetch only jobstats files which id is larger or equal to the smallest new job id
    query = 'SELECT * FROM efficiency_rawdata WHERE job_id >= ?'
    min_job_id = min(new_jobs.keys()) - 20000 # take 20k extra jobs to make sure to not miss long running jobs

    # get the jobstats files for the new jobs
    db['jobstats'][cluster]['cur'].execute(query, [min_job_id])

    # Tracer()()
    
    for i,result in enumerate(ResultIter(db['jobstats'][cluster]['cur'])):

        # Tracer()()
        # print(i)

        # save the jobstats file to the jobs
        try:
            try:
                new_jobs[result['job_id']]['jobstats_files'].append(dict(result))
            except KeyError:
                # initiate the array if it's the first time the job is seen
                new_jobs[result['job_id']]['jobstats_files'] = [dict(result)]

        except:
            # skip jobstats files for jobs that are not in new_jobs
            # Tracer()()
            continue

        # Tracer()()
        # print progress
        if i % 10000 == 0:
            print(i)


        # # reconstruct the jobstats file
        # jobstats_file = zlib.decompress(base64.b64decode(result[1]))
        
        # # process jobstats file
        # # Tracer()()
        # toInsert.append(processFile(jobstats_file, new_jobs[cluster][int(result[0].split('.')[0])]))

    return new_jobs






# @profile
def process_job(job):

    # Tracer()()
    # print(type(job))
    # print(job)

    # skip jobs with missing jobstats files
    try:
        jobstats_files = job['jobstats_files']
    except KeyError:
        # if debug:
        #     print("Stat file missing: %s\t%s" % (node, jobid))
        return

    # get job info
    nodes = job['nodes'].split(',')
    jobid = job['job_id']
    proj_id = job['proj_id']
    user = job['user']
    date_finished = job['date'].split(' ')[0]
    state = job['jobstate']
    cluster = job['cluster']
    cores_tot = job['cores']

    # init the counter
    cpu_tot = []
    mem_tot = []
    cores_active = []
    cores = None
    mem_limit = None
    mem_limits = []
    mem_maxes = []
    entry_counters = []


    # go through the jobstats files for each node in the job
    for jobstats_file in jobstats_files:

        # pick out the actual jobstats string
        jobstats_file = jobstats_file['jobstats']

        # Tracer()()



        # decompress the jobstats file
        jobstats_file = zlib.decompress(base64.b64decode(jobstats_file)).decode('utf-8')
        
        # reset node specific things
        mem_node_usage = []
        entry_counter = 0

        # process the stat file line by line
        for line in jobstats_file.split("\n"):

            # skip empty lines
            if line == '':
                continue

            # Tracer()()

            try:
                line_split = line.split()
                
                # sanity check all data types, and crash if anything is wrong
                datetime.strptime(line_split[0], '%Y-%m-%dT%H:%M:%S')
                int(line_split[1])
                float(line_split[2])
                float(line_split[3])
                float(line_split[4])

                # add the mem value
                mem_tot.append(float(line_split[3]))
                mem_node_usage.append(float(line_split[3]))


                # add the cpu values
                # Tracer()()
                cpu_usage = list(map(float, line_split[5:]))     # [float(element) for element in line_split[5:]]
                cores_active.append(sum([1 if x>10 else 0 for x in cpu_usage])) # count nr of cores with more than 10% usage
                cpu_tot += cpu_usage

                # save limit and cores if it is has not been saved before
                try:
                    cores+1
                except:
                    cores = len(line_split[5:])
                    mem_limit = float(line_split[2])

                # count the number of lines in this jobstats file
                entry_counter = entry_counter + 1

                # Tracer()()

            except:

                # if not line.startswith('L'):
                #     print("Weird line: {}".format(line))
                # Tracer()()
                continue
        
        # save node specific values
        if len(mem_node_usage) == 0:
            print("ERROR: mem_node_usage length 0: {}".format(job['job_id']))
            continue

        mem_limits.append(mem_limit)
        mem_maxes.append(max(mem_node_usage))
        entry_counters.append(entry_counter)

    # summarize the collected data
    # Tracer()()
    # save the peak memory and average cpu usage
    try:
        job_entry_max = max(entry_counters)
        expected_entries_tot = cores_tot/cores * job_entry_max

        # find the node with the most full memory
        mem_ratios = [m/l for m,l in zip(mem_maxes, mem_limits)]
        most_mem_full_idx = mem_ratios.index(max(mem_ratios))
        job_mem_max = mem_maxes[most_mem_full_idx]
        job_mem_limit = mem_limits[most_mem_full_idx]
        # or should i just include the whole array in the db?

        # compensate for lost jobstats files for nodes in job
        if len(cores_active) < expected_entries_tot:
            cores_active += [0] * int(expected_entries_tot-len(cores_active))
        if len(mem_tot) < expected_entries_tot:
            mem_tot += [0] * int(expected_entries_tot-len(mem_tot))
        if len(cpu_tot) < job_entry_max*cores_tot:
            cpu_tot += [0] * int(job_entry_max*cores_tot-len(cpu_tot))
        
        # calculate the mean cpu usage
        cpu_mean = sum(cpu_tot) / len(cpu_tot)
    # if either cpu or memory values are missing, skip the job
    except:
        return

    # Tracer()()



    # calculate the percentile of used cores
    cpu_cores_used_percentile = percentile(cores_active, 0.75)

    mem_median = statistics.median(mem_tot)


    # return the result
    return [jobid, cluster, proj_id, user, cpu_mean, cpu_cores_used_percentile, cores_tot, job_mem_max, mem_median, job_mem_limit, job_entry_max, state, date_finished, ",".join(nodes)]










def insert(db, cluster, to_insert):
    

    # skip empty batches
    if len(to_insert) == 0:
        return

    # Tracer()()
    # insert the records into the db
    try:
        db['efficiency'][cluster]['cur'].executemany(u"INSERT OR REPLACE INTO efficiency (job_id, cluster, proj_id, user, cpu_mean, cpu_cores_used_percentile, cores, mem_peak, mem_median, mem_limit, entries, state, date_finished, nodelist) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_insert)
    except:
        # Tracer()()
        sys.exit("Error, wtf")

    # update the updated-timestamp
    db['efficiency'][cluster]['cur'].execute("INSERT OR REPLACE INTO updated (table_name,date,date_human) VALUES (?,?,?);", ["efficiency", updateStart, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(updateStart))])

    # save the database entry
    db['efficiency'][cluster]['db'].commit()





# @profile
def main():


    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobstats", help="path to the collected_jobstats folder", required=True)
    parser.add_argument("-s", "--slurm_accounting", help="path to the slurm_accounting folder", required=True)
    parser.add_argument("-e", "--efficiency", help="path to the efficiency folder", required=True)
    parser.add_argument("-p", "--processes", type=int, help="the number of processes/threads to use when processing jobs", default=1)
    args = parser.parse_args()


    # get the db root argument
    db = {'jobstats': {'root': args.jobstats}}
    if not os.path.isdir(db['jobstats']['root']):
        raise SystemExit("ERROR - Jobstats database root folder not found: {}".format(sys.argv[0], db['jobstats']['root']))



    # get the slurm_accounting path
    db['slurm_accounting'] = {'root': args.slurm_accounting} 
    
    # make sure the path exists
    if not os.path.isdir(db['slurm_accounting']['root']):
        raise SystemExit("ERROR - slurm_accounting database root path not found: {}".format(sys.argv[0], db['slurm_accounting']['root']))


    # get the efficiency path
    db['efficiency'] = {'root': args.efficiency}

    # make sure the path exists
    if not os.path.isdir(db['efficiency']['root']):
        raise SystemExit("ERROR - efficiency database root path not found: {}".format(sys.argv[0], db['efficiency']['root']))

    
    # Tracer()()




    # save the time the run started
    updateStart = time.time()


    # connect to all the dbs
    for cluster_slurm_db in sorted(glob.glob(os.path.join(db['slurm_accounting']['root'], '*.sqlite'))):

        # get the cluster name without the file ending
        cluster = os.path.splitext(os.path.basename(cluster_slurm_db))[0]
        # Tracer()()


        if cluster in ['bianca', 'sens-bianca']:
            continue

        # connect to the dbs
        db = connectDB(db, 'slurm_accounting', cluster)
        db = connectDB(db, 'jobstats', cluster)
        db = connectDB(db, 'slurm_accounting', cluster)

        # get when the efficiency db was last updated
        db, last_updated = get_last_updated(db, 'efficiency', cluster)

        # Tracer()()


        # get all jobs that has finished since last update
        new_jobs = get_new_jobs(db, 'slurm_accounting', cluster, last_updated)

        # skip cluster if there are no new jobs since last run
        if len(new_jobs) == 0:
            continue

        # Tracer()()
        # process the collected files

        # Tracer()()
        # get corresponding jobstats files
        print("Fetching jobstats files for {} jobs".format(cluster))
        new_jobs = get_jobstats_files(db, cluster, new_jobs)
        
        print("Processing {} jobs".format(cluster))

        parallellize = False
        if not parallellize:
            # no parallelization
            to_insert = []
            for job in [res for res in new_jobs.values() if 'jobstats_files' in res.keys()]:
                to_insert.append(process_job(job))

        else:
            # parse the files in parallel and wait until all threads have finished
            pool=Pool(processes=args.processes)
             
            to_insert = pool.map(process_job, [res for res in new_jobs.values() if 'jobstats_files' in res.keys()])
            pool.close()
            pool.join()

        # insert the jobs to db
        print("Inserting {} jobs into {} db".format(len(to_insert),  cluster))
        insert(db, cluster, [res for res in to_insert if res is not None])

        # reset
        new_jobs = {}
        to_insert = []




if __name__ == "__main__":
    main()
