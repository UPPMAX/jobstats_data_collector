#!/bin/env python3.6.7
# -*- coding: utf-8 -*-

# A script to collect jobstats files and store them in a database
#
# Usage: python collect_jobstats.py <path to collected_jobstats folder> <path to slurm_accounting.sqlite>


import sqlite3
import sys
import os
import shutil
import subprocess
from datetime import datetime
import time
from pathlib import Path
import fnmatch
import zlib 
import base64
from multiprocessing import Pool
import argparse


from IPython.core.debugger import Tracer








### SETTINGS

# commit the collected jobs each progress report
commit_every_progress_report = 1


slurmPath = '/sw/share/slurm/'
# slurmPath = '/dev/shm/dahlo/slurm'






# returns the connection to the database
def connectDB(db, type, cluster):

    # Tracer()()

    # check if the file exists and has a size > 0
    if not os.path.isfile("{}/{}.sqlite".format(db[type]['db_root'], cluster)):
        # if os.stat("{}/{}.sqlite".format(db['db_root'], cluster)).st_size > 0:
        #     # try repairing it if it does
        #     print("Trying to repair {}/{}.sqlite".format(db['db_root'], cluster))
        #     subprocess.call(["{}/repair_sqlite3db.sh".format(db['db_root']), "{}/{}.sqlite".format(db['db_root'], cluster)])
        # else:
        #     replace_db(db['db_root'], cluster)
    # else:
        create_db(db[type]['db_root'], cluster)

    db[type][cluster] = {}
    db[type][cluster]['db'] = sqlite3.connect("{}/{}.sqlite".format(db[type]['db_root'], cluster))
    db[type][cluster]['cur'] = db[type][cluster]['db'].cursor()
    return db





def connectSlurmDB(db, path):
    db['slurm_accounting'] = {}
    db['slurm_accounting']['db'] = sqlite3.connect(path)
    db['slurm_accounting']['cur'] = db['slurm_accounting']['db'].cursor()
    return db





def replace_db(db_root, cluster):
    # the db has to be replaced
    print("Moving {0}/{1}.sqlite to {0}/{1}.sqlite.{2}.corrupt to avoid losing data.".format(db_root, cluster, datetime.now().strftime('%Y-%m-%d_%s')))
    shutil.move("{}/{}.sqlite".format(db_root, cluster), "{}/{}.sqlite.{}.corrupt".format(db_root, cluster, ))
    create_db(db_root, cluster)




def create_db(db_root, cluster):

    print("Creating new db: {}/{}.sqlite".format(os.path.normpath(db_root), cluster))
    # shutil.move("{}/{}".format(db_root, cluster), "{}/{}.corrupt".format(db_root, cluster))
    shutil.copy("{}/empty_shard".format(db_root), "{}/{}.sqlite".format(db_root, cluster))







def get_new_files(db, cluster, slurmPath):

    # Tracer()()

    # init
    lastUpdated = get_lastupdated(db, 'jobstats', cluster)
    # lastUpdated = 1526217072

    # get the jobs that have finished after the last update
    res = db['slurm_accounting'][cluster]['cur'].execute("SELECT job_id, nodes FROM slurm_accounting WHERE cluster=? AND end>=?", [cluster, lastUpdated-(3600 * 24 * 30)]).fetchall()

    # construct paths to where the jobstats files should be
    new_files = []
    for job_id, nodes in res:
        for node in nodes.split(','):
            new_files.append("{}/{}/uppmax_jobstats/{}/{}".format(slurmPath, cluster, node, job_id))

    return new_files





def get_new_files_bianca(db, cluster):
    return





def get_lastupdated(db, type, cluster):

    # get the timestamp
    res = db[type][cluster]['cur'].execute('SELECT date FROM updated').fetchone()

    # if it's the first time the database is updated
    if not res:
        return 0
    
    return float(res[0])





def get_jobs_in_db(db, cluster):
    # Tracer()()

    job_memory = set()
    db['jobstats'][cluster]['cur'].execute("SELECT job_id,node FROM efficiency_rawdata")
    for job_id,node in db['jobstats'][cluster]['cur'].fetchall():
        job_memory.add("{}.{}".format(job_id, node))

    return job_memory






def insert(db, cluster, to_insert):

    # skip empty batches
    if len(to_insert) == 0:
        return

    # insert the records into the db
    try:
        # replace duplicates, that should not even exist in the first place..
        db['jobstats'][cluster]['cur'].executemany(u"INSERT OR REPLACE INTO efficiency_rawdata (job_id, node, jobstats) VALUES (?,?,?);", to_insert)
    except:
        print("Failed to insert a batch info {}".format(cluster))
        return # skip errors
        # Tracer()()

    # save the database entry
    db['jobstats'][cluster]['db'].commit()

    # append the records to the new backup archive
    with open("{}/{}".format(jobstats_backup_root, cluster), 'a') as archive_file:

        # convert the lists to lines
        # Tracer()()
        lines = []
        for line in to_insert:
            # Tracer()()
            lines.append("\t".join(line)+'\n')
        # Tracer()()
        # then write all the lines in one go
        archive_file.writelines(lines)





# @profile
def process_file(file):
    

        # if the jobstats file does not exist
        try:

            with open(file, 'r') as raw_file:

                node, job_id = file.split('/')[-2:]

                # construct line for insertion
                content = raw_file.read()
                # Tracer()()
                compressed_content = base64.b64encode(zlib.compress(bytes(content, 'utf-8'))).decode('utf-8')
                # Tracer()()
                # compressed_content = base64.b64encode(bytes(content, 'utf-8')).decode('utf-8')
                return [job_id, node, compressed_content]

        # if the jobstats file does not exist, skip it
        except FileNotFoundError:
            pass




# @profile
def process_files(db, cluster, new_files, updateStart):

    # Tracer()()

    # get a list of all jobs already in the db
    job_memory = get_jobs_in_db(db, cluster)
    
    # Tracer()()
    # go through the list of potentially new files and filter out the ones that are already in the database
    new_files_filtered = []
    for file in new_files:

        # check if the file already has been saved
        node, job_id = file.split('/')[-2:]
        job_key = "{}.{}".format(job_id, node) 
        if job_key in job_memory:
            continue

        new_files_filtered.append(file)


    # parse the files in parallel and wait until all threads have finished
    pool=Pool(processes=args.processes)
    processed_files = pool.map(process_file, new_files_filtered)
    pool.close()
    pool.join()

    # Tracer()()
    # insert all not None values
    insert(db, cluster, [res for res in processed_files if res is not None])


    # update the updated-timestamp
    db['jobstats'][cluster]['cur'].execute("INSERT OR REPLACE INTO updated (table_name,date,date_human) VALUES (?,?,?);", ["efficiency_rawdata", updateStart, datetime.fromtimestamp(updateStart).strftime('%Y-%m-%d %H:%M:%S')])

    # save the database entry
    db['jobstats'][cluster]['db'].commit()










parser = argparse.ArgumentParser()
parser.add_argument("-j", "--jobstats", help="path to the collected_jobstats folder", required=True)
parser.add_argument("-s", "--slurm_accounting", help="path to the slurm_accounting folder", required=True)
parser.add_argument("-p", "--processes", type=int, help="the number of processes/threads to use when processing files", default=1)
args = parser.parse_args()


# Tracer()()






# get the db root argument

db = {'jobstats': {'db_root':args.jobstats}}
if not os.path.isdir(db['jobstats']['db_root']):
    raise SystemExit("ERROR - collected_jobstats folder not found: {}".format(sys.argv[0], args['jobstats']))
jobstats_backup_root = '{}/backup/'.format(db['jobstats']['db_root'])
os.makedirs(jobstats_backup_root, exist_ok=True)



db['slurm_accounting'] = {'db_root':args.slurm_accounting}
# make sure the path exists
if not os.path.isdir(db['slurm_accounting']['db_root']):
    raise SystemExit("ERROR - slurm_accounting folder not found: {}".format(sys.argv[0], args['slurm_accounting']))


print("Starting the update using {} processes".format(args.processes))

# save the time the run started
updateStart = time.time()



# Tracer()()

# # get all cluster names
for cluster in sorted([ name for name in os.listdir(slurmPath) if os.path.isdir(os.path.join(slurmPath, name)) ], reverse=False):

    # if cluster != 'rackham':
    #     continue


    # Tracer()()

    # skip folder that don't have jobstats files
    if not os.path.isdir('{}/{}/uppmax_jobstats/'.format(slurmPath, cluster)):
        continue

    # get a list of all jobs that are new since last update
    if cluster == 'bianca' or cluster == 'sens-bianca':

        continue

        # loop over all virtual clusters as well
        for vcluster in sorted([ name for name in os.listdir('{}/{}/uppmax_jobstats'.format(slurmPath, cluster)) if os.path.isdir(os.path.join('{}/{}/uppmax_jobstats'.format(slurmPath, cluster), name)) ]):

            # construct the clustername
            clustername = "{}.{}".format(cluster, vcluster)

            # fetch a list of new jobs since last update
            print("Getting {} jobs that are new since last update.".format(clustername))
            new_files = get_new_files_bianca(db, clustername, slurmPath)

            # process the new jobstats files
            print("Processing new {} jobs.".format(clustername))
            process_files(db, clustername, new_files, updateStart)

    else:

         # connect to the dbs
        db = connectDB(db, 'slurm_accounting', cluster)
        db = connectDB(db, 'jobstats', cluster)

        # Tracer()()
        # construct the clustername
        clustername = cluster

        # fetch a list of new jobs since last update
        print("Getting {} jobs that are new since last update.".format(clustername))
        new_files = get_new_files(db, clustername, slurmPath)
        # Tracer()()
        # process the new jobstats files
        print("Processing new {} jobs.".format(clustername))
        process_files(db, clustername, new_files, updateStart)
        # Tracer()()
        

print("Update finished.")





