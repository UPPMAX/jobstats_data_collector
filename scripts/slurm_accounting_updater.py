#!/bin/env python3.5
# -*- coding: utf-8 -*-

# A script to update the slurm_accounting database
# 
# Usage: python slurm_accounting_updater.py <path to database file>


import sqlite3
from datetime import datetime, timedelta
import os
import ntpath
import time
import re
import string
import sys
import fnmatch
import argparse
import shutil
from multiprocessing import Pool


from IPython.core.debugger import Tracer



### SETTINGS

# set to 1 if the db should be commited after each slurm file has been processed. 1 is slower (~25% longer time) but uses much less memory (~0 vs 20gb).
commit_every_loop = 1


# remove all weird characters from strings
def format_string(s):

    valid_chars = set(string.printable)
    name = ''.join(c for c in s if c in valid_chars)
    name = re.sub(r"\s+", '_', name)
    return name




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



def create_db(db_root, cluster):

    print("Creating new db: {}/{}.sqlite".format(os.path.normpath(db_root), cluster))
    # shutil.move("{}/{}".format(db_root, cluster), "{}/{}.corrupt".format(db_root, cluster))
    shutil.copy("{}/empty_shard".format(db_root), "{}/{}.sqlite".format(db_root, cluster))




# get the data when the database was last updated
def get_last_updated(db, cluster):

    # check if the projects has ever been updated
    query = "SELECT date FROM updated WHERE table_name='slurm_accounting';"
    db['slurm_accounting'][cluster]['cur'].execute(query)
    result = db['slurm_accounting'][cluster]['cur'].fetchone()
    

    # if it has never been updated before, i.e. it's the first run
    if not result:

        # initiate the record
        query = "INSERT INTO updated (table_name, date, date_human) VALUES ('slurm_accounting', 0, '1970-01-01 00:00:00');"
        db['slurm_accounting'][cluster]['cur'].execute(query)
        return 0


    # return the date
    return float(result[0])







# update the jobs
def updateJobs(db, clusters, last_updated, currentDate):

    # create a cursor
    cur = db.cursor()

    if commit_every_loop == 0:
        # initialize the list where jobs to be inserted are collected
        batchInsert = list()

    # update each cluster
    for cluster_name in sorted(clusters):

        # Tracer()()
        # save as the clusters name
        cluster = cluster_name

        # get all the files
        for root, dirnames, filenames in os.walk('/sw/share/slurm/{}/accounting/'.format(cluster)):

            # filter out the slurm accounting files
            for slurmFile in sorted(fnmatch.filter(filenames, '????-??-??')):

                # only if the file has not been processed yet, and is not todays file (assumed that the program is run slightly after midnight, and we only want to save complete slurm log files)
                if slurmFile >= last_updated and slurmFile <= currentDate:

                    if commit_every_loop == 1:
                        # empty the list where jobs to be inserted are collected
                        batchInsert = list()

                    # open the file
                    sf = open(os.path.join(root, slurmFile), 'r', errors='replace')
                    print("Processing new: {}".format(os.path.join(root, slurmFile)))

                    # go through the file
                    for line in sf:

                        # get the start and end time of the job
                        match = re.search('^(\S+ \S+) jobstate=(\S+) jobid=(\S+) username=(\S+) account=(\S+) start=(\d+) end=(\d+).+ nodes=(\S*).* procs=(\d+).+ jobname=(.*) partition=(\S+)', line)

                        if match:

                            # skip if run time is zero
                            start = int(match.groups()[5])
                            end = int(match.groups()[6])
                            if (start  ==  end) or (start == 0) or (end == 0):
                                continue

                            date = match.groups()[0]	
                            jobstate = match.groups()[1]
                            job_id = match.groups()[2]
                            username = match.groups()[3]
                            proj_id = match.groups()[4]
                            start = match.groups()[5]
                            end = match.groups()[6]
                            nodes = match.groups()[7]
                            procs = match.groups()[8]
                            jobname = format_string(match.groups()[9])
                            partition = match.groups()[10]

                            # skip empty jobs, must be something wrong with them
                            if procs == 0:
                                continue


                            # generate a node list
                            # get cluster prefix and a node list
                            match = re.search('^([a-zA-Z0-9-]+)\[?([0-9,\-]+)\]?$', nodes)
                            if match:
                                cluster_prefix = match.groups()[0]
                                nodes_elements = match.groups()[1]

                                # split the elements on commas
                                nodes_elements = nodes_elements.split(",")

                                # init
                                nodes = []

                                # go through the node elements
                                for element in nodes_elements:

                                    # if it is a range
                                    match = re.search('(\d+)\-(\d+)', element)
                                    if match:

                                        # construct the range and add to node list
                                        nodes = nodes + list(range(int(match.groups()[0]), int(match.groups()[1])+1))

                                    # if it's not an interval, it must be a single node (right?)
                                    else:
                                        nodes = nodes + [int(element)]

                                # append cluster prefix to all node numbers and concatinate with commas
                                nodes = ",".join([cluster_prefix + str(node) for node in nodes])

                            # if there were no nodes
                            else:
                                nodes = ""

                            # add the virtual cluster name to bianca clusters
                            if cluster_name == 'bianca':
                                cluster = "{}.{}".format(cluster_name, proj_id)

                            # store the job info
                            batchInsert.append([date, cluster, job_id, proj_id, username, start, end, partition, procs, nodes, jobname, jobstate])

                        # not a valid line, investigate
                        else:
                            print("INVALID LINE: {}".format(line))
                            pass

                    if commit_every_loop == 1:
                        # insert the collected jobs
                        cur.executemany(u"INSERT OR REPLACE INTO slurm_accounting (date, cluster, job_id, proj_id, user, start, end, partition, cores, nodes, jobstate, jobname) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", batchInsert)


    if commit_every_loop == 0:
        # insert the collected jobs
        cur.executemany(u"INSERT OR REPLACE INTO slurm_accounting (date, job_id, proj_id, user, start, end, partition, cores, nodes, cluster, jobstate, jobname) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", batchInsert)

    # update the last_updated record
    query = u"UPDATE updated SET date = '{}' WHERE table_name = '{}'".format(currentDate, 'slurm_accounting')
    cur.execute(query)

    # save the database entry
    db.commit()





# @profile
def fetch_new_jobs(cluster):

    # check when the database was last updated
    # print(get_last_updated(db, cluster))
    last_updated = datetime.fromtimestamp(get_last_updated(db, cluster))

    # init
    to_insert = []

    # compile regex
    valid_slurm_line = re.compile(r'^(\S+ \S+) jobstate=(\S+) jobid=(\S+) username=(\S+) account=(\S+) start=(\d+) end=(\d+).+ nodes=(\S*).* procs=(\d+).+ jobname=(.*) partition=(\S+)')
    valid_node_names = re.compile(r'^([a-zA-Z0-9-]+)\[?([0-9,\-]+)\]?$')
    valid_node_range = re.compile(r'(\d+)\-(\d+)')

    # handle virtual clusters for bianca
    cluster_name = cluster


    # get all the files
    for root, dirnames, filenames in os.walk('/sw/share/slurm/{}/accounting/'.format(cluster)):

        # filter out the slurm accounting files
        for slurmFile in sorted(fnmatch.filter(filenames, '????-??-??')):

            # skip files that have already been processed
            if slurmFile < last_updated.strftime('%Y-%m-%d'):
                continue

            # open the file
            with open(os.path.join(root, slurmFile), 'r', errors='replace') as sf:
                print("Processing new: {}".format(os.path.join(root, slurmFile)))

                # go through the file
                for line in sf:

                    # Tracer()()

                    # get the start and end time of the job
                    match = valid_slurm_line.match(line)

                    # not a valid line, investigate
                    if not match:
                        print("INVALID LINE: {}".format(line))
                        continue


                    # skip if run time is zero
                    start = int(match.groups()[5])
                    end = int(match.groups()[6])
                    if (start  ==  end) or (start == 0) or (end == 0):
                        continue
                    
                    # readability
                    date = match.groups()[0]	
                    jobstate = match.groups()[1]
                    jobid = match.groups()[2]
                    username = match.groups()[3]
                    proj_id = match.groups()[4]
                    start = match.groups()[5]
                    end = match.groups()[6]
                    nodes = match.groups()[7]
                    procs = match.groups()[8]
                    jobname = format_string(match.groups()[9])
                    partition = match.groups()[10]

                    # skip empty jobs, must be something wrong with them
                    if procs == 0:
                        continue


                    # generate a node list
                    # get cluster prefix and a node list
                    match = valid_node_names.match(nodes)
                    if match:
                        cluster_prefix = match.groups()[0]
                        nodes_elements = match.groups()[1]

                        # split the elements on commas
                        nodes_elements = nodes_elements.split(",")

                        # init
                        nodes = []

                        # go through the node elements
                        for element in nodes_elements:

                            # if it is a range
                            match = valid_node_range.match(element)
                            if match:

                                # construct the range and add to node list
                                nodes = nodes + list(range(int(match.groups()[0]), int(match.groups()[1])+1))

                            # if it's not an interval, it must be a single node (right?)
                            else:
                                nodes = nodes + [int(element)]

                        # append cluster prefix to all node numbers and concatinate with commas
                        nodes = ",".join([cluster_prefix + str(node) for node in nodes])

                    # if there were no nodes
                    else:
                        nodes = ""

                    # add the virtual cluster name to bianca clusters
                    if cluster_name == 'bianca':
                        bianca_project_id = os.path.split(root)[1]
                        cluster = "{}.{}".format(cluster_name, bianca_project_id)

                    # store the job info
                    to_insert.append([date, cluster, jobid, proj_id, username, start, end, partition, procs, nodes, jobname, jobstate])

    # Tracer()()
    # insert the collected jobs
    db['slurm_accounting'][cluster_name]['cur'].executemany(u"INSERT OR REPLACE INTO slurm_accounting (date, cluster, job_id, proj_id, user, start, end, partition, cores, nodes, jobname, jobstate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_insert)

    # update the last_updated record (will not work well for biancas virtual clusters.. oh well, a problem for another day)
    db['slurm_accounting'][cluster_name]['cur'].execute("INSERT OR REPLACE INTO updated (table_name,date,date_human) VALUES (?,?,?);", ["slurm_accounting", updateStart.strftime('%s'), updateStart.strftime('%Y-%m-%d %H:%M:%S')])

    # save changes to db
    db['slurm_accounting'][cluster_name]['db'].commit()

    # reset
    to_insert = []









parser = argparse.ArgumentParser()
parser.add_argument("-s", "--slurm_accounting", help="path to the slurm_accounting folder", required=True)
parser.add_argument("-p", "--processes", type=int, help="the number of processes/threads to use when processing files", default=1)
args = parser.parse_args()

# save db root
db = {}
db['slurm_accounting'] = {'db_root':args.slurm_accounting}
# make sure the path exists
if not os.path.isdir(db['slurm_accounting']['db_root']):
    raise SystemExit("ERROR - slurm_accounting folder not found: {}".format(sys.argv[0], args['slurm_accounting']))


print("Starting the update using {} processes".format(args.processes))

# save the time the run started
updateStart = datetime.now()



# get the cluster names
clusters = [ name for name in os.listdir('/sw/share/slurm/') if os.path.isdir(os.path.join('/sw/share/slurm/', name)) ]


# connect to dbs
for cluster in clusters:
    db = connectDB(db, 'slurm_accounting', cluster)



# # devel non parallel
# for wp in work_packages:
#     fetch_new_jobs(wp)
# sys.exit()

# process the clusters in parallel
pool=Pool(processes=args.processes)
pool.map(fetch_new_jobs, clusters)
pool.close()
pool.join()

