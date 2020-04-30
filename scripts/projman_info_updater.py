#!/bin/env python3.6.7
# -*- coding: utf-8 -*-

# A script to summarize projects resource usage to determin if they can be extended or are running efficiently
#
# Usage: python script.py <path to efficiency folder> <path to slurm_accounting.sqlite>





import sqlite3
import sys
import os
from datetime import datetime, timedelta, date
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
import collections
import json
import urllib.request


# from IPython.core.debugger import Tracer







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

    # Tracer()()

    # check if the file exists and has a size > 0
    if not os.path.isfile("{}/{}.sqlite".format(db[type]['root'], cluster)):
        # if os.stat("{}/{}.sqlite".format(db['db_root'], cluster)).st_size > 0:
        #     # try repairing it if it does
        #     print("Trying to repair {}/{}.sqlite".format(db['db_root'], cluster))
        #     subprocess.call(["{}/repair_sqlite3db.sh".format(db['db_root']), "{}/{}.sqlite".format(db['db_root'], cluster)])
        # else:
        #     replace_db(db['db_root'], cluster)
    # else:
        create_db(db[type]['root'], cluster)

    db[type][cluster] = {}
    db[type][cluster]['db'] = sqlite3.connect("{}/{}.sqlite".format(db[type]['root'], cluster))
    db[type][cluster]['db'].row_factory = sqlite3.Row
    db[type][cluster]['cur'] = db[type][cluster]['db'].cursor()
    return db



def create_db(db_root, cluster):

    print("Creating new db: {}/{}.sqlite".format(os.path.normpath(db_root), cluster))
    # shutil.move("{}/{}".format(db_root, cluster), "{}/{}.corrupt".format(db_root, cluster))
    shutil.copy("{}/empty_shard".format(db_root), "{}/{}.sqlite".format(db_root, cluster))




# get the data when the database was last updated
def get_last_updated(db, type, cluster):

    # check if the projects has ever been updated
    query = "SELECT date FROM updated WHERE table_name='slurm_accounting';"
    db[type][cluster]['cur'].execute(query)
    result = db[type][cluster]['cur'].fetchone()
    

    # if it has never been updated before, i.e. it's the first run
    if not result:

        # initiate the record
        query = "INSERT INTO updated (table_name, date, date_human) VALUES ('slurm_accounting', 0, '1970-01-01 00:00:00');"
        db[type][cluster]['cur'].execute(query)
        return 0


    # return the date
    return result[0]





# @profile
def get_corehour_activity(db, type, cluster, corehour_activity):

    # init
    past_6m = datetime.now() - timedelta(days=182)

    # fetch all jobs from the past 6 months
    query = "SELECT * FROM efficiency WHERE date_finished>=?"
    results = db[type][cluster]['cur'].execute(query, [past_6m.strftime('%Y-%m-%d')])

    # init
    nested_dict = lambda: collections.defaultdict(nested_dict)
    collected_jobs = nested_dict()


    # go through all the jobs and summarize them per day. Wait with summarizing per week to avoid using slow datetime functions for every single job
    for i,res in enumerate(ResultIter(db[type][cluster]['cur'])):

        # Tracer()()

        # roughly estimate the jobs core hour usage. Since entries are made every 5 minutes, it gives a decent estimate for the used core hours
        corehours_used = res['cores']  *  res['entries'] / 12

        # determin the job efficiency, as the max of either core usage or memory usage
        job_efficiency = max(res['cpu_mean']/100,  res['mem_peak']/res['mem_limit'])

        # add the job's corehours and efficiency to the pot
        try:
            collected_jobs[res['proj_id']][res['date_finished']]['corehours'] += corehours_used
            collected_jobs[res['proj_id']][res['date_finished']]['efficiency'] += corehours_used * job_efficiency
        except TypeError:
            collected_jobs[res['proj_id']][res['date_finished']]['corehours'] = 0
            collected_jobs[res['proj_id']][res['date_finished']]['efficiency'] = 0
            collected_jobs[res['proj_id']][res['date_finished']]['corehours'] += corehours_used
            collected_jobs[res['proj_id']][res['date_finished']]['efficiency'] += corehours_used * job_efficiency

        # save the date of the last job
        try:
            if collected_jobs[res['proj_id']]['last_job_date'] < res['date_finished']:
                collected_jobs[res['proj_id']]['last_job_date'] = res['date_finished']
        except TypeError:
            # first time seeing this project
            collected_jobs[res['proj_id']]['last_job_date'] = res['date_finished']


    # summarize the usage by constructing an array with as many elements as weeks in a half year and filling each element with the jobs that finished during that week
    now = datetime.now()
    for proj in collected_jobs:

        # Tracer()()

        # init the week arrays to calculate weekly means
        corehour_usage = [0] * 27 # 27 weeks in ~6 months
        corehour_efficiency = [0] * 27 # 27 weeks in ~6 months

        # save the date of the last job
        corehour_activity[cluster][proj]['last_job_date'] = collected_jobs[proj]['last_job_date']
        

        # for each day with finished jobs for this proj, save the usage and efficiency to be able to summarize to means in the next step
        for day in collected_jobs[proj]:
            # Tracer()()

            # skip the job date key
            if day == 'last_job_date':
                continue
                
            # get how many weeks ago this day was
            week = int((now - datetime.strptime(day, '%Y-%m-%d')).days/7)


            # save the usage and efficiency
            corehour_usage[week] += collected_jobs[proj][day]['corehours']
            corehour_efficiency[week] += collected_jobs[proj][day]['efficiency'] 

        
        # Tracer()()
        # calculate the weekly means
        corehour_activity[cluster][proj]['corehours'] = [dict()] * 27 # 27 weeks in ~6 months
        corehour_activity[cluster][proj]['efficiency'] = [dict()] * 27 # 27 weeks in ~6 months
        for week in range(len(corehour_usage)):

            # Tracer()()

            # catch empty weeks and set them to zero
            if corehour_usage[week] == 0:
                corehour_activity[cluster][proj]['corehours'][week] = 0
                corehour_activity[cluster][proj]['efficiency'][week] = 0
                continue

            corehour_activity[cluster][proj]['corehours'][week] = corehour_usage[week]
            corehour_activity[cluster][proj]['efficiency'][week] = corehour_efficiency[week] / corehour_usage[week]


    # Tracer()()

    return corehour_activity








def convert_to_projman(corehour_activity, storage_activity, projects):

    # init
    current = {}

    # process the corehour data first
    for cluster in corehour_activity:

        for proj in corehour_activity[cluster]:
            
            try:
                current[proj]['corehour'][cluster] = {}
            except:
                current[proj] = {}
                current[proj]['corehour'] = {}
                current[proj]['corehour'][cluster] = {}

            
            # the 6 month activity is the number of week that have any corehour usage
            # Tracer()()
            current[proj]['corehour'][cluster]['6mactivity'] = sum(week > 0 for week in corehour_activity[cluster][proj]['corehours'])


            # the 1 week efficiency is the efficiency for all jobs run the last week
            current[proj]['corehour'][cluster]['1wusage'] = corehour_activity[cluster][proj]['corehours'][0]
            current[proj]['corehour'][cluster]['1wefficiency'] = corehour_activity[cluster][proj]['efficiency'][0]
            if current[proj]['corehour'][cluster]['1wusage'] == 0:
            	current[proj]['corehour'][cluster]['1wefficiency'] = 1

            
            # the 1 month efficiency is the average efficiency of all jobs run during the past 4 weeks
            # Tracer()()
            corehours = 0
            efficiency = 0
            for week in range(4):
                corehours += corehour_activity[cluster][proj]['corehours'][week]
                efficiency += corehour_activity[cluster][proj]['efficiency'][week] * corehour_activity[cluster][proj]['corehours'][week]
            current[proj]['corehour'][cluster]['1musage'] = corehours
            if corehours > 0:
                current[proj]['corehour'][cluster]['1mefficiency'] = efficiency / corehours
            else:
                # if no jobs are run
                current[proj]['corehour'][cluster]['1mefficiency'] = 1


            # the 6 month efficiency is the average efficiency of all jobs run during the past 26 weeks
            # Tracer()()
            corehours = 0
            efficiency = 0
            for week in range(len(corehour_activity[cluster][proj]['corehours'])):
                corehours += corehour_activity[cluster][proj]['corehours'][week]
                efficiency += corehour_activity[cluster][proj]['efficiency'][week] * corehour_activity[cluster][proj]['corehours'][week]
            current[proj]['corehour'][cluster]['6musage'] = corehours
            if corehours > 0:
                current[proj]['corehour'][cluster]['6mefficiency'] = efficiency / corehours
            else:
                # if no jobs are run
                current[proj]['corehour'][cluster]['6mefficiency'] = 1


            # the allocation is the number of corehours the project has allocated on this cluster
            current[proj]['corehour'][cluster]['allocation'] = 0
            for cluster_allocation in projects[proj]['Allocations']:

                # find the current cluster and save the allocation value
                if cluster_allocation['Resource'] == cluster:
                    current[proj]['corehour'][cluster]['allocation'] = cluster_allocation['Value']


            # the last_job_date is the number of weeks ago the last job was run by the project
            current[proj]['corehour'][cluster]['last_job_date'] = corehour_activity[cluster][proj]['last_job_date']
            

            # if proj == 'snic2017-7-274':
            #     Tracer()()







    # TODO when the stats portal is up
    # process the storage data












    # process the general data
    for proj in projects:

        # save the values
        #Tracer()()
        try:
            current[proj]['general'] = {}
        except KeyError:
            current[proj] = {}
            current[proj]['general'] = {}

        current[proj]['general']['start_date'] = projects[proj]['Start']
        current[proj]['general']['end_date'] = projects[proj]['End']
        current[proj]['general']['pi'] = projects[proj].get('Principal', {})
        current[proj]['general']['deputy'] = projects[proj].get('Deputy', {})
        current[proj]['general']['technical'] = projects[proj].get('Technical', {})
        current[proj]['general']['type'] = projects[proj].get('Type', {})
        current[proj]['general']['title'] = projects[proj].get('Title', "")
        current[proj]['general']['vr_class'] = projects[proj].get('Class', "")
        current[proj]['general']['raw'] = projects[proj]


        # # fill in the other keys if they are missing
        # if 'storage' not in current[proj]:
        #     current[proj]['storage'] = {}

        # if 'corehours' not in current[proj]:
        #     current[proj]['corehours'] = {}






    return current







def insert(db, current):

    now = datetime.now()

    # go through the projects
    to_insert = []
    for proj in sorted(current):
        # if proj == 'snic2017-7-274':
        #         Tracer()()
        to_insert.append([proj, json.dumps(current[proj]), now.strftime('%Y-%m-%d %H:%M:%S')])
    
    # insert the data
    db['projman']['cur'].executemany("INSERT OR REPLACE INTO projman_info (proj_id,data,updated) VALUES (?,?,?)", to_insert)

    # update the update date
    db['projman']['cur'].execute("INSERT OR REPLACE INTO updated (table_name,date,date_human) VALUES (?,?,?)", ['projman_info', now.strftime('%s'), now.strftime('%Y-%m-%d %H:%M:%S')])

    # save the changes
    db['projman']['db'].commit()








# @profile
def main():



    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--projman_info_db", help="path to the projman_info database", required=True)
    parser.add_argument("-e", "--efficiency", help="path to the efficiency folder", required=True)
    # parser.add_argument("-p", "--processes", type=int, help="the number of processes/threads to use when processing jobs", default=1)
    args = parser.parse_args()



    # get the efficiency path
    db = {'efficiency': {'root': args.efficiency}}

    # make sure the path exists
    if not os.path.isdir(db['efficiency']['root']):
        raise SystemExit("ERROR - efficiency database root path not found: {} - {}".format(sys.argv[0], db['efficiency']['root']))

    

    # get the projman_info path
    db['projman'] = {'path': args.projman_info_db}
    # make sure the path exists
    if not os.path.isfile(db['projman']['path']):
        raise SystemExit("ERROR - projman database root path not found: {} - {}".format(sys.argv[0], db['projman']['path']))
    db['projman']['db'] = sqlite3.connect(db['projman']['path'])
    db['projman']['db'].row_factory = sqlite3.Row
    db['projman']['cur'] = db['projman']['db'].cursor()



    # save the time the run started
    updateStart = time.time()



    # get the cluster names
    clusters = [ name for name in os.listdir('/sw/share/slurm/') if os.path.isdir(os.path.join('/sw/share/slurm/', name)) ]
    
    # init
    nested_dict = lambda: collections.defaultdict(nested_dict)
    corehour_activity = nested_dict()

    # connect to dbs
    for cluster in clusters:

        print('Fetching recent {} jobs'.format(cluster))

        # connect to the db
        db = connectDB(db, 'efficiency', cluster)
        
        # get the cluster activity
        corehour_activity = get_corehour_activity(db, 'efficiency', cluster, corehour_activity)

    # Tracer()()
    # TODO when the stats portal is live
    print("Fetching storage information")
    storage_activity = nested_dict()



    # fetch project allocations
    print("Fetching general project info from SUP API")
    req = urllib.request.Request('http://api.uppmax.uu.se:5000/api/v1/projects')
    with urllib.request.urlopen(req) as response:
        projects = json.loads(response.read().decode('utf-8'))


    # convert the data to a projman formatted dict
    print("Converting data to projman format")
    current = convert_to_projman(corehour_activity, storage_activity, projects)
    # Tracer()()
    # insert the data to the db
    print("Inserting data to projman database")
    insert(db, current)


if __name__ == '__main__':
    main()
