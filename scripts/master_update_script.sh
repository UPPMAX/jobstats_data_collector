#!/bin/sh
set -ex

# set python version
# alias python="/sw/comp/python/2.7.6_rackham_new/bin/python"

# source config file
#source ../settings.conf

export RAMDRIVE_PATH="/dev/shm/compstore/"
export NETWORK_PATH="/sw/share/compstore/production/statistics"

# copy database to ramdrive
mkdir -p $RAMDRIVE_PATH
rsync -ar $NETWORK_PATH/dbs/* $RAMDRIVE_PATH

# chown -R valent:staff $RAMDRIVE_PATH
# chmod -R 770 $RAMDRIVE_PATH 

# update slurm_accounting
python $NETWORK_PATH/scripts/slurm_accounting_updater.py -s $RAMDRIVE_PATH/slurm_accounting/ -p 30

# update collected_jobstats
python $NETWORK_PATH/scripts/collect_jobstats.py -j $RAMDRIVE_PATH/collected_jobstats/ -s $RAMDRIVE_PATH/slurm_accounting/ -p 40

# update efficiency
python $NETWORK_PATH/scripts/efficiency_updater.py -j $RAMDRIVE_PATH/collected_jobstats/ -s $RAMDRIVE_PATH/slurm_accounting/ -e $RAMDRIVE_PATH/efficiency/ -p 30

# update projman_info
python $NETWORK_PATH/scripts/projman_info_updater.py -d $RAMDRIVE_PATH/projman_info.sqlite -e $RAMDRIVE_PATH/efficiency/





# copy the files back to network location

MAX_RETRIES=10
i=0

# Set the initial return code to failure
RC=1

# continue as long as the previous rsync failed and the max # retrys has not been reached yet
while [ $RC -ne 0 -a $i -lt $MAX_RETRIES ]
do
	i=$(($i+1))
	rsync -ar --remove-source-files $RAMDRIVE_PATH/* $NETWORK_PATH/dbs.staged/
	RC=$?
done

if [ $i -eq $MAX_RETRIES ]
then
	echo "Hit maximum number of retries, giving up."
	exit
fi


# if the script has not exited yet, it must have succeeded, right?
mv $NETWORK_PATH/dbs $NETWORK_PATH/dbs.toDelete
mv $NETWORK_PATH/dbs.staged $NETWORK_PATH/dbs
rm -r $NETWORK_PATH/dbs.toDelete
