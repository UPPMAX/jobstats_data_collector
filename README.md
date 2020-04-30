# Future Plan
* Get all jobstats files and slurm logs from all clusters to a central location
* Break most tables out to separate sqlite files
  * general jobinfo from the slurm logs
  * storage history
  * corehour history
  * project info
  * efficiency db as is
  * current project info
  * efficiency raw files as is

That way we can run all update scripts simultaiously without risking database locks. One db update == one cronjob and update script. The file size will be more managable and the processes easier to separate and understand. Adding new dbs will not require modifications of existing dbs etc.

* go through the tables and see which are actually used, also if anything more is needed
* Rewrite all scripts from scratch, but write them in a smart way this time


# Current setup

## general.sqlite
This db contains general information about jobs and projects.



### jobs
This table contains information gathered from SLURMs log files (/sw/share/slurm/cluster/accounting/)

Field | Description
--- | ---
date | Date the job finished
job_id | SLURM job id
proj_id | Project that ran the job
user | User that ran the job
start | Epoch time the job started
end | Epoch time the jobs ended
partition | Partition the job ran on
cores | Number of cores booked by job
cluster | Cluster the job ran on
nodes | The node(s) the job ran on
jobname | The name the user gave the job 
jobstate | The exit state of the job

**Indexes:** nodes-start, job_id, job_id-cluster, proj_id-cluster, proj_id-end-start-cluster, cluster-date
_______



### current_projects
This table contains a snapshot of projects usage as it looks right now. It's used to feed projman with precomputed numbers to speed things up.

Field | Description
--- | ---
proj_id | Project id
data | JSON formated data containing things like storage/corehour usage and activity, efficiency, project time limits etc
updated | Timestamp when the data was computed

**Indexes:** proj_id
_______



### storage_history
This table contains projects historic storage usage. One data point per project per day. (/sw/share/project_storage_stats)

Field | Description
--- | ---
proj_id | Project id
date | Date
usage | How much backed up space was used
quota | How larget the backed up quota was
path | Path to the backed up space
noback_usage | Nobackup usage
noback_quota | Nobackup quota
noback_path | Nobackup path
inbox | How much was stored in their inbox

**Indexes:** date, date-path, date-nobackup_path, proj_id-date, path, proj_id
_______



### corehour_history
This table contains projects historic corehour usage. One data point per project per day per cluster. (/sw/share/project_stats)

Field | Description
--- | ---
proj_id | Project id
date | Date
cluster | Which cluster the grant is on
grant | How many corehours are granted
30day_usage | The corehour usage the last 30 days

**Indexes:** cluster-date
_______



### corehour_monthly / corehour_weekly
This table contains projects summarized corehour usage. One data point per project per week/month per cluster. The summary is to avoid recalculating the sum of all jobs during a longer period. Used in scilifelab reports.

Field | Description
--- | ---
proj_id | Project id
month/week | The month/week the corehours were used
cluster | Which cluster the corehours were used on
used | The corehour usage during the specified time period

**Indexes:** proj_id-month/week-cluster
_______



### last_updated
This table contains timestamps for when each (major) table in the db was updated.

Field | Description
--- | ---
type | The table in that was updated
date | The date it was updated
time | The time it was updated

**Indexes:** None
_______



### projects
This table contains data about projects, parsed by the projects file (/sw/uppmax/etc/projects).

Field | Description
--- | ---
proj_id | Project id
start | Start date of the project
end | End date of the project
pi | Name of the PI
deputy | Name of the deputy (if any)
technical | Name of the technical (if any)
members | List of all project members
active | Y if the project is active (not expired) and N if it is not
type | Type of the project [uppmax,uppnex,snic,course]
mail | Email to the contact person of the project

**Indexes:** proj_id






## job_efficiency.sqlite
This database contains summarized efficiency statistics for all jobs run. Some of the data is redundant as it also exists in the general.sqlite, but is here as well to avoid 



### jobs
This table contains the efficiency statistics, one line per job.

Field | Description
--- | ---
job_id | Job id
cluster | The cluster the jobs was run on
proj_id | The project id
user | The user who ran the job
cpu_mean | The mean CPU usage
cpu_cores_used_percentile | The 75th percentile of cores used more than ~10%
cores | The number of cores booked by the job
mem_peak | The peak memory usage at any time during the job
mem_median | The median memory usage
mem_limit | The amount of memory available to use by the job
counts | The number of measurment point for the job (1 every 5 minutes of run time)
state | The jobs exit state
date_finished | The date the job finished
nodelist | The nodes the job ran on

**Indexes:** None
_______



### last_updated
This table contains timestamps for when each (major) table in the db was updated.

Field | Description
--- | ---
type | The table in that was updated
date | The date it was updated
time | The time it was updated

**Indexes:** None






## job_stats_raw
This database is a collection of databases, one per each node. This database sharding is done to be able to speed analysis up since the file access time can be limiting. It also enables easier parallellization by avoiding database locks when multiple processes try to write to the same db. By assigning different nodes to different processes collisions can be avoided.

### jobs
This table contains the raw resource usage statistics file for each job run on the node. Since the files are deleted from the file system every 30 days these databases were created to be able to recalculate everything from scratch if needed.

Field | Description
--- | ---
job_id | The id of the job
jobstats | The raw resource usage statistics as a blob

**Indexes:** job_id





## job_stats_raw - archive
These are not database per se, only plain text files. I figured that the more I process the datai, the larger the risk of me messing something up. These text files are simply a concatination of all resource usage statistics files that are created by all running jobs. They are divied into 1 text file per calendar month, where each job is in the file corresponding to the month the job finished running.













