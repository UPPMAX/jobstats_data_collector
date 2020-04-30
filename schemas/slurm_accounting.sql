DROP INDEX IF EXISTS "slurm_accounting-job_id-cluster";
DROP INDEX IF EXISTS "slurm_accounting-proj_id-cluster";
DROP INDEX IF EXISTS "slurm_accounting-proj_id-end-start-cluster";

DROP TABLE IF EXISTS slurm_accounting;
CREATE TABLE slurm_accounting (	"date" TEXT NOT NULL, 
								"cluster" TEXT NOT NULL, 
								"job_id" INTEGER NOT NULL, 
								"proj_id" TEXT NOT NULL, 
								"user" TEXT NOT NULL, 
								"start" INTEGER NOT NULL, 
								"end" INTEGER NOT NULL, 
								"partition" TEXT NOT NULL, 
								"cores" INTEGER NOT NULL, 
								"nodes" TEXT NOT NULL, 
								"jobname" TEXT, 
								"jobstate" TEXT NOT NULL, 
								PRIMARY KEY (job_id, cluster) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL,
						"date_human" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "slurm_accounting-job_id-cluster" on slurm_accounting (job_id, cluster);
CREATE INDEX "slurm_accounting-proj_id-cluster" on slurm_accounting (proj_id, cluster);
CREATE INDEX "slurm_accounting-proj_id-end-start-cluster" on slurm_accounting (proj_id, end, start, cluster);
CREATE INDEX "slurm_accounting-end-cluster" on slurm_accounting (end, cluster);
CREATE INDEX "slurm_accounting-end" on slurm_accounting (end);