DROP INDEX IF EXISTS "efficiency-job_id-cluster";
DROP INDEX IF EXISTS "efficiency-proj_id-end-start-cluster";

DROP TABLE IF EXISTS efficiency;
CREATE TABLE efficiency (	"cluster" TEXT NOT NULL, 
							"job_id" INTEGER NOT NULL, 
							"proj_id" TEXT NOT NULL, 
							"user" TEXT NOT NULL, 
							"nodelist" TEXT NOT NULL, 
							"cpu_mean" REAL NOT NULL, 
							"cpu_cores_used_percentile" REAL NOT NULL, 
							"cores" REAL NOT NULL, 
							"mem_peak" REAL NOT NULL, 
							"mem_median" REAL NOT NULL, 
							"mem_limit" REAL, 
							"entries" REAL NOT NULL, 
							"state" TEXT NOT NULL, 
							"date_finished" TEXT NOT NULL, 
							PRIMARY KEY (cluster, job_id) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL,
						"date_human" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "efficiency-job_id-cluster" on efficiency (cluster, job_id);