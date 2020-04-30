DROP INDEX IF EXISTS "efficiency_rawdata-job_id_node";
DROP INDEX IF EXISTS "efficiency_rawdata-job_id";

DROP TABLE IF EXISTS efficiency_rawdata;
CREATE TABLE "efficiency_rawdata"(
  "job_id" INTEGER NOT NULL,
  "node" TEXT NOT NULL,
  "jobstats" TEXT NOT NULL,
  PRIMARY KEY (job_id, node) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL,
						"date_human" TEXT NOT NULL
);

CREATE INDEX "efficiency_rawdata-job_id_node" on efficiency_rawdata (job_id, node);
CREATE INDEX "efficiency_rawdata-job_id" on efficiency_rawdata (job_id);
