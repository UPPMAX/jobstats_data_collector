DROP INDEX IF EXISTS "projman_info-proj_id";

DROP TABLE IF EXISTS projman_info;
CREATE TABLE projman_info (	"proj_id" TEXT PRIMARY KEY NOT NULL, 
							"data" TEXT NOT NULL, 
							"updated" TEXT NOT NULL 
);


DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL,
						"date_human" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "projman_info-proj_id" on projman_info (proj_id);