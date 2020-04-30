DROP INDEX IF EXISTS "corehour_history-date-cluster";

DROP TABLE IF EXISTS corehour_history;
CREATE TABLE corehour_history (	"date" TEXT NOT NULL, 
								"proj_id" TEXT NOT NULL, 
								"cluster" TEXT NOT NULL, 
								"grant" INTEGER NOT NULL, 
								"30day_usage" REAL NOT NULL, 
								PRIMARY KEY (date, proj_id) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "corehour_history-date-cluster" on corehour_history (date, cluster);