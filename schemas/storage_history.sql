DROP INDEX IF EXISTS "storage_history-date";
DROP INDEX IF EXISTS "storage_history-path";
DROP INDEX IF EXISTS "storage_history-proj_id";
DROP INDEX IF EXISTS "storage_history-date-path";
DROP INDEX IF EXISTS "storage_history-date-nobackup_path";
DROP INDEX IF EXISTS "storage_history-proj_id-path";

DROP TABLE IF EXISTS storage_history;
CREATE TABLE storage_history (	"date" TEXT NOT NULL, 
								"proj_id" TEXT NOT NULL, 
								"usage" REAL NOT NULL, 
								"quota" REAL NOT NULL, 
								"path" TEXT NOT NULL, 
								"nobackup_usage" REAL NOT NULL, 
								"nobackup_quota" REAL NOT NULL, 
								"nobackup_path" TEXT NOT NULL,
								PRIMARY KEY (date, proj_id) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "storage_history-date" on storage_history (date);
CREATE INDEX "storage_history-path" on storage_history (path);
CREATE INDEX "storage_history-proj_id" on storage_history (proj_id);
CREATE INDEX "storage_history-date-path" on storage_history (date, path);
CREATE INDEX "storage_history-date-nobackup_path" on storage_history (date, nobackup_path);
CREATE INDEX "storage_history-proj_id-path" on storage_history (proj_id, path);