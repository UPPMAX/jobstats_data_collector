DROP INDEX IF EXISTS "projects-proj_id";

DROP TABLE IF EXISTS projects;
CREATE TABLE projects (	"proj_id" TEXT NOT NULL, 
						"start" TEXT NOT NULL, 
						"end" TEXT NOT NULL, 
						"pi" TEXT NOT NULL, 
						"deputy" TEXT NOT NULL, 
						"technical" TEXT NOT NULL, 
						"members" TEXT NOT NULL, 
						"active" TEXT NOT NULL, 
						"type" TEXT NOT NULL, 
						"mail" TEXT NOT NULL, 
						PRIMARY KEY (proj_id) 
);

DROP TABLE IF EXISTS updated;
CREATE TABLE updated (	"table_name" TEXT PRIMARY KEY NOT NULL, 
						"date" TEXT NOT NULL
);

VACUUM;

-- indexes, check if needed
CREATE INDEX "projects-proj_id" on projects (proj_id);