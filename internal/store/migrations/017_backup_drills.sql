CREATE TABLE backup_drills (
 id TEXT PRIMARY KEY,
 namespace TEXT NOT NULL,
 cluster TEXT NOT NULL,
 phase TEXT NOT NULL,
 trigger TEXT NOT NULL,
 created INTEGER NOT NULL,
 updated INTEGER NOT NULL,
 report BLOB NOT NULL
);
CREATE INDEX backup_drills_cluster_created ON backup_drills(namespace,cluster,created DESC,id DESC);
CREATE INDEX backup_drills_active ON backup_drills(created,id) WHERE phase NOT IN ('Succeeded','Failed');
