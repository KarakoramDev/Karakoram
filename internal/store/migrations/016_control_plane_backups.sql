CREATE TABLE control_plane_backups (
 id TEXT PRIMARY KEY,
 phase TEXT NOT NULL,
 created INTEGER NOT NULL,
 data BLOB NOT NULL
);
CREATE UNIQUE INDEX control_plane_backups_active ON control_plane_backups ((1)) WHERE phase IN ('Pending','Running');
CREATE INDEX control_plane_backups_created ON control_plane_backups(created DESC,id DESC);
