-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

-- Failed tracking does not stop desired-state cleanup. Absence proves deletion
-- completed even when the tracking budget was exhausted before the last cleanup.
UPDATE records AS operation SET phase='Succeeded', data=json_set(data,
 '$.phase','Succeeded', '$.message','Cluster deleted; cleanup completed after tracking failed',
 '$.completedAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'),
 '$.updatedAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'))
WHERE kind='operation' AND phase='Failed' AND json_extract(data,'$.kind')='delete'
 AND NOT EXISTS (SELECT 1 FROM clusters AS cluster
  WHERE cluster.namespace=coalesce(nullif(json_extract(operation.data,'$.namespace'),''),'default')
   AND cluster.name=json_extract(operation.data,'$.cluster')
   AND (coalesce(json_extract(operation.data,'$.clusterIncarnation'),'')=''
    OR json_extract(operation.data,'$.clusterIncarnation')=CAST(cluster.created AS TEXT)));

-- A terminal deletion cannot own Deleting inventory forever. Preserve its remote
-- target and disallow restore, but permit another idempotent deletion attempt.
UPDATE records AS inventory SET phase='Failed', data=json_set(data,
 '$.phase','Failed', '$.error','Backup deletion did not complete; repair the dependency and retry deletion')
WHERE kind='backup' AND phase='Deleting'
 AND EXISTS (SELECT 1 FROM records AS operation WHERE operation.kind='operation'
  AND operation.scope=inventory.scope AND operation.phase IN ('Failed','Cancelled')
  AND json_extract(operation.data,'$.kind')='delete-backup'
  AND json_extract(operation.data,'$.target.name')=inventory.key)
 AND NOT EXISTS (SELECT 1 FROM records AS operation WHERE operation.kind='operation'
  AND operation.scope=inventory.scope AND operation.phase IN ('Pending','Running')
  AND json_extract(operation.data,'$.kind')='delete-backup'
  AND json_extract(operation.data,'$.target.name')=inventory.key);
