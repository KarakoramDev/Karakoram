-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

UPDATE records SET data=json_set(data, '$.target', json_object(
  'kind', CASE
    WHEN coalesce(json_extract(data,'$.payload.backup'),'')<>'' THEN 'backup'
    WHEN coalesce(json_extract(data,'$.payload.restore'),'')<>'' THEN 'restore'
    WHEN coalesce(json_extract(data,'$.payload.member'),'')<>'' THEN 'member'
    WHEN coalesce(json_extract(data,'$.payload.user'),'')<>'' THEN 'system-user'
    ELSE 'cluster' END,
  'name', coalesce(nullif(json_extract(data,'$.payload.backup'),''), nullif(json_extract(data,'$.payload.restore'),''),
    nullif(json_extract(data,'$.payload.member'),''), nullif(json_extract(data,'$.payload.user'),''), json_extract(data,'$.cluster'))))
WHERE kind='operation' AND coalesce(json_extract(data,'$.target.name'),'')='';

UPDATE records SET data=json_set(data, '$.message', coalesce(json_extract(data,'$.kind'),'operation') || CASE phase
  WHEN 'Succeeded' THEN ' completed successfully'
  WHEN 'Cancelled' THEN ' cancelled'
  ELSE ' failed; inspect cluster conditions and events' END)
WHERE kind='operation' AND phase IN ('Succeeded','Failed','Cancelled') AND coalesce(json_extract(data,'$.message'),'')='';

UPDATE records AS inventory SET data=json_set(data, '$.operationId',
 (SELECT operation.key FROM records AS operation WHERE operation.kind='operation' AND operation.scope=inventory.scope
  AND json_extract(operation.data,'$.kind')=inventory.kind AND json_extract(operation.data,'$.target.name')=inventory.key
  ORDER BY operation.created DESC,operation.key DESC LIMIT 1))
WHERE kind IN ('backup','restore') AND coalesce(json_extract(data,'$.operationId'),'')=''
 AND EXISTS (SELECT 1 FROM records AS operation WHERE operation.kind='operation' AND operation.scope=inventory.scope
  AND json_extract(operation.data,'$.kind')=inventory.kind AND json_extract(operation.data,'$.target.name')=inventory.key);

UPDATE records AS inventory SET phase='Failed', data=json_set(data, '$.phase','Failed', '$.error',
 (SELECT json_extract(operation.data,'$.message') FROM records AS operation WHERE operation.kind='operation' AND operation.key=json_extract(inventory.data,'$.operationId')),
 '$.completedAt', (SELECT coalesce(json_extract(operation.data,'$.completedAt'), json_extract(operation.data,'$.updatedAt')) FROM records AS operation WHERE operation.kind='operation' AND operation.key=json_extract(inventory.data,'$.operationId')))
WHERE kind IN ('backup','restore') AND phase IN ('Pending','Running')
 AND EXISTS (SELECT 1 FROM records AS operation WHERE operation.kind='operation' AND operation.key=json_extract(inventory.data,'$.operationId') AND operation.phase IN ('Failed','Cancelled'));
