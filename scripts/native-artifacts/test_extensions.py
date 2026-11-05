#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from extensions import PINS, install_previous_sql, pgrx_upgrade_sql


class UpgradeSQLTests(unittest.TestCase):
    def test_source_release_and_sql_default_versions_are_distinct(self):
        for name, release, sql in (
                ('pg_net', '0.20.5', '0.20.4'),
                ('plpgsql_check', '2.8.11', '2.8'),
                ('pg_hashids', '1.3.0', '1.3')):
            with self.subTest(extension=name):
                self.assertEqual(PINS[name]['version'], release)
                self.assertEqual(PINS[name]['sqlVersion'], sql)

    def test_refreshes_functions_without_dropping_dependents(self):
        source = '''CREATE  FUNCTION "json_matches_schema"() RETURNS bool LANGUAGE c;
create function graphql.comment_directive(comment_ text) returns jsonb language sql;
create or replace function graphql.resolve() returns jsonb language sql;
create sequence if not exists graphql.seq_schema_version as int cycle;
create event trigger graphql_watch_ddl
    on ddl_command_end
    execute procedure graphql.increment_schema_version();
create event trigger graphql_watch_drop
    on sql_drop
    execute procedure graphql.increment_schema_version();
'''
        result = pgrx_upgrade_sql(source)
        self.assertEqual(result.lower().count('create or replace function'), 3)
        self.assertNotIn('create event trigger', result)
        self.assertNotIn('DROP', result)
        self.assertIn('create sequence if not exists', result)
        self.assertEqual(pgrx_upgrade_sql(result), result)

    def test_previous_sql_keeps_current_install_and_upgrade_scripts(self):
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode='w') as baseline:
            for name in ('pg_net--0.20.3.sql', 'pg_net--0.20.4.sql',
                         'pg_net--0.20.3--0.20.4.sql', 'pgjwt--0.2.0.sql'):
                content = b'previous install SQL\n'
                member = tarfile.TarInfo('./usr/share/postgresql/17/extension/'+name)
                member.size = len(content)
                baseline.addfile(member, io.BytesIO(content))
        payload.seek(0)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            sql = output/'usr/share/postgresql/17/extension'
            sql.mkdir(parents=True)
            (sql/'pg_net--0.20.4.sql').write_text('current install SQL\n')
            with tarfile.open(fileobj=payload) as baseline, patch('extensions.tarfile.open', return_value=baseline):
                install_previous_sql('pg_net', output)
            self.assertEqual((sql/'pg_net--0.20.3.sql').read_text(), 'previous install SQL\n')
            self.assertEqual((sql/'pg_net--0.20.4.sql').read_text(), 'current install SQL\n')
            self.assertEqual(sorted(path.name for path in sql.iterdir()),
                             ['pg_net--0.20.3.sql', 'pg_net--0.20.4.sql'])


if __name__ == '__main__':
    unittest.main()
