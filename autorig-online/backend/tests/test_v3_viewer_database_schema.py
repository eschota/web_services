import unittest

from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateTable

import database


class V3ViewerTaskVisibilitySchemaTests(unittest.TestCase):
    def test_task_model_has_nonnullable_public_default(self):
        column = database.Task.__table__.c.is_public
        self.assertFalse(column.nullable)
        self.assertIsNotNone(column.server_default)
        self.assertEqual(str(column.server_default.arg).upper(), "TRUE")

    def test_fresh_sqlite_and_postgres_task_tables_include_visibility(self):
        sqlite_ddl = str(CreateTable(database.Task.__table__).compile(dialect=sqlite.dialect()))
        postgres_ddl = str(CreateTable(database.Task.__table__).compile(dialect=postgresql.dialect()))
        self.assertIn("is_public BOOLEAN DEFAULT TRUE NOT NULL", sqlite_ddl)
        self.assertIn("is_public BOOLEAN DEFAULT TRUE NOT NULL", postgres_ddl)

    def test_existing_database_migrations_are_guarded(self):
        import inspect

        source = inspect.getsource(database.init_db)
        self.assertIn("ALTER TABLE tasks ADD COLUMN is_public BOOLEAN NOT NULL DEFAULT 1", source)
        self.assertIn("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS is_public BOOLEAN NOT NULL DEFAULT TRUE", source)


if __name__ == "__main__":
    unittest.main()
