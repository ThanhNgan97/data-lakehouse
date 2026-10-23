"""Idempotent additive migration for local and demo PostgreSQL databases."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import models  # noqa: F401 - register ORM metadata
from db.database import Base, engine
from db.schema_migrations import apply_additive_migrations


def migrate() -> None:
    Base.metadata.create_all(bind=engine)
    apply_additive_migrations(engine)
    print("Database migration completed.")


if __name__ == "__main__":
    migrate()
