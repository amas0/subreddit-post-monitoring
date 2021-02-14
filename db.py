import sqlite3
from dataclasses import dataclass
from pathlib import Path

from submission import SubmissionMeta, SubmissionTemporalData


@dataclass(frozen=True)
class DBColumn:
    name: str
    type: str
    options: str = ''

    @property
    def create_statement(self) -> str:
        return ' '.join((self.name, self.type, self.options)).strip()


@dataclass(frozen=True)
class DBTable:
    name: str
    schema: list[DBColumn]
    options: str = ''

    @property
    def create_statement(self) -> str:
        params = ',\n'.join([col.create_statement for col in self.schema] + [self.options])
        params = params.strip().rstrip(',')
        statement = f'CREATE TABLE {self.name} ({params});'
        return statement


SUBMISSION_TABLE = DBTable(name='submissions', schema=[
    DBColumn(name='id', type='TEXT', options='PRIMARY KEY'),
    DBColumn(name='author', type='TEXT', options='NOT NULL'),
    DBColumn(name='created_utc', type='INTEGER', options='NOT NULL'),
    DBColumn(name='name', type='TEXT', options='NOT NULL'),
    DBColumn(name='permalink', type='TEXT', options='NOT NULL'),
    DBColumn(name='selftext', type='TEXT'),
    DBColumn(name='title', type='TEXT', options='NOT NULL')
])
STATS_TABLE = DBTable(name='stats', schema=[
    DBColumn(name='submission_id', type='TEXT', options='NOT NULL'),
    DBColumn(name='time_utc', type='INTEGER', options='NOT NULL'),
    DBColumn(name='ups', type='INTEGER', options='NOT NULL'),
    DBColumn(name='downs', type='INTEGER', options='NOT NULL'),
    DBColumn(name='num_comments', type='INTEGER', options='NOT NULL'),
], options='PRIMARY KEY (submission_id, time_utc)')


class MonitoringDB:
    SUBMISSION_TABLE = SUBMISSION_TABLE
    STATS_TABLE = STATS_TABLE

    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.validate_or_create_tables()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
            print(exc_type)
        else:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()

    @property
    def tables(self) -> set[str]:
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return {x[0] for x in self.cursor.fetchall()}

    def validate_or_create_tables(self) -> None:
        """Checks for the existence of defined tables, otherwise creates them"""
        tables = self.tables
        if self.SUBMISSION_TABLE.name not in tables:
            self.cursor.execute(self.SUBMISSION_TABLE.create_statement)
        if self.STATS_TABLE.name not in tables:
            self.cursor.execute(self.STATS_TABLE.create_statement)

    def get_submission_by_id(self, submission_id: str):
        self.cursor.execute("SELECT * FROM submissions where id = ?",
                            (submission_id,))
        if db_response := self.cursor.fetchone():
            return SubmissionMeta(*db_response)
        else:
            return None

    def insert_submission(self, sub: SubmissionMeta) -> None:
        if self.get_submission_by_id(sub.id) is None:
            self.cursor.execute("INSERT INTO submissions VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (sub.id, sub.author, sub.created_utc, sub.name, sub.permalink,
                                 sub.selftext, sub.title))
