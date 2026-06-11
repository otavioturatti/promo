"""Fakes e utilitários para testes sem banco/HTTP reais."""
from contextlib import contextmanager

from psycopg2 import sql


class FakeCursor:
    """Cursor que captura execute() e devolve resultados pré-definidos."""

    def __init__(self, fetch_result=None):
        self.executed = []  # lista de (query, params)
        self._fetch_result = fetch_result if fetch_result is not None else []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._fetch_result

    def fetchone(self):
        return self._fetch_result[0] if self._fetch_result else None

    @property
    def rowcount(self):
        return len(self._fetch_result)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, *a, **k):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


@contextmanager
def fake_conn_cm(cursor):
    """Substituto para database.get_conn() nos testes."""
    yield FakeConn(cursor)


def identifiers_in(query) -> set:
    """Nomes de tabela (Identifier) presentes numa query sql.Composed/Identifier."""
    names = set()
    if isinstance(query, sql.Composed):
        for part in query.seq:
            if isinstance(part, sql.Identifier):
                names.update(part.strings)
    elif isinstance(query, sql.Identifier):
        names.update(query.strings)
    return names
