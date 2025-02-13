import logging
from typing import Union

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils import create_database, database_exists

from .schema import Base

class PostgresDatabase:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: Union[str, int],
        db: str,
        is_async: bool = False
    ) -> None:
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)

        dialect = "postgresql+asyncpg" if is_async else "postgresql+psycopg2"
        db_uri = f"{dialect}://{username}:{password}@{host}:{port}/{db}"
        self.db_url = db_uri
        if not database_exists(db_uri):
            create_database(db_uri)

        self.logger.info("creating database with uri: {}".format(db_uri))
        self.engine = create_engine(db_uri, pool_size=15, max_overflow=5)
        self.session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        return self.session()

    def disconnect(self) -> None:
        self.session.close_all()
        self.logger.info(f"Disconnect from PGVector: {self.host}:{self.port}")

if __name__ == "__main__":
    db = PostgresDatabase(
        username="airflow",
        password="airflow",
        host="172.17.0.1",
        port=5432,
        db="danielsyahputra"
    )
    print(db)