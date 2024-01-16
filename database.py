from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import database_exists, create_database


class DatabaseManager:
    _engine = None
    _session = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            engine_url  = 'postgresql+psycopg2://postgres:0mn1c0ns4@postgres:5432/stream'
            if not database_exists(engine_url):
                create_database(engine_url)
            cls._engine = create_engine(engine_url)
        return cls._engine

    @classmethod
    def get_session(cls):
        if cls._session is None:
            engine = cls.get_engine()
            cls._session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        return cls._session

engine = DatabaseManager.get_engine()

Base = declarative_base()
Base.query = DatabaseManager.get_session().query_property()

shared_session = DatabaseManager.get_session()
