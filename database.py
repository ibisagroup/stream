from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base

class DatabaseManager:
    _engine = None
    _session = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            cls._engine = create_engine('sqlite:///stream.sqlite3')
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
