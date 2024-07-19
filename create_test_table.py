import logging
import os

from sqlalchemy import create_engine, Column, Sequence, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import DatabaseError

from sqlalchemy_interbase import *

# Enable logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

relative_path = 'TEST.DB'
database_path = os.path.join(os.getcwd(), relative_path)
connection_string = f"interbase://sysdba:masterkey@localhost:3050/{database_path}?charset=WIN1252"
engine = create_engine(connection_string, echo=True)

# Create session class
Session = sessionmaker(bind=engine)

session = Session()

try:
    result = session.execute(text("SELECT RTRIM(RDB$CHARACTER_SET_NAME) FROM RDB$DATABASE"))
    charset_name = result.scalar()
except DatabaseError as err:
    # TODO: install UDF
    print(err)
finally:
    session.close()

Base = declarative_base()


class IBase(Base):
    __abstract__ = True
    __table_args__ = {'implicit_returning': False}


class Sampletable(Base):
    __tablename__ = 'SAMPLETABLE2'
    id = Column(IBINTEGER, default=Sequence(f'{__tablename__}_id_gen'), primary_key=True, nullable=False, autoincrement=True)
    ib_blob = Column(IBBLOB(segment_size=80))
    ib_bool = Column(IBBOOLEAN())
    ib_cstring = Column(IBVARCHAR(length=10, charset='WIN1252', collation='WIN1252'))
    ib_date = Column(IBDATE())
    ib_double = Column(IBDOUBLE_PRECISION())
    ib_float = Column(IBFLOAT())
    ib_long = Column(IBINTEGER())
    ib_short = Column(IBSMALLINT())
    ib_text = Column(IBCHAR(length=10, charset='WIN1252', collation='WIN1252'))
    ib_time = Column(IBTIME())
    ib_timestamp = Column(IBTIMESTAMP())
    ib_varying = Column(IBVARCHAR(length=10, charset='WIN1252', collation='WIN1252'))



Base.metadata.create_all(engine)

# session = Session()
#
# instances = [
#     SampleTable(name='Test1', value=123),
#     SampleTable(name='Test2', value=456)
# ]
# session.add_all(instances)
# session.commit()
#
# items = session.query(SampleTable).all()
# for i in items:
#     print(i.id, i.name, i.value)
#
# session.close()
