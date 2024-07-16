import logging
import os

from sqlalchemy import create_engine, Column, Sequence, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import DatabaseError

from sqlalchemy_interbase import IBINTEGER, IBVARCHAR

# Enable logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

relative_path = 'TEST.DB'
database_path = os.path.join(os.getcwd(), relative_path)
connection_string = f"interbase://sysdba:masterkey@localhost:3051/{database_path}?charset=WIN1252"
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


class SampleTable4(IBase):
    __tablename__ = 'sampletable4'

    id = Column(IBINTEGER,
                Sequence(f'{__tablename__}_id_gen'),
                primary_key=True,
                autoincrement=True
                )
    name = Column(IBVARCHAR(50))
    value = Column(IBINTEGER)


Base.metadata.create_all(engine)

session = Session()

instances = [
    SampleTable4(name='Test1', value=123),
    SampleTable4(name='Test2', value=456)
]
session.add_all(instances)
session.commit()

items = session.query(SampleTable4).all()
for i in items:
    print(i.id, i.name, i.value)

session.close()
