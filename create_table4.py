import logging
import os

from sqlalchemy import create_engine, Column, Sequence, Identity
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_interbase.base import IBDialect, ib_types

# Налаштування логування
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

relative_path = 'TEST.DB'
dsn = os.path.join(os.getcwd(), relative_path)
connection_string = f'interbase://test:testkey@localhost/{dsn}'
engine = create_engine(connection_string, echo=True)

# Створення класу для сесій
Session = sessionmaker(bind=engine)

# Оголошення базового класу для класів, які будуть мапитися в таблиці
Base = declarative_base()


# Оголошення класу, який дозволяє автоматично створювати генератори та тригери
class AutoIncrementBase(Base):
    __abstract__ = True
    __table_args__ = {'implicit_returning': False}


# Оголошення класу таблиці з використанням базового класу
class SampleTable4(AutoIncrementBase):
    __tablename__ = 'sampletable4'

    # id = Column(Integer, primary_key=True, autoincrement=True)

    id = Column(ib_types.IBINTEGER,
                Identity(start=1, cycle=True),
                # Sequence(f'{__tablename__}_id_gen'),
                primary_key=True,
                # autoincrement=True
                )
    name = Column(ib_types.IBVARCHAR(50))
    value = Column(ib_types.IBINTEGER)


Base.metadata.create_all(engine)

# session = Session()
# instances = [
#     SampleTable4(name='Test1', value=123),
#     SampleTable4(name='Test2', value=456)
# ]
# session.add_all(instances)
# session.commit()
# session.close()
