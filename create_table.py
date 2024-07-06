import os
import time

from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import sessionmaker, declarative_base
from interbase_dialect_fdb import FBDialect_interbase
assert FBDialect_interbase

# Get the current working directory
current_dir = os.getcwd()

# Define the database file path
relative_path = 'TEST.DB'
dsn = os.path.join(current_dir, relative_path)

# Define the connection string
# connection_string = f'firebird+interbase://sysdba:masterkey@localhost/{dsn}'
connection_string = f'firebird+interbase://test:testkey@localhost/{dsn}'

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Create a session class
Session = sessionmaker(bind=engine)

# Create a base class for declarative class definitions
Base = declarative_base()

# Define your declarative model class
class SampleTable(Base):
    __tablename__ = 'sample_table'
    __table_args__ = {'implicit_returning': False}

    # Використання послідовності SAMPLE_TABLE_ID_SEQ
    id = Column(Integer, Sequence('SAMPLE_TABLE_ID_SEQ'), primary_key=True)
    name = Column(String(50))
    value = Column(Integer)

# Disable RETURNING clause for each column
# for col in inspect(SampleTable).columns:
#     col._returning = False

# Create the table in the database
Base.metadata.create_all(engine)

# Example usage: adding data to the table
# Create a session
session = Session()

# Add some data
sample_data = [
    SampleTable(name='Example1', value=100),
    SampleTable(name='Example2', value=200),
]

# Add data to the session and commit
session.add_all(sample_data)
session.commit()

# Query the data
data = session.query(SampleTable).all()
for item in data:
    print(f'ID: {item.id}, Name: {item.name}, Value: {item.value}')

# Close the session

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    session.close()