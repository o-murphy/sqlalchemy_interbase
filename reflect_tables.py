import logging
import os

import sqlalchemy_interbase.base

assert sqlalchemy_interbase.base
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base

# Enable logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Define your connection URL
relative_path = 'TEST.DB'
database_path = os.path.join(os.getcwd(), relative_path)
connection_string = f"interbase://localhost:3050/{database_path}?charset=WIN1252"

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()
metadata.reflect(bind=engine)

Base = declarative_base()


def generate_class_code(table):
    class_name = table.name.capitalize()

    # Extract primary key columns
    primary_keys = [col.name for col in table.primary_key.columns]

    # Generate column definitions with types and primary key constraints
    columns = "\n    ".join([
        f"{col.name.strip().lower()} = Column({repr(col.type)}{', primary_key=True' if col.name in primary_keys else ''})"
        for col in table.columns
    ])

    # Generate class code
    class_code = f"""
class {class_name.strip()}(Base):
    __tablename__ = '{table.name.strip()}'
    {columns}
"""
    return class_code


# Generate and print the class definitions
for table_name, table in metadata.tables.items():
    if table_name != "sample_table":
        continue
    class_code = generate_class_code(table)
    print(class_code)


class BaseModel:
    @classmethod
    def __declare_last__(cls):
        # This method will be called after all classes have been declared
        pass


models = {}

for table_name, table in metadata.tables.items():
    # Create a new class for each table
    class_name = table_name.capitalize()
    model = type(class_name, (Base, BaseModel), {'__table__': table})
    models[class_name] = model

# Generate and print the class definitions again if needed
for class_name, model in models.items():
    print(generate_class_code(model.__table__))
