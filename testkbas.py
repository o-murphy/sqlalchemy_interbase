import os

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

# Replace with your actual database connection details
relative_path = 'TEST.DB'
dsn = os.path.join(os.getcwd(), relative_path)
connection_string = f'firebird+fdb://sysdba:masterkey@localhost/{dsn}'

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()
metadata.reflect(bind=engine)
#
# Base = declarative_base()
#
# def generate_class_code(table):
#     class_name = table.name.capitalize()
#     columns = ",\n    ".join([f"{col.name} = Column({repr(col.type)})" for col in table.columns])
#     class_code = f"""
# class {class_name}(Base):
#     __tablename__ = '{table.name}'
#     {columns}
# """
#     return class_code
#
# # Generate and print the class definitions
# for table_name, table in metadata.tables.items():
#     if table_name != "sample_table":
#         continue
#     class_code = generate_class_code(table)
#     print(class_code)
#
# class BaseModel:
#     @classmethod
#     def __declare_last__(cls):
#         # This method will be called after all classes have been declared
#         pass
#
# models = {}
#
# for table_name, table in metadata.tables.items():
#     # Create a new class for each table
#     class_name = table_name.capitalize()
#     model = type(class_name, (Base, BaseModel), {'__table__': table})
#     models[class_name] = model
#
# # Generate and print the class definitions again if needed
# for class_name, model in models.items():
#     print(generate_class_code(model.__table__))