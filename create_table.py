import logging
import os
from sqlalchemy import create_engine, Column, Integer, DDL, event
from sqlalchemy.orm import sessionmaker, declarative_base, declared_attr
from interbase_dialect_fdb import FBDialect_interbase
import sqlalchemy_firebird.types as fb_types

# Налаштування логування
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Визначення шляху до файлу бази даних
relative_path = 'TEST.DB'
dsn = os.path.join(os.getcwd(), relative_path)

# Створення рядка з'єднання
connection_string = f'firebird+interbase://test:testkey@localhost/{dsn}'

# Створення об'єкту для взаємодії з базою даних
engine = create_engine(connection_string, echo=True)

# Створення класу для сесій
Session = sessionmaker(bind=engine)

# Оголошення базового класу для класів, які будуть мапитися в таблиці
Base = declarative_base()

# Оголошення класу, який дозволяє автоматично створювати генератори та тригери
class AutoIncrementBase(Base):
    __abstract__ = True

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def __declare_first__(cls):
        cls._create_generator_and_trigger()

    @classmethod
    def _create_generator_and_trigger(cls):
        table_name = cls.__tablename__
        generator_name = f'{table_name}_id_gen'
        trigger_name = f'{table_name}_bi'

        print(f"Creating generator {generator_name} and trigger {trigger_name} for table {table_name}")

        # Створення генератора
        generator_ddl = DDL(f'CREATE GENERATOR {generator_name}')
        event.listen(cls.__table__, 'before_create', generator_ddl)

        # Створення тригера для використання генератора
        trigger_ddl = DDL(f"""
        CREATE TRIGGER {trigger_name} FOR {table_name}
        ACTIVE BEFORE INSERT POSITION 0
        AS
        BEGIN
          IF (NEW.id IS NULL) THEN
            NEW.id = GEN_ID({generator_name}, 1);
        END
        """)
        event.listen(cls.__table__, 'after_create', trigger_ddl)

# Оголошення класу таблиці з використанням базового класу
class SampleTable2(AutoIncrementBase):
    id = Column(fb_types.FBINTEGER, primary_key=True, autoincrement=True)
    name = Column(fb_types.FBVARCHAR(50))
    value = Column(fb_types.FBINTEGER)

# Виклик методу для створення генератора та тригера
SampleTable2._create_generator_and_trigger()

# Створення таблиці у базі даних
Base.metadata.create_all(engine)
