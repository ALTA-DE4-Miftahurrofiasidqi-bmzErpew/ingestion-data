from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError


class DatabaseConnection:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.engine = None
        self.Session = None
        self.metadata = MetaData()

    def connect(self):
        try:
            self.engine = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.engine)
            print(f"Successfully connect to database: {self.connection_string}")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if self.engine:
            self.engine.dispose()
            print(f"Connection to database closed: {self.connection_string}")

    def get_session(self):
        if not self.Session:
            self.connect()
        return self.Session()

    def get_table(self, table_name):
        try:
            return Table(table_name, self.metadata, autoload_with=self.engine)
        except NoSuchTableError:
            return None

    def create_table(self, source_table, target_table_name):
        columns = [
            Column(column.name, column.type, primary_key=column.primary_key)
            for column in source_table.columns
        ]
        target_table = Table(
            target_table_name, self.metadata, *columns, extend_existing=True
        )
        target_table.create(self.engine)
        return target_table
