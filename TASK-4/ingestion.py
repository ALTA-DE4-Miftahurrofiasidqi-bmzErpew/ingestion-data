from db_connection import DatabaseConnection


class DataIngestion:
    def __init__(self, source_db: DatabaseConnection, target_db: DatabaseConnection):
        self.source_db = source_db
        self.target_db = target_db

    def fetch_data(self, source_table_name):
        session = self.source_db.get_session()
        source_table = self.source_db.get_table(source_table_name)
        if source_table is None:
            raise Exception(
                f"Table {source_table_name} does not exist in the source database."
            )
        data = session.query(source_table).all()
        session.close()
        return data, source_table

    def insert_data(self, data, source_table, target_table_name):
        session = self.target_db.get_session()
        target_table = self.target_db.get_table(target_table_name)
        if target_table is None:
            target_table = self.target_db.create_table(source_table, target_table_name)
        for row in data:
            session.execute(target_table.insert().values(row._asdict()))
        session.commit()
        session.close()
        print(f"Sucesfully inserted data to table: {target_table_name}")

    def execute(self, source_table_name):
        target_table_name = "_py_ingestion_" + source_table_name
        data, source_table = self.fetch_data(source_table_name)

        self.insert_data(data, source_table, target_table_name)
