from random_data.generate_data import generate_data
import duckdb


class DuckDBUtils:
    @staticmethod
    def generate_test_data(rows=100):
        return generate_data(rows)

    @staticmethod
    def validate_table(con, table: str):
        try:
            con = duckdb.connect()
            con.execute(f"DESCRIBE {table}")
            con.close()
            return True
        except:
            return False
