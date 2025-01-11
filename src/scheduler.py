from db_utils import create_tables, insert_industry_data, insert_tickers_data, insert_sector_data
from dotenv import load_dotenv

load_dotenv()

def schedule_ingest_data():
    create_tables()
    insert_industry_data()
    insert_sector_data()
    insert_tickers_data()

if __name__ == "__main__":
    schedule_ingest_data()
