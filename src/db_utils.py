import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
from dotenv import load_dotenv
import pandas as pd
import logging 

# Load environment variables
load_dotenv()

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")


def fetch_company_overview(symbol):
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={os.getenv("ALPHA_VANTAGE_API")}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

def create_tables():
    """
    Creates database tables by executing DDL statements from a file.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    ddl_file_path = "public/ddl.sql"

    try:
        if not os.path.exists(ddl_file_path):
            raise FileNotFoundError(f"{ddl_file_path} not found.")

        with open(ddl_file_path, 'r') as ddl_file:
            ddl_queries = ddl_file.read()

        cursor.execute(ddl_queries)
        conn.commit()
        print("Tables created successfully.")
    
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to create tables: {e}")
    
    finally:
        cursor.close()
        conn.close()


def fetch_industry_id(industry_name, conn):
    """
    Fetch the ID of an industry by its name.
    """
    query = sql.SQL("SELECT id FROM {} WHERE industry_name = %s").format(
        sql.Identifier('industries')
    )
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (industry_name,))
            industry_id = cursor.fetchone()
            return industry_id[0] if industry_id else None
        except Exception as e:
            raise Exception(f"Failed to fetch industry ID: {e}")
        
        
def fetch_ticker_id(ticker, conn):
    """
    Fetch the ID of a company by its ticker symbol.
    """
    query = sql.SQL("SELECT id FROM {} WHERE ticker = %s").format(
        sql.Identifier('tickers')
    )
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (ticker,))
            ticker_id = cursor.fetchone()
            return ticker_id[0] if ticker_id else None
        except Exception as e:
            raise Exception(f"Failed to fetch ticker ID: {e}")
        
        
def fetch_sector_id(sector_name, conn):
    """
    Fetch the ID of a sector by its name.
    """
    query = sql.SQL("SELECT id FROM {} WHERE sector_name = %s").format(
        sql.Identifier('sectors')
    )
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (sector_name,))
            sector_id = cursor.fetchone()
            return sector_id[0] if sector_id else None
        except Exception as e:
            raise Exception(f"Failed to fetch sector ID: {e}")


def insert_industry_data():
    """
    Insert unique industry names from the CSV file into the 'industries' table.
    """
    conn = get_db_connection()
    file_path = "public/Equity.csv"
    insert_query = sql.SQL("INSERT INTO {} (industry_name) VALUES (%s) ON CONFLICT DO NOTHING").format(
        sql.Identifier('industries')
    )

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found.")

        df = pd.read_csv(file_path)

        if 'Industry New Name' not in df.columns:
            raise ValueError("Column 'Industry New Name' not found in the CSV file.")

        industry_data = df['Industry New Name'].dropna().unique()
        industry_values = [(industry,) for industry in industry_data]

        with conn.cursor() as cursor:
            execute_batch(cursor, insert_query, industry_values)
        
        conn.commit()
        print(f"{len(industry_values)} industries inserted successfully.")
    
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert industry data: {e}")
    
    finally:
        conn.close()
        
        
def insert_sector_data():
    """
    Insert unique industry names from the CSV file into the 'industries' table.
    """
    conn = get_db_connection()
    file_path = "public/Equity.csv"
    insert_query = sql.SQL("INSERT INTO {} (sector_name) VALUES (%s) ON CONFLICT DO NOTHING").format(
        sql.Identifier('sectors')
    )

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found.")

        df = pd.read_csv(file_path)

        if 'Sector Name' not in df.columns:
            raise ValueError("Column 'Sector Name' not found in the CSV file.")

        sector_data = df['Sector Name'].dropna().unique()
        sector_values = [(sector,) for sector in sector_data]

        with conn.cursor() as cursor:
            execute_batch(cursor, insert_query, sector_values)
        
        conn.commit()
        print(f"{len(sector_values)} sectors inserted successfully.")
    
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert sectors data: {e}")
    
    finally:
        conn.close()


def insert_tickers_data():
    """
    Insert securities data (ticker, security_name, industry_id) from the CSV file into the 'securities' table.
    """
    conn = get_db_connection()
    file_path = "public/Equity.csv"
    insert_query = sql.SQL(
        "INSERT INTO {} (ticker, security_name, sector_id, industry_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
    ).format(sql.Identifier('tickers'))

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found.")

        df = pd.read_csv(file_path)

        required_columns = ['Security Id', 'Security Name', 'Sector Name', 'Industry New Name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in CSV file: {', '.join(missing_columns)}")

        ticker_data = df[['Security Id', 'Security Name', 'Sector Name', 'Industry New Name']].dropna()

        security_values = []
        for _, row in ticker_data.iterrows():
            industry_id = fetch_industry_id(row['Industry New Name'], conn)
            sector_id = fetch_sector_id(row['Sector Name'], conn)
            if industry_id is None:
                print(f"Industry '{row['Industry New Name']}' not found. Skipping row.")
                continue
            security_values.append((row['Security Id'], row['Security Name'], sector_id, industry_id))

        if security_values:
            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, security_values)
            conn.commit()
            print(f"{len(security_values)} securities inserted successfully.")
        else:
            print("No securities data to insert.")

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert security data: {e}")
    
    finally:
        conn.close()
