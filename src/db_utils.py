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
        logging.info("Tables created successfully.")
    
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
        
        
def fetch_all_tickers():
    """
    Fetch all tickers from the 'tickers' table.
    """
    query = sql.SQL("SELECT * FROM {}").format(
        sql.Identifier('tickers')
    )
    conn = get_db_connection()
    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
            tickers = cursor.fetchall()
            return tickers if tickers else None
        except Exception as e:
            raise Exception(f"Failed to fetch tickers: {e}")
        
        
def fetch_ticker_data(ticker, conn):
    """
    Fetch the ID of a company by its ticker symbol.
    """
    query = sql.SQL("SELECT * FROM {} WHERE ticker = %s").format(
        sql.Identifier('tickers')
    )
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (ticker,))
            ticker_data = cursor.fetchone()
            return ticker_data if ticker_data else None
        except Exception as e:
            raise Exception(f"Failed to fetch ticker data: {e}")
        
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
        logging.info(f"{len(industry_values)} industries inserted successfully.")
    
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
        logging.info(f"{len(sector_values)} sectors inserted successfully.")
    
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
                logging.info(f"Industry '{row['Industry New Name']}' not found. Skipping row.")
                continue
            security_values.append((row['Security Id'], row['Security Name'], sector_id, industry_id))

        if security_values:
            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, security_values)
            conn.commit()
            logging.info(f"{len(security_values)} securities inserted successfully.")
        else:
            logging.info("No securities data to insert.")

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert security data: {e}")
    
    finally:
        conn.close()

def insert_dividend_metrics(json_data):
    """
    Insert dividend metrics data into the 'dividend_metrics' table based on JSON input.
    :param json_data: dict containing dividend-related data.
    """
    conn = get_db_connection()
    insert_query = sql.SQL(
        "INSERT INTO {} (ticker_id, dividend_yield, payout_ratio, dividend_growth) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
    ).format(sql.Identifier('dividend_metrics'))

    try:
        # Extract the required dividend metrics from JSON data
        ticker_id = fetch_ticker_id(json_data.get("symbol"), conn)
        if ticker_id is None:
            raise ValueError(f"Ticker '{json_data.get('symbol')}' not found.")

        dividend_yield = json_data.get("dividendYield")
        payout_ratio = json_data.get("payoutRatio")
        dividend_growth = json_data.get("fiveYearAvgDividendYield")  # Assuming 5-year average dividend yield as growth

        # Validate extracted values
        if dividend_yield is None or payout_ratio is None or dividend_growth is None:
            raise ValueError("Missing one or more required dividend metrics in the JSON data.")

        # Prepare data for insertion
        dividend_values = [(ticker_id, dividend_yield, payout_ratio, dividend_growth)]

        # Insert into the database
        with conn.cursor() as cursor:
            execute_batch(cursor, insert_query, dividend_values)
        conn.commit()
        logging.info(f"Dividend metrics for '{json_data.get('symbol')}' inserted successfully.")

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert dividend metrics data: {e}")

    finally:
        conn.close()
        
        
def insert_company_data(json_data):
    """
    Insert company data into the 'company' table based on JSON input.
    :param json_data: dict containing company-related data.
    """
    conn = get_db_connection()
    insert_query = sql.SQL(
        """
        INSERT INTO {} (
            ticker_id,
            short_name,
            long_name,
            industry,
            industry_key,
            industry_disp,
            sector,
            sector_key,
            sector_disp,
            address1,
            address2,
            city,
            zip,
            country,
            phone,
            fax,
            website,
            long_business_summary,
            full_time_employees,
            currency,
            exchange,
            quote_type,
            symbol,
            underlying_symbol
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ticker_id)
        DO UPDATE SET
            short_name = EXCLUDED.short_name,
            long_name = EXCLUDED.long_name,
            industry = EXCLUDED.industry,
            industry_key = EXCLUDED.industry_key,
            industry_disp = EXCLUDED.industry_disp,
            sector = EXCLUDED.sector,
            sector_key = EXCLUDED.sector_key,
            sector_disp = EXCLUDED.sector_disp,
            address1 = EXCLUDED.address1,
            address2 = EXCLUDED.address2,
            city = EXCLUDED.city,
            zip = EXCLUDED.zip,
            country = EXCLUDED.country,
            phone = EXCLUDED.phone,
            fax = EXCLUDED.fax,
            website = EXCLUDED.website,
            long_business_summary = EXCLUDED.long_business_summary,
            full_time_employees = EXCLUDED.full_time_employees,
            currency = EXCLUDED.currency,
            exchange = EXCLUDED.exchange,
            quote_type = EXCLUDED.quote_type,
            symbol = EXCLUDED.symbol,
            underlying_symbol = EXCLUDED.underlying_symbol
        """
    ).format(sql.Identifier('company'))


    try:
        # Extract and prepare data from JSON
        ticker_id = fetch_ticker_id(json_data.get('symbol'), conn)
        if ticker_id is None:
            raise ValueError(f"Ticker '{json_data.get('symbol')}' not found.")
        
        logging.info(ticker_id)


        # Extract values, using defaults where necessary
        company_values = (
            ticker_id,
            json_data.get("shortName",""),
            json_data.get("longName",""),
            json_data.get("industry",""),
            json_data.get("industryKey",""),
            json_data.get("industryDisp",""),
            json_data.get("sector",""),
            json_data.get("sectorKey",""),
            json_data.get("sectorDisp",""),
            json_data.get("address1",""),
            json_data.get("address2",""),
            json_data.get("city",""),
            json_data.get("zip",""),
            json_data.get("country",""),
            json_data.get("phone",""),
            json_data.get("fax",""),
            json_data.get("website",""),
            json_data.get("longBusinessSummary",""),
            json_data.get("fullTimeEmployees",0),
            json_data.get("currency",""),
            json_data.get("exchange",""),
            json_data.get("quoteType",""),
            json_data.get("symbol",""),
            json_data.get("underlyingSymbol",""),
        )
        
        # Insert data into the database
        with conn.cursor() as cursor:
            cursor.execute(insert_query, company_values)
        conn.commit()
        logging.info(f"Company data for '{json_data.get('symbol')}' inserted/updated successfully.")

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert/update company data: {e}")

    finally:
        conn.close()

