import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
from dotenv import load_dotenv
import pandas as pd
import logging 
import math

# Load environment variables
load_dotenv()

def sanitize(value):
    """
    Replace None or NaN with None to ensure database compatibility.
    """
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else value

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
    Insert tickers data (ticker, security_name, industry_id) from the CSV file into the 'tickers' table.
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
        
        
def insert_dividend_data(dividend_data, symbol):
    """
    Insert dividend data into the 'dividends' table based on a dictionary input.
    :param dividend_data: dict containing dividend-related data with dates as keys and amounts as values.
    :param symbol: The stock ticker symbol.
    """
    # Validate input
    if not isinstance(dividend_data, dict):
        raise ValueError("dividend_data must be a dictionary.")

    # Connect to the database
    try:
        with get_db_connection() as conn:
            # Prepare the SQL query
            insert_query = sql.SQL(
                """
                INSERT INTO {} (
                    ticker_id,
                    action_date,
                    price
                )
                VALUES (
                    %s, %s, %s
                )
                ON CONFLICT DO NOTHING
                """
            ).format(sql.Identifier('dividends'))

            # Fetch the ticker ID
            ticker_id = fetch_ticker_id(symbol, conn)
            if ticker_id is None:
                raise ValueError(f"Ticker '{symbol}' not found.")

            # Prepare data for insertion
            dividend_values = [(ticker_id, date, amount) for date, amount in dividend_data.items()]

            # Insert into the database
            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, dividend_values)
            conn.commit()

            logging.info(f"Dividend data for {symbol} inserted successfully.")

    except Exception as e:
        logging.error(f"Failed to insert dividend data: {e}")
        raise


def insert_balance_sheet(balance_sheet, symbol):
    """
    Inserts balance_sheet data into the 'balance_sheets' table based on dictionary input.
    :param balance_sheet: dict containing balance sheet data with dates as keys and column-value dictionaries as values.
    :param symbol: The stock ticker symbol.
    """
    if not isinstance(balance_sheet, dict):
        raise ValueError("balance_sheet must be a dictionary.")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string.")
    
    c = [
                    "treasury_shares_number",
                    "ordinary_shares_number",
                    "share_issued",
                    "total_debt",
                    "tangible_book_value",
                    "invested_capital",
                    "working_capital",
                    "net_tangible_assets",
                    "capital_lease_obligations",
                    "common_stock_equity",
                    "total_capitalization",
                    "total_equity_gross_minority_interest",
                    "stockholders_equity",
                    "other_equity_interest",
                    "retained_earnings",
                    "additional_paid_in_capital",
                    "capital_stock",
                    "common_stock",
                    "total_liabilities_net_minority_interest",
                    "total_non_current_liabilities_net_minority_interest",
                    "derivative_product_liabilities",
                    "long_term_debt_and_capital_lease_obligation",
                    "long_term_capital_lease_obligation",
                    "long_term_debt",
                    "long_term_provisions",
                    "current_liabilities",
                    "other_current_liabilities",
                    "current_deferred_taxes_liabilities",
                    "current_debt_and_capital_lease_obligation",
                    "current_capital_lease_obligation",
                    "pension_and_other_post_retirement_benefit_plans_current",
                    "current_provisions",
                    "payables",
                    "other_payable",
                    "dividends_payable",
                    "total_tax_payable",
                    "accounts_payable",
                    "total_assets",
                    "total_non_current_assets",
                    "other_non_current_assets",
                    "non_current_prepaid_assets",
                    "non_current_deferred_taxes_assets",
                    "financial_assets",
                    "other_investments",
                    "investment_in_financial_assets",
                    "available_for_sale_securities",
                    "goodwill_and_other_intangible_assets",
                    "other_intangible_assets",
                    "goodwill",
                    "net_ppe",
                    "accumulated_depreciation",
                    "gross_ppe",
                    "construction_in_progress",
                    "other_properties",
                    "machinery_furniture_equipment",
                    "buildings_and_improvements",
                    "land_and_improvements",
                    "properties",
                    "current_assets",
                    "other_current_assets",
                    "hedging_assets_current",
                    "assets_held_for_sale_current",
                    "restricted_cash",
                    "prepaid_assets",
                    "inventory",
                    "finished_goods",
                    "work_in_process",
                    "raw_materials",
                    "other_receivables",
                    "taxes_receivable",
                    "accounts_receivable",
                    "allowance_for_doubtful_accounts_receivable",
                    "gross_accounts_receivable",
                    "cash_cash_equivalents_and_short_term_investments",
                    "other_short_term_investments",
                    "cash_and_cash_equivalents",
                    "cash_equivalents",
                    "cash_financial"
    ]

    columns = [
        "TreasurySharesNumber", "OrdinarySharesNumber", "ShareIssued", "TotalDebt", 
        "TangibleBookValue", "InvestedCapital", "WorkingCapital", "NetTangibleAssets",
        "CapitalLeaseObligations", "CommonStockEquity", "TotalCapitalization", 
        "TotalEquityGrossMinorityInterest", "StockholdersEquity", "OtherEquityInterest", 
        "RetainedEarnings", "AdditionalPaidInCapital", "CapitalStock", "CommonStock",
        "TotalLiabilitiesNetMinorityInterest", "TotalNonCurrentLiabilitiesNetMinorityInterest",
        "DerivativeProductLiabilities", "LongTermDebtAndCapitalLeaseObligation", 
        "LongTermCapitalLeaseObligation", "LongTermDebt", "LongTermProvisions", 
        "CurrentLiabilities", "OtherCurrentLiabilities", "CurrentDeferredTaxesLiabilities",
        "CurrentDebtAndCapitalLeaseObligation", "CurrentCapitalLeaseObligation", 
        "PensionAndOtherPostRetirementBenefitPlansCurrent", "CurrentProvisions", 
        "Payables", "OtherPayable", "DividendsPayable", "TotalTaxPayable", "AccountsPayable",
        "TotalAssets", "TotalNonCurrentAssets", "OtherNonCurrentAssets", 
        "NonCurrentPrepaidAssets", "NonCurrentDeferredTaxesAssets", "FinancialAssets",
        "OtherInvestments", "InvestmentInFinancialAssets", "AvailableForSaleSecurities",
        "GoodwillAndOtherIntangibleAssets", "OtherIntangibleAssets", "Goodwill", "NetPpe",
        "AccumulatedDepreciation", "GrossPpe", "ConstructionInProgress", "OtherProperties", 
        "MachineryFurnitureEquipment", "BuildingsAndImprovements", "LandAndImprovements",
        "Properties", "CurrentAssets", "OtherCurrentAssets", "HedgingAssetsCurrent", 
        "AssetsHeldForSaleCurrent", "RestrictedCash", "PrepaidAssets", "Inventory",
        "FinishedGoods", "WorkInProcess", "RawMaterials", "OtherReceivables", "TaxesReceivable",
        "AccountsReceivable", "AllowanceForDoubtfulAccountsReceivable", "GrossAccountsReceivable",
        "CashCashEquivalentsAndShortTermInvestments", "OtherShortTermInvestments", 
        "CashAndCashEquivalents", "CashEquivalents", "CashFinancial"
    ]

    try:
        with get_db_connection() as conn:
            ticker_id = fetch_ticker_id(symbol, conn)
            if ticker_id is None:
                raise ValueError(f"Ticker '{symbol}' not found.")

            logging.info(f"Inserting balance sheet data for ticker ID: {ticker_id}")
            
            insert_query = sql.SQL("""
                INSERT INTO balance_sheets (
                    ticker_id, report_date, {columns}
                ) VALUES (%s, %s, {placeholders})
                ON CONFLICT DO NOTHING
            """).format(
                columns=sql.SQL(', ').join(map(sql.Identifier, c)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in columns)
            )

            values = [
                (ticker_id, report_date) + tuple(
                    sanitize(row_data.get(column)) for column in columns
                )
                for report_date, row_data in balance_sheet.items()
            ]

            if not values:
                logging.warning("No balance sheet data to insert.")
                return

            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, values)
            conn.commit()

            logging.info(f"Successfully inserted balance sheet data for symbol: {symbol}")
    except Exception as e:
        logging.error(f"Failed to insert balance sheet data for symbol '{symbol}': {e}")
        raise


def update_tickers_data(ticker_data, symbol):
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string.")
    conn = get_db_connection()
    insert_query = sql.SQL(
        """
        UPDATE {}
        SET day_high = %s,
        day_low = %s,
        fifty_day_average = %s,
        last_price = %s,
        last_volume = %s,
        market_cap = %s,
        open = %s,
        previous_close = %s,
        regular_market_previous_close = %s,
        year_high = %s,
        year_low = %s,
        shares = %s,
        ten_day_average_volume = %s,
        three_month_average_volume = %s,
        two_hundred_day_average = %s,
        yearChange = %s
        WHERE ticker = %s
        """
    ).format(sql.Identifier('tickers'))

    try:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string.")

        with conn.cursor() as cursor:
            cursor.execute(insert_query, (ticker_data.get("dayHigh"),
        ticker_data.get("dayLow"),
        ticker_data.get("fiftyDayAverage"),
        ticker_data.get("lastPrice"),
        ticker_data.get("lastVolume"),
        ticker_data.get("marketCap"),
        ticker_data.get("open"),
        ticker_data.get("previousClose"),
        ticker_data.get("regularMarketPreviousClose"),
        ticker_data.get("yearHigh"),
        ticker_data.get("yearLow"),
        ticker_data.get("shares"),
        ticker_data.get("tenDayAverageVolume"),
        ticker_data.get("threeMonthAverageVolume"),
        ticker_data.get("twoHundredDayAverage"),
        ticker_data.get("yearChange"),symbol))
        conn.commit()
        logging.info(f"Security data for '{symbol}' updated successfully.")

    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to update security data: {e}")
    
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
        
        
