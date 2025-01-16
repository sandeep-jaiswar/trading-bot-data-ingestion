import os
import math
import logging
import requests
import pandas as pd
from psycopg2 import sql, connect
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def sanitize(value):
    """
    Replace None, NaN, or empty strings with None for database compatibility.
    """
    return value if value and not (isinstance(value, float) and math.isnan(value)) else None


def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.
    """
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")

    try:
        return connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def execute_query(query, params=None, fetch_one=False, fetch_all=False, connection=None):
    """
    Helper function to execute database queries safely.
    If no connection is provided, a new one is created.
    """
    should_close_connection = connection is None
    connection = connection or get_db_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            if fetch_one:
                return cursor.fetchone()
            if fetch_all:
                return cursor.fetchall()
            connection.commit()
    except Exception as e:
        if should_close_connection:
            connection.rollback()
        logging.error(f"Query execution failed: {e}")
        raise
    finally:
        if should_close_connection:
            connection.close()


def fetch_single_id(table, column, value, connection=None):
    """
    Fetch the ID from a table where the column matches the given value.
    If no connection is provided, a new one is created.
    """
    query = sql.SQL("SELECT id FROM {} WHERE {} = %s").format(
        sql.Identifier(table), sql.Identifier(column)
    )
    try:
        result = execute_query(query, (value,), fetch_one=True, connection=connection)
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Failed to fetch ID from {table} where {column} = {value}: {e}")
        raise

def fetch_all_tickers(connection=None):
    """
    Fetch all tickers from the 'tickers' table.
    If no connection is provided, a new one is created.
    """
    query = sql.SQL("SELECT * FROM tickers")
    try:
        tickers = execute_query(query, fetch_all=True, connection=connection)
        if not tickers:
            logging.info("No tickers found in the database.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to fetch all tickers: {e}")
        raise

def create_tables(connection=None):
    """
    Create database tables by executing DDL statements from a file.
    If no connection is provided, a new one is created.
    """
    ddl_file_path = "public/ddl.sql"

    if not os.path.exists(ddl_file_path):
        logging.error(f"DDL file not found at {ddl_file_path}")
        raise FileNotFoundError(f"DDL file not found: {ddl_file_path}")

    try:
        with open(ddl_file_path, 'r') as ddl_file:
            ddl_queries = ddl_file.read()

        execute_query(ddl_queries, connection=connection)
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Failed to create tables: {e}")
        raise

def insert_data_from_csv(file_path, table_name, column_name, csv_column, connection=None):
    """
    Insert unique data from a CSV file into the specified table.
    If no connection is provided, a new one is created.
    """
    if not os.path.exists(file_path):
        logging.error(f"CSV file not found: {file_path}")
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    try:
        df = pd.read_csv(file_path)
        if csv_column not in df.columns:
            logging.error(f"Column '{csv_column}' not found in the CSV file.")
            raise ValueError(f"Missing column '{csv_column}' in CSV file.")

        unique_data = df[csv_column].dropna().unique()
        values = [(item,) for item in unique_data]

        query = sql.SQL("INSERT INTO {} ({}) VALUES (%s) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table_name), sql.Identifier(column_name)
        )

        with connection or get_db_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, query, values)
            conn.commit()

        logging.info(f"Inserted {len(values)} records into {table_name} from '{file_path}'.")
    except Exception as e:
        logging.error(f"Failed to insert data from CSV into {table_name}: {e}")
        raise

def insert_tickers_data(file_path="public/Equity.csv", connection=None):
    """
    Insert tickers and their details into the database from the CSV file.
    If no connection is provided, a new one is created.
    """
    required_columns = ["Security Id", "Security Name", "Sector Name", "Industry New Name"]

    if not os.path.exists(file_path):
        logging.error(f"CSV file not found: {file_path}")
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    try:
        # Load and validate CSV
        df = pd.read_csv(file_path)
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing columns in CSV file: {', '.join(missing_columns)}")
            raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

        ticker_data = df[required_columns].dropna()
        records = []

        # Process each row
        with connection or get_db_connection() as conn:
            for _, row in ticker_data.iterrows():
                industry_id = fetch_single_id("industries", "industry_name", row["Industry New Name"], conn)
                sector_id = fetch_single_id("sectors", "sector_name", row["Sector Name"], conn)

                if industry_id is None or sector_id is None:
                    logging.warning(f"Skipping row due to missing IDs: {row.to_dict()}")
                    continue

                records.append((row["Security Id"], row["Security Name"], sector_id, industry_id))

            # Insert data
            if records:
                query = sql.SQL(
                    """
                    INSERT INTO tickers (ticker, security_name, sector_id, industry_id)
                    VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """
                )
                with conn.cursor() as cursor:
                    execute_batch(cursor, query, records)
                conn.commit()

        logging.info(f"Inserted {len(records)} tickers successfully from '{file_path}'.")
    except Exception as e:
        logging.error(f"Failed to insert tickers data: {e}")
        raise

def fetch_industry_id(industry_name, connection=None):
    """
    Fetch the ID of an industry by its name.
    """
    return fetch_single_id("industries", "industry_name", industry_name, connection)

        
def fetch_ticker_id(ticker, connection=None):
    """
    Fetch the ID of a company by its ticker symbol.
    """
    return fetch_single_id("tickers", "ticker", ticker, connection)

        
        
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


def fetch_sector_id(sector_name, connection=None):
    """
    Fetch the ID of a sector by its name.
    """
    return fetch_single_id("sectors", "sector_name", sector_name, connection)



def insert_industry_data(file_path="public/Equity.csv", connection=None):
    """
    Insert unique industry names from the CSV file into the 'industries' table.
    """
    try:
        insert_data_from_csv(
            file_path=file_path,
            table_name="industries",
            column_name="industry_name",
            csv_column="Industry New Name",
            connection=connection
        )
        logging.info("Industry data inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to insert industry data: {e}")
        raise

        
def insert_sector_data(file_path="public/Equity.csv", connection=None):
    """
    Insert unique sector names from the CSV file into the 'sectors' table.
    """
    try:
        insert_data_from_csv(
            file_path=file_path,
            table_name="sectors",
            column_name="sector_name",
            csv_column="Sector Name",
            connection=connection
        )
        logging.info("Sector data inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to insert sector data: {e}")
        raise


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
        
        
def insert_dividend_data(dividend_data, symbol, connection=None):
    """
    Insert dividend data into the 'dividends' table based on a dictionary input.
    :param dividend_data: dict containing dividend-related data with dates as keys and amounts as values.
    :param symbol: The stock ticker symbol.
    :param connection: Optional database connection for reuse.
    """
    if not isinstance(dividend_data, dict):
        raise ValueError("dividend_data must be a dictionary.")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string.")

    query = sql.SQL(
        """
        INSERT INTO dividends (ticker_id, action_date, price)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """
    )

    try:
        with connection or get_db_connection() as conn:
            ticker_id = fetch_ticker_id(symbol, conn)
            if ticker_id is None:
                raise ValueError(f"Ticker '{symbol}' not found.")

            values = [(ticker_id, date, amount) for date, amount in dividend_data.items()]
            if not values:
                logging.info(f"No dividend data to insert for ticker: {symbol}.")
                return

            with conn.cursor() as cursor:
                execute_batch(cursor, query, values)
            conn.commit()

        logging.info(f"Inserted {len(values)} dividend records for ticker '{symbol}'.")
    except Exception as e:
        logging.error(f"Failed to insert dividend data for '{symbol}': {e}")
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
    
    insert_query = sql.SQL("""
        INSERT INTO balance_sheets (
            ticker_id, report_date, {columns}
        ) VALUES (%s, %s, {placeholders})
        ON CONFLICT DO NOTHING
        """).format(
            columns=sql.SQL(', ').join(map(sql.Identifier, c)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in columns)
        )

    try:
        with get_db_connection() as conn:
            ticker_id = fetch_ticker_id(symbol, conn)
            if ticker_id is None:
                raise ValueError(f"Ticker '{symbol}' not found.")

            logging.info(f"Inserting balance sheet data for ticker ID: {ticker_id}")

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
    
    
def insert_cashflow(cashflow, symbol):
    if not isinstance(cashflow, dict):
        raise ValueError("cashflow must be a dictionary.")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string.")
    
    c = [
        'free_cash_flow',
        'capital_expenditure',
        'end_cash_position',
        'beginning_cash_position',
        'effect_of_exchange_rate_changes',
        'changes_in_cash',
        'financing_cash_flow',
        'interest_paid_cff',
        'cash_dividends_paid',
        'common_stock_dividend_paid',
        'investing_cash_flow',
        'net_other_investing_changes',
        'interest_received_cfi',
        'net_investment_purchase_and_sale',
        'sale_of_investment',
        'purchase_of_investment',
        'net_business_purchase_and_sale',
        'sale_of_business',
        'purchase_of_business',
        'net_ppe_purchase_and_sale',
        'sale_of_ppe',
        'purchase_of_ppe',
        'operating_cash_flow',
        'taxes_refund_paid',
        'change_in_working_capital',
        'change_in_other_current_liabilities',
        'change_in_other_current_assets',
        'change_in_payable',
        'change_in_inventory',
        'change_in_receivables',
        'other_non_cash_items',
        'provisionand_write_offof_assets',
        'depreciation_and_amortization',
        'amortization_cash_flow',
        'depreciation',
        'gain_loss_on_investment_securities',
        'net_foreign_currency_exchange_gain_loss',
        'gain_loss_on_sale_of_ppe',
        'gain_loss_on_sale_of_business',
        'net_income_from_continuing_operations'
    ]

    columns = [
        'FreeCashFlow',
        'CapitalExpenditure',
        'EndCashPosition',
        'BeginningCashPosition',
        'EffectOfExchangeRateChanges',
        'ChangesInCash',
        'FinancingCashFlow',
        'InterestPaidCFF',
        'CashDividendsPaid',
        'CommonStockDividendPaid',
        'InvestingCashFlow',
        'NetOtherInvestingChanges',
        'InterestReceivedCFI',
        'NetInvestmentPurchaseAndSale',
        'SaleOfInvestment',
        'PurchaseOfInvestment',
        'NetBusinessPurchaseAndSale',
        'SaleOfBusiness',
        'PurchaseOfBusiness',
        'NetPPEPurchaseAndSale',
        'SaleOfPPE',
        'PurchaseOfPPE',
        'OperatingCashFlow',
        'TaxesRefundPaid',
        'ChangeInWorkingCapital',
        'ChangeInOtherCurrentLiabilities',
        'ChangeInOtherCurrentAssets',
        'ChangeInPayable',
        'ChangeInInventory',
        'ChangeInReceivables',
        'OtherNonCashItems',
        'ProvisionandWriteOffofAssets',
        'DepreciationAndAmortization',
        'AmortizationCashFlow',
        'Depreciation',
        'GainLossOnInvestmentSecurities',
        'NetForeignCurrencyExchangeGainLoss',
        'GainLossOnSaleOfPPE',
        'GainLossOnSaleOfBusiness',
        'NetIncomeFromContinuingOperations'
    ]

    # Ensure that the number of columns in `c` matches the placeholders.
    placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in columns)
    
    insert_query = sql.SQL("""
        INSERT INTO cashflows (
            ticker_id, report_date, {columns}
        ) VALUES (%s, %s, {placeholders})
        ON CONFLICT DO NOTHING
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, c)),
        placeholders=placeholders
    )

    try:
        with get_db_connection() as conn:
            ticker_id = fetch_ticker_id(symbol, conn)
            if ticker_id is None:
                raise ValueError(f"Ticker '{symbol}' not found.")
            
            logging.info(f"Inserting cashflow data for ticker ID: {ticker_id}")
            
            values = [
                (ticker_id, report_date) + tuple(
                    sanitize(row_data.get(column)) for column in c
                )
                for report_date, row_data in cashflow.items()
            ]

            if not values:
                logging.warning("No cashflow data to insert.")
                return

            with conn.cursor() as cursor:
                # Log the query and values for debugging.
                logging.info(f"Insert query: {insert_query.as_string(conn)}")
                logging.info(f"Values: {values}")

                # Execute batch insertion.
                execute_batch(cursor, insert_query, values)
            conn.commit()

            logging.info(f"Successfully inserted cashflow data for symbol: {symbol}")

    except Exception as e:
        logging.error(f"Failed to insert cashflow data for symbol '{symbol}': {e}")
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
            cursor.execute(insert_query,
                    (ticker_data.get("dayHigh"),
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
        logging.error(f"Failed to update ticker data for '{symbol}': {e}")
        raise Exception(f"Failed to update security data: {e}")
        
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
        
        
