CREATE TABLE IF NOT EXISTS public.industries (
    id SERIAL PRIMARY KEY,
    industry_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS public.sectors (
    id SERIAL PRIMARY KEY,
    sector_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS public.tickers (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(16) NOT NULL UNIQUE,
    security_name VARCHAR(255) NOT NULL,
    industry_id INT,
    sector_id INT,
    CONSTRAINT fk_industry FOREIGN KEY (industry_id) REFERENCES industries (id) ON DELETE SET NULL,
    CONSTRAINT fk_sector FOREIGN KEY (sector_id) REFERENCES sectors (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.company (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    short_name VARCHAR(255),
    long_name VARCHAR(255),
    industry VARCHAR(255),
    industry_key VARCHAR(255),
    industry_disp VARCHAR(255),
    sector VARCHAR(255),
    sector_key VARCHAR(255),
    sector_disp VARCHAR(255),
    address1 TEXT,
    address2 TEXT,
    city VARCHAR(255),
    zip VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(50),
    fax VARCHAR(50),
    website VARCHAR(255),
    long_business_summary TEXT,
    full_time_employees INT,
    currency VARCHAR(10),
    exchange VARCHAR(50),
    quote_type VARCHAR(50),
    symbol VARCHAR(50),
    underlying_symbol VARCHAR(50),
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL,
    CONSTRAINT unique_ticker_id UNIQUE (ticker_id)
);

CREATE TABLE IF NOT EXISTS public.company_officers (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    name VARCHAR(255),
    age INT,
    title VARCHAR(255),
    year_born INT,
    fiscal_year INT,
    total_pay NUMERIC,
    exercised_value NUMERIC,
    unexercised_value NUMERIC,
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.risk_metrics (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    audit_risk INT,
    board_risk INT,
    compensation_risk INT,
    shareholder_rights_risk INT,
    overall_risk INT,
    governance_epoch_date BIGINT,
    compensation_as_of_epoch_date BIGINT,
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.financial_metrics (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    price_hint INT,
    previous_close NUMERIC,
    open_price NUMERIC,
    day_low NUMERIC,
    day_high NUMERIC,
    regular_market_previous_close NUMERIC,
    regular_market_open NUMERIC,
    regular_market_day_low NUMERIC,
    regular_market_day_high NUMERIC,
    dividend_rate NUMERIC,
    dividend_yield NUMERIC,
    ex_dividend_date BIGINT,
    payout_ratio NUMERIC,
    five_year_avg_dividend_yield NUMERIC,
    beta NUMERIC,
    trailing_pe NUMERIC,
    forward_pe NUMERIC,
    volume BIGINT,
    regular_market_volume BIGINT,
    average_volume BIGINT,
    average_volume_10days BIGINT,
    ask NUMERIC,
    market_cap BIGINT,
    fifty_two_week_low NUMERIC,
    fifty_two_week_high NUMERIC,
    price_to_sales_trailing_12_months NUMERIC,
    fifty_day_average NUMERIC,
    two_hundred_day_average NUMERIC,
    trailing_annual_dividend_rate NUMERIC,
    trailing_annual_dividend_yield NUMERIC,
    enterprise_value BIGINT,
    profit_margins NUMERIC,
    float_shares BIGINT,
    shares_outstanding BIGINT,
    held_percent_insiders NUMERIC,
    held_percent_institutions NUMERIC,
    implied_shares_outstanding BIGINT,
    book_value NUMERIC,
    price_to_book NUMERIC,
    last_fiscal_year_end BIGINT,
    next_fiscal_year_end BIGINT,
    most_recent_quarter BIGINT,
    earnings_quarterly_growth NUMERIC,
    net_income_to_common BIGINT,
    trailing_eps NUMERIC,
    forward_eps NUMERIC,
    last_split_factor VARCHAR(10),
    last_split_date BIGINT,
    enterprise_to_revenue NUMERIC,
    enterprise_to_ebitda NUMERIC,
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.analyst_ratings (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    target_high_price NUMERIC,
    target_low_price NUMERIC,
    target_mean_price NUMERIC,
    target_median_price NUMERIC,
    recommendation_mean NUMERIC,
    recommendation_key VARCHAR(50),
    number_of_analyst_opinions INT,
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.dividend_metrics (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    last_dividend_value NUMERIC,
    last_dividend_date BIGINT,
    CONSTRAINT fk_ticker FOREIGN KEY (ticker_id) REFERENCES tickers (id) ON DELETE SET NULL
);