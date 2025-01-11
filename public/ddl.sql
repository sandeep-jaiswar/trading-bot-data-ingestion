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

-- CREATE TABLE IF NOT EXISTS public.financials (
--     id SERIAL PRIMARY KEY,
--     ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
--     market_cap DECIMAL(20, 2),
--     ebitda DECIMAL(20, 2),
--     pe_ratio DECIMAL(10, 2),
--     peg_ratio DECIMAL(10, 2),
--     book_value DECIMAL(10, 2),
--     dividend_per_share DECIMAL(10, 2),
--     dividend_yield DECIMAL(10, 4),
--     eps DECIMAL(10, 2),
--     revenue_per_share DECIMAL(10, 2),
--     profit_margin DECIMAL(10, 4),
--     operating_margin DECIMAL(10, 4),
--     return_on_assets DECIMAL(10, 4),
--     return_on_equity DECIMAL(10, 4),
--     revenue DECIMAL(20, 2),
--     gross_profit DECIMAL(20, 2),
--     diluted_eps DECIMAL(10, 2),
--     quarterly_earnings_growth DECIMAL(10, 4),
--     quarterly_revenue_growth DECIMAL(10, 4)
-- );

-- CREATE TABLE IF NOT EXISTS public.analyst_ratings (
--     id SERIAL PRIMARY KEY,
--     ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
--     analyst_target_price DECIMAL(10, 2),
--     strong_buy INT,
--     buy INT,
--     hold INT,
--     sell INT,
--     strong_sell INT
-- );

-- CREATE TABLE IF NOT EXISTS public.stock_data (
--     id SERIAL PRIMARY KEY,
--     ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
--     trailing_pe DECIMAL(10, 2),
--     forward_pe DECIMAL(10, 2),
--     price_to_sales_ratio DECIMAL(10, 2),
--     price_to_book_ratio DECIMAL(10, 2),
--     ev_to_revenue DECIMAL(10, 2),
--     ev_to_ebitda DECIMAL(10, 2),
--     beta DECIMAL(10, 2),
--     week_52_high DECIMAL(10, 2),
--     week_52_low DECIMAL(10, 2),
--     day_50_moving_avg DECIMAL(10, 2),
--     day_200_moving_avg DECIMAL(10, 2)
-- );

-- CREATE TABLE IF NOT EXISTS public.dividend_dates (
--     id SERIAL PRIMARY KEY,
--     ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
--     dividend_date DATE,
--     ex_dividend_date DATE
-- );



