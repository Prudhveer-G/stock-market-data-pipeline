CREATE TABLE IF NOT EXISTS stock_prices (
    ticker VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    volume BIGINT,
    PRIMARY KEY (ticker, trade_date)
);
