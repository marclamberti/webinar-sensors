CREATE TABLE IF NOT EXISTS partners (
    name TEXT,
    execution_date DATE,
    description TEXT,
    processed BOOLEAN,
    PRIMARY KEY(name, execution_date)
);