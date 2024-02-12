CREATE TABLE IF NOT EXISTS hotel_analysis_table(
    analysis_id SERIAL NOT NULL PRIMARY KEY,
    reservation_id integer,
    full_name varchar,
    reservation_date timestamp,
    start_date timestamp,
    end_date timestamp,
    total_price numeric,
    provider varchar,
    payment_status varchar,
    created_at timestamp NOT NULL DEFAULT NOW()
);