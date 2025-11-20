CREATE TABLE IF NOT EXISTS imdb (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    year VARCHAR(50),
    duration VARCHAR(50),
    genre VARCHAR(255),
    rating NUMERIC,
    votes NUMERIC
);

-- Import from CSV
COPY imdb(title, year, duration, genre, rating, votes)
FROM 'imdb.csv'
CSV HEADER
DELIMITER ';';
