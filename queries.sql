-- Top 5 genres by total votes and average rating
WITH genre_split AS (
    SELECT unnest(string_to_array(genre, ',')) AS g, rating, votes
    FROM imdb
)
SELECT TRIM(g) AS genre,
       AVG(rating) AS avg_rating,
       SUM(votes) AS total_votes
FROM genre_split
GROUP BY TRIM(g)
ORDER BY total_votes DESC
LIMIT 5;

-- Years with the most well liked movies
SELECT
    SUBSTRING(year FROM '\((\d{4})') AS release_year,
    COUNT(*) AS well_liked_count
FROM imdb
WHERE rating >= 80 AND votes >= 1000
GROUP BY release_year
ORDER BY well_liked_count DESC
LIMIT 10;

-- Duration ranges with average rating, total movies, total votes
SELECT
    CASE
        WHEN CAST(split_part(duration, ' ', 1) AS INT) <= 60 THEN '0-60 min'
        WHEN CAST(split_part(duration, ' ', 1) AS INT) <= 120 THEN '61-120 min'
        WHEN CAST(split_part(duration, ' ', 1) AS INT) <= 180 THEN '121-180 min'
        ELSE '181+ min'
    END AS duration_range,
    AVG(rating) AS avg_rating,
    COUNT(*) AS total_movies,
    SUM(votes) as total_votes
FROM imdb
GROUP BY duration_range
ORDER BY duration_range;
