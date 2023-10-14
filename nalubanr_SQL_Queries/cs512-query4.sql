-- Find the average length of films in each rating provided that the statements on one of the following lines
-- is true about the film (ie. If any one or more of the following are true, the film should be included
-- in the average for its rating, otherwise it should be excluded)

-- It has a rental rate of less than $2
-- It has a length of less than 110 minutes
-- It is rated G and has a replacement cost of exactly $19.99

-- The results should return the average length in a column named "avg_len" and the rating. It should be ordred by the
-- average length ascending.

SELECT AVG(length) AS avg_len, rating 
FROM `film` 
WHERE (rental_rate < 2 OR length < 110 OR (rating = 'G' AND replacement_cost = 19.99)) 
GROUP BY rating 
ORDER BY avg_len ASC;