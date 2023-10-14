
-- What is the relationship between the average rating of a restaurant and the number of reviews it has received?

-- Do restaurants with more reviews have a higher average stars or is there no significant relationship?

SELECT D.business_id, D.name, D.state, D.city, SUM(R.review_count) AS ReviewCount, AVG(R.stars) AS AverageStars
FROM business_details AS D
JOIN business_review AS R
ON D.business_id = R.business_id
GROUP BY D.business_id, D.name, D.state, D.city;

