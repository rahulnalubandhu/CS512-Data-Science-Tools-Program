-- Select the first name, last name and film title of all films an actor with the last name Cage acted in.
-- Order by the film title, descending. There should be one row for every actor/film pair.

SELECT actor.first_name, actor.last_name, film.title 
FROM `actor`
INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id 
INNER JOIN film ON film_actor.film_id = film.film_id 
WHERE actor.last_name = 'Cage' 
ORDER BY film.title DESC;