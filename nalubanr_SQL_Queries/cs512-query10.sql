-- Update the bsg_people table. Set anyone named Gaius Baltar to be from Caprica. This should use a subquery.

UPDATE `bsg_people`
SET homeworld = (SELECT id FROM `bsg_planets` WHERE bsg_planets.name = 'Caprica')
WHERE fname = 'Gaius' AND lname = 'Baltar';



--below is the query that i have written to see if that is updating as it is replacing the value so i gave a different planet name 
--this is just to check if the above query is working or not.
--UPDATE `bsg_people`
--SET homeword = (SELECT id FROM `bsg_planets` WHERE bsg_planets.name = 'Leonis') this sets the value to 2 
--WHERE fname = 'Gaius' AND lname = 'Baltar'; 