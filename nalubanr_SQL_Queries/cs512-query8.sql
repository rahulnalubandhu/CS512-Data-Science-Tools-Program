-- Insert Anastasia Dualla from Sagittaron into the bsg_people table. You should use a subquery to do this.

INSERT INTO `bsg_people` (fname , lname , homeworld) 
VALUES ('Anastasia' , 'Dualla' , (SELECT id FROM `bsg_planets` WHERE name = 'Sagittaron'));

--here homeworld has a sub query where it selects the id =7 from bsg_planets which has a planet name Sagittaron