SELECT 
    pin.category,
    geo.country,
    COUNT(pin.ind) AS category_count
    FROM pin
      JOIN geo ON pin.ind = geo.ind
    GROUP BY geo.country, pin.category
    ORDER BY geo.country, category_count DESC;

SELECT 
    pin.category,
    YEAR(geo.timestamp) AS post_year,
    COUNT(pin.ind) AS category_count,
    RANK() OVER (PARTITION BY pin.category ORDER BY COUNT(pin.ind) DESC) AS category_rank
    FROM pin
    JOIN geo ON pin.ind = geo.ind
    GROUP BY category, post_year;

WITH follower_ranks AS 
(SELECT 
    user.user_name AS name,
    geo.country AS country,
    pin.follower_count AS follower_count,
    RANK() OVER (ORDER BY follower_count DESC) AS follower_rank
    FROM user JOIN
    pin ON user.ind = pin.ind JOIN
    geo ON pin.ind = geo.ind)

    SELECT  
        name,
        country,
        AVG(follower_count)
    FROM follower_ranks
        WHERE follower_rank = 1
        GROUP BY name, country;


SELECT 
    CASE WHEN user.age < 18 THEN 'under 18'
         WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
         WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
         WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
         ELSE '50+' END AS age_group,
    pin.category,
    COUNT(pin.category) AS category_count
    FROM pin JOIN user ON user.ind = pin.ind
    GROUP BY age_group, pin.category
    ORDER BY pin.category, age_group;


SELECT 
    CASE WHEN user.age < 18 THEN 'under 18'
         WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
         WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
         WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
         ELSE '50+' END AS age_group,
    MEDIAN(pin.follower_count)
    FROM pin JOIN user ON user.ind = pin.ind
    GROUP BY age_group;


CREATE OR REPLACE TEMPORARY VIEW distinct_users AS
SELECT 
  YEAR(date_joined) AS join_year,
  user_name, 
  date_joined,
  age,
  CASE WHEN user.age < 18 THEN 'under 18'
         WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
         WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
         WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
         ELSE '50+' END AS age_group,
  AVG(pin.follower_count) AS follower_count
  FROM user JOIN
  pin ON pin.ind = user.ind
  GROUP BY user_name, date_joined, age;


SELECT 
  COUNT(user_name) AS number_users_joined,
  join_year
  FROM distinct_users
  GROUP BY join_year
  ORDER BY join_year;


SELECT 
    join_year,
    MEDIAN(follower_count)
    FROM distinct_users
    GROUP BY join_year;


SELECT 
    join_year,
    age_group,
    MEDIAN(follower_count)
    FROM distinct_users
    GROUP BY join_year, age_group
    ORDER BY age_group, join_year;

