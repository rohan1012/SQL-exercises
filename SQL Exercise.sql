-----Food Service Visits-----

----Pull 10 visits to food service industry----

SELECT *
FROM da.clean_visits_201607_present a
LEFT JOIN da.brand_table b
ON a.brand = b.brand
WHERE b.vertical = 'Food Service'
LIMIT 10;

----Pull 10 visits to food service industry from January 2018----

SELECT *
FROM da.clean_visits_201607_present a
LEFT JOIN da.brand_table b
ON a.brand = b.brand
WHERE b.vertical = 'Food Service'
AND a.visit_month > '2018-01-01'
LIMIT 10;

----Pull 10 visits to food service industry from January 2018 for the highest android sdk that existed in January----

SELECT *
FROM da.brand_table a
LEFT JOIN da.clean_visits_201607_present b
ON a.brand = b.brand
WHERE a.vertical = 'Food Service'
AND a.date_added > '2018-01-01'
AND b.sdk_version IN (
    SELECT
           MAX(b.sdk_version)
    FROM b
    WHERE DATE_PART(month, b.visit_date) = '01'
    AND b.platform = '2')
LIMIT 10;

----What is the average weekly volume (visits/week) for Wendy’s in 2017----

SELECT
       CAST(AVG(weekly_count) AS FLOAT) AS average_weekly_volume
FROM (
        SELECT
             visit_week,
             COUNT(*) AS weekly_count
        FROM da.clean_visits_201607_present
        WHERE brand = 'Wendy''s'
        AND DATE_PART('year', visit_week) = '2017'
      GROUP BY visit_week
      ORDER BY visit_week) AS average_temp;

----How many visits occur per month per platform to the food service industry (across all months in 2017)----

SELECT
    date_part(month, a.visit_date) AS month,
    a.platform,
    COUNT(*) AS total_visits
FROM da.clean_visits_201607_present a
LEFT JOIN da.brand_table b
ON a.brand = b.brand
WHERE b.vertical = 'Food Service'
AND date_part(year, a.visit_date) = '2017'
GROUP BY date_part(month, a.visit_date),
         a.platform
ORDER BY date_part(month, a.visit_date),
         a.platform;

----How many visits to food service industry occur by application on average per day in the duration of recent three months in our network----

SELECT DISTINCT
    a.app_id,
    date_part(day, a.visit_date) AS date,
    COUNT(*) AS total_visits,
    RANK() OVER (ORDER BY total_visits DESC) AS app_rank
FROM da.clean_visits_201607_present a
LEFT JOIN da.brand_table b
ON a.brand = b.brand
WHERE b.vertical = 'Food Service'
AND (a.visit_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month')
    AND a.visit_date < DATE_TRUNC('month', CURRENT_DATE))
GROUP BY a.app_id,
         date;

----Pull food service visit information for users that have 15 days of data per month in any of the last three full months----

SELECT *
FROM da.clean_visits_201607_present
WHERE user_id IN (
                    SELECT DISTINCT
                        user_id
                    FROM (
                            SELECT
                               user_id,
                               visit_month,
                               days_data,
                               CASE
                                   WHEN days_data >= 15
                                       THEN 1
                                   ELSE 0
                                END AS minimum_requirement
                            FROM da.user_days_per_month
                            WHERE visit_month >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month')
                                                    AND visit_month < DATE_TRUNC('month', CURRENT_DATE)) temp
                    GROUP BY user_id
                    HAVING SUM(minimum_requirement) >= 1);

----Pull food service visit information for users that have 15 days of data per month in each of the last three full months (user should meet requirement in all three months)----

SELECT *
FROM da.clean_visits_201607_present
WHERE user_id IN (
                    SELECT DISTINCT
                        user_id
                    FROM (
                            SELECT
                               user_id,
                               visit_month,
                               days_data,
                               CASE
                                   WHEN days_data >= 15
                                       THEN 1
                                   ELSE 0
                                END AS minimum_requirement
                            FROM da.user_days_per_month
                            WHERE visit_month >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month')
                                                    AND visit_month < DATE_TRUNC('month', CURRENT_DATE)) temp
                    GROUP BY user_id
                    HAVING SUM(minimum_requirement) >= 3);

----Create a temporary table with the number of visits per week per brand----

CREATE TEMPORARY TABLE
    brand_visits_per_week
    AS (
        WITH top_brands
            AS (
                SELECT
                    brand,
                    CASE
                    WHEN brand_ranking <= 100
                        THEN 1
                    ELSE 0
                    END AS top_brand
                FROM (
                        SELECT
                            brand,
                            RANK() OVER (ORDER BY COUNT(*) DESC) AS brand_ranking
                        FROM da.clean_visits_201607_present
                        GROUP BY brand) AS brand_visits_ranking)
    SELECT
        temp.brand,
        visit_week,
        visits_per_week,
        top_brand
    FROM (
            SELECT
                 brand,
                 visit_week,
                 COUNT(*) AS visits_per_week
            FROM da.clean_visits
            GROUP BY brand,
                    visit_week
            ORDER BY brand,
                    visit_week) temp
LEFT JOIN top_brands
    ON top_brands.brand = temp.brand
ORDER BY temp.brand,
        visit_week);

SELECT *
FROM brand_visits_per_week
LIMIT 10;

-----Digging Deeper into DA Schema-----

----Calculate share of visits for all brands for each month from 2019----

SELECT
       brand,
       visit_month,
       (visits/CAST (total_monthly_visits AS FLOAT)) AS monthly_share_of_visit
FROM (
         SELECT
                brand,
                visit_month,
                COUNT(*) AS visits,
                SUM(COUNT(*)) OVER (PARTITION BY visit_month) AS total_monthly_visits
         FROM da.clean_visits_201607_present
         WHERE DATE_PART(year, visit_month) = '2019'
         GROUP BY brand,
                  visit_month) temp
ORDER BY visit_month,
         monthly_share_of_visit DESC;

--Add the count of visits and total visits included to the output from #10--

SELECT
       brand,
       visit_month,
       visits,
       total_monthly_visits,
       (visits/CAST (total_monthly_visits AS FLOAT)) AS monthly_share_of_visit
FROM (
         SELECT
                brand,
                visit_month,
                COUNT(*) AS visits,
                SUM(COUNT(*)) OVER (PARTITION BY visit_month) AS total_monthly_visits
         FROM da.clean_visits_201607_present
         WHERE DATE_PART(year, visit_month) = '2019'
         GROUP BY brand,
                  visit_month) temp
ORDER BY visit_month,
         monthly_share_of_visit DESC;

----Pull share of visits for all burger brands for each month from 2019--

SELECT
       brand,
       visit_month,
       visits,
       total_monthly_visits,
       (visits/CAST (total_monthly_visits AS FLOAT)) AS monthly_share_of_visit
FROM (
         SELECT
                a.brand,
                a.visit_month,
                COUNT(*) AS visits,
                SUM(COUNT(*)) OVER (PARTITION BY visit_month) AS total_monthly_visits
         FROM da.clean_visits_201607_present a
         LEFT JOIN da.brand_table b
         ON a.brand = b.brand
         WHERE DATE_PART(year, visit_month) = '2019'
         AND b.flavor LIKE '%Burger%'
         GROUP BY a.brand,
                  a.visit_month) temp
ORDER BY visit_month,
         monthly_share_of_visit DESC;

----Pull all visit information for users that have more than 2 visits per month to 7-Eleven----

SELECT *
FROM da.clean_visits_201607_present
WHERE user_id IN (
                    SELECT DISTINCT
                        user_id
                     FROM (
                            SELECT
                                user_id,
                                visit_month,
                                COUNT(*) AS seven_eleven_monthly_visits
                            FROM da.clean_visits_201607_present
                            WHERE brand = '7-Eleven'
                            GROUP BY user_id,
                                    visit_month
                            HAVING seven_eleven_monthly_visits > 2) AS seven_eleven_temp);

----Pull number of visits per day to 7-Eleven only for the users that have more than 2 visits per month to 7-Eleven----

SELECT
    visit_date,
    COUNT(*) AS seven_eleven_daily_visits
FROM da.clean_visits_201607_present
WHERE user_id IN (
                    SELECT DISTINCT
                        user_id
                     FROM (
                            SELECT
                                user_id,
                                visit_month,
                                COUNT(*) AS seven_eleven_monthly_visits
                            FROM da.clean_visits_201607_present
                            WHERE brand = '7-Eleven'
                            GROUP BY user_id,
                                    visit_month
                            HAVING seven_eleven_monthly_visits > 2) AS seven_eleven_temp)
GROUP BY visit_date
ORDER BY visit_date;

----Pull the count of visits for the list of LSR brands----

--By using a JOIN--

SELECT
       a.brand,
       COUNT(*) AS visits
FROM da.clean_visits_201607_present a
LEFT JOIN da.brand_table b
ON a.brand = b.brand
WHERE market = 'LSR'
GROUP BY a.brand
ORDER BY a.brand;

--BY using IN--

SELECT
       brand,
       COUNT(*) AS visits
FROM da.clean_visits_201607_present
WHERE brand IN(
                SELECT
                    brand
                FROM da.brand_table
                WHERE market = 'LSR')
GROUP BY brand
ORDER BY brand;

--BY using EXISTS--

SELECT
        a.brand,
        COUNT(*) AS visits
FROM da.clean_visits_201607_present a
WHERE EXISTS(
                SELECT a.brand
                FROM da.brand_table b
                WHERE a.brand = b.brand
                AND b.market = 'LSR')
GROUP BY a.brand
ORDER BY a.brand;

----Create a temporary table with the number of visits per week per brand. Include a column for top_brand that is equal to 1 when the brand has an industry rank and 0 otherwise----

CREATE TEMPORARY TABLE
    top_brands_visits_per_week
    AS (
        SELECT
            temp.brand,
            visit_week,
            weekly_visits,
        CASE
        WHEN foot_traffic_industry_rank IS NOT NULL
        OR transaction_industry_rank IS NOT NULL
        THEN 1
        ELSE 0
        END AS top_brands
        FROM (
                SELECT
                    a.brand,
                    visit_week,
                    COUNT(*) AS weekly_visits
                FROM da.clean_visits_201607_present a
                LEFT JOIN da.brand_table b
                ON a.brand = b.brand
                GROUP BY a.brand,
                         visit_week
                ORDER BY a.brand,
                         visit_week) temp
    LEFT JOIN da.brand_table c
    ON temp.brand = c.brand
    ORDER BY brand,
             visit_week);

SELECT *
FROM top_brands_visits_per_week
LIMIT 10;

----Use an update statement to update the table from #17 to set top brand = 1 based on the accuracy_top_brands_by_volume table (top brands should include only those on the accuracy_top_brands_by_volume table)----

--Using a CASE statement--

UPDATE
    top_brands_visits_per_week
SET
    top_brands =
        CASE
        WHEN brand IN (
                        SELECT brand FROM da.accuracy_top_brands_by_volume)
        THEN 1
        ELSE 0
        END;

--Using IN statement--

UPDATE
    top_brands_visits_per_week
SET
    top_brands = 0;
UPDATE
    top_brands_visits_per_week
SET
    top_brands = 1
WHERE brand IN(
                SELECT
                    brand
                FROM da.accuracy_top_brands_by_volume);

--Using JOIN--

UPDATE
    top_brands_visits_per_week
SET
    top_brands = 0;
UPDATE
    top_brands_visits_per_week
SET
    top_brands = 1
FROM da.accuracy_top_brands_by_volume a
INNER JOIN top_brands_visits_per_week b
ON a.brand = b.brand;

-----Understanding all Visits-----

----Pull a list of users that were on a single sdk for the past 6 months----

SELECT DISTINCT
    user_id,
    sdk_version
FROM(
        SELECT
            user_id,
            sdk_version,
            MIN(sdk_version) AS base_sdk,
            MAX(sdk_version) AS top_sdk
        FROM da.clean_visits_201607_present
        WHERE (visit_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 month')
                AND visit_date < DATE_TRUNC('month', CURRENT_DATE))
        GROUP BY  user_id,
                  sdk_version) temp
WHERE base_sdk = top_sdk;

----Create a temporary table with the number of days of data a user had each month----

CREATE TEMPORARY TABLE
        user_monthly_data
        AS (
            SELECT
                user_id,
                DATE_TRUNC('month', visit_date) AS visit_month,
                COUNT(*) AS monthly_visits
            FROM(
                SELECT DISTINCT
                    user_id,
                    visit_date
                FROM da.clean_visits_201607_present) temp
            GROUP BY visit_month,
                     user_id
            ORDER BY visit_month);

SELECT *
FROM  user_monthly_data
LIMIT 10;

----Use temp table from #20 to pull list of users that were on a single sdk for the last 6 months and have at least 15 days of data in each of the months----

SELECT DISTINCT
    user_id,
    sdk_version,
    visit_month
FROM(
        SELECT
            user_id,
            sdk_version,
            visit_month,
            MIN(sdk_version) AS base_sdk,
            MAX(sdk_version) AS top_sdk
        FROM da.clean_visits_201607_present
        WHERE (visit_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 month')
                AND visit_date < DATE_TRUNC('month', CURRENT_DATE))
        GROUP BY  user_id,
                  sdk_version,
                  visit_month) temp
WHERE base_sdk = top_sdk
AND user_id IN(
                SELECT
                    user_id
                FROM user_monthly_data
                WHERE monthly_data > 15);

----Pull all visits for a single user using a Samsung model SM-G925P (HINT: cobra.user_data)----

SELECT *
FROM cobra.visits
WHERE user_id IN(
                    SELECT
                        user_id
                    FROM cobra.user_data
                    WHERE device_model LIKE '%SM-G925P%'
    );

----How many visits are filtered out each month for each of the different filter rules (last 12 months only)----

SELECT
    visit_month,
    filtered_rule,
    COUNT(*) AS visit_count
FROM(
        SELECT
            DATE_TRUNC('month', arrival_local) AS visit_month,
            filtered_rule
        FROM cobra.visits_filtered
        WHERE arrival_local >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 month')
        AND arrival_local < DATE_TRUNC('month', CURRENT_DATE)
        ) temp
GROUP BY visit_month,
         filtered_rule
ORDER BY visit_month,
         filtered_rule;

----How many total visits were there in May 2017 and how many did we register (loaded to visits table) in May 2017----

SELECT
    visits_201705 + filtered_visits_201705 AS total_visits,
    visits_201705 AS registered_visits
FROM(
        SELECT(
                  SELECT
                         COUNT(*)
                  FROM cobra.visits_201705
              ) visits_201705,
              (
                  SELECT
                         COUNT(*)
                  FROM cobra.visits_filtered_201705
              ) filtered_visits_201705
        ) temp;

--Understanding the surveys tables in Cobra and Surveys schema--

----Calculate the % of surveys in November 2019 that were completed----

SELECT
    completed_surveys,
    total_surveys,
    (CAST(completed_surveys AS FLOAT) / total_surveys) * 100 AS completed_surveys_percentage_201911
FROM(
        SELECT(
                SELECT
                    COUNT (*)
                FROM cobra.surveys_201911
                WHERE completed_timestamp_utc IS NOT NULL
                  ) completed_surveys,
               (
                SELECT
                   COUNT (*)
                FROM cobra.surveys_201911
                WHERE completed_timestamp_utc IS NOT NULL
                OR expiry_date <= CURRENT_DATE
                   )total_surveys
        ) temp;

----Calculate the % of surveys in November 2019 that were terminated----

SELECT
    terminated_surveys,
    total_surveys,
    (CAST(terminated_surveys AS FLOAT) / total_surveys) * 100 AS terminated_surveys_percentage_201911
FROM(
        SELECT(
                SELECT
                    COUNT (*)
                FROM cobra.surveys_201911
                WHERE terminated_timestamp_utc IS NOT NULL
                  ) terminated_surveys,
               (
                SELECT
                   COUNT (*)
                FROM cobra.surveys_201911
                WHERE completed_timestamp_utc IS NOT NULL
                OR expiry_date <= CURRENT_DATE
                   )total_surveys
        ) temp;

----Calculate the % of surveys in November 2019 that were viewed but not completed or terminated----

SELECT
    viewed_surveys,
    total_surveys,
    (CAST(viewed_surveys AS FLOAT) / total_surveys) * 100 AS viewed_surveys_percentage_201911
FROM(
        SELECT(
                SELECT
                    COUNT (*)
                FROM cobra.surveys_201911
                WHERE terminated_timestamp_utc IS NULL
                AND completed_timestamp_utc IS NULL
                AND viewed_timestamp_utc IS NOT NULL
                  ) viewed_surveys,
               (
                SELECT
                   COUNT (*)
                FROM cobra.surveys_201911
                WHERE completed_timestamp_utc IS NOT NULL
                OR expiry_date <= CURRENT_DATE
                   )total_surveys
        ) temp;

----Pull all survey responses from November 2019 where the question was “What is your gender?”----

SELECT
    survey_id,
    user_id,
    answer_id
FROM cobra.survey_responses_201911
WHERE question = 'What is your gender?'
AND survey_platform = 'Typeform'
ORDER BY survey_id;

----How many people in November 2019 responded that they were Female vs Male vs some other response----


SELECT
    answer_id,
    COUNT (*) AS count
FROM cobra.survey_responses_201911
WHERE question = 'What is your gender?'
GROUP BY answer_id;

----Pull the list of all survey apps, which platform they are on and which publisher they belong to----

SELECT *
FROM surveys.survey_apps;

----For all publishers, pull what the completion cost and terminate cost would be for a survey that is currently running that was 6 minutes long (loi = 6) and where terminates do not count as completes----

SELECT
    app_publisher,
    SUM(complete_cost * 6) AS total_complete_cost,
    SUM(terminate_cost * 6) AS total_terminate_cost
FROM surveys.app_publisher_costs
WHERE loi_min <= 6
AND loi_max >= 6
AND count_terminate_as_complete = FALSE
AND survey_date_til >= CURRENT_DATE
GROUP BY app_publisher;

----Related to the question above, which publisher has the most expensive completion cost----

SELECT
    app_publisher,
    SUM(complete_cost * 6) AS total_complete_cost
FROM surveys.app_publisher_costs
WHERE loi_min <= 6
AND loi_max >= 6
AND count_terminate_as_complete = FALSE
AND survey_date_til >= CURRENT_DATE
GROUP BY app_publisher
ORDER BY total_complete_cost DESC
LIMIT 1;