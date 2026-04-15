WITH candidate AS (
    SELECT
        ky.id AS yacht_id,
        kp.date,
        kp.days,
        JSON_EXTRACT(apiya.api_model_primary_key_parsed, '$.id') AS yacht_api_id,
        JSON_EXTRACT(apilocfrom.api_model_primary_key_parsed, '$.id') AS location_api_id,
        ABS(kp.discount - ROUND(100 - (mp.price * 100 / mp.basePrice), 2)) AS diff
    FROM 1019_yachtic2.yachts AS ky
    JOIN 1019_yachtic2.models AS km
        ON km.id = ky.model_id
    JOIN 1019_yachtic2.prices AS kp
        ON kp.yacht_id = ky.id
    JOIN 1019_yachtic2.companies
        ON companies.id = ky.company_id
    JOIN 1019_yachtic2.locations
        ON locations.id = ky.location_id
    JOIN 1019_yachtic2.abeon_permalinks
        ON abeon_permalinks.model_id = ky.id
       AND abeon_permalinks.model = 'yachts'
    JOIN 1019_yachtic2_stores_mmk.mmk_resources AS my
        ON CONCAT('mmk:', my.source_record_id) = ky.externalIdentity
    JOIN 1019_yachtic2_stores_mmk.mmk_searchresultsfilter AS mp
        ON mp.resource_id = my.source_record_id
       AND mp.datefrom = kp.date
       AND kp.location_from_id = kp.location_to_id
       AND mp.baseid = mp.basetoid
       AND kp.days = mp.days
    JOIN 1019_yachtic2_stores_mmk.mmk_bases
        ON mmk_bases.source_record_id = mp.baseid
    JOIN 1019_yachtic2.apiConnections AS apiya
        ON CONCAT('{"id":', ky.id, '}') = apiya.model_primary_key_parsed
       AND apiya.api_model_name = 'yachtsApi'
    JOIN 1019_yachtic2.apiConnections AS apilocfrom
        ON CONCAT('{"id":', kp.location_from_id, '}') = apilocfrom.model_primary_key_parsed
       AND apilocfrom.api_model_name = 'LocationsApi'
    WHERE kp.days = 7
      AND mp.days = 7
      AND DAYOFWEEK(kp.date) = 7
      AND (
            (kp.date >= STR_TO_DATE(CONCAT(YEAR(CURDATE()), '-04-01'), '%Y-%m-%d')
             AND kp.date < STR_TO_DATE(CONCAT(YEAR(CURDATE()), '-11-01'), '%Y-%m-%d'))
         OR (kp.date >= STR_TO_DATE(CONCAT(YEAR(CURDATE()) + 1, '-04-01'), '%Y-%m-%d')
             AND kp.date < STR_TO_DATE(CONCAT(YEAR(CURDATE()) + 1, '-11-01'), '%Y-%m-%d'))
      )
      AND kp.available = 1
      AND ky.status = 10
      AND ky.removedByApi = 0
      AND ky.externalIdentity LIKE 'mmk%'
      AND km.status = 10
      AND km.removedByApi = 0
      AND companies.status = 10
      AND companies.removedByApi = 0
      AND locations.status = 10
      AND locations.removedByApi = 0
      AND abeon_permalinks.status = 10
      AND my.cabins > 0
      AND my.servicetype <> 'Cabin'
      AND (my.berths > 0 OR my.maxpeopleonboard > 0)
      AND my.heads > 0
      AND my.id NOT IN (8875,31715,31716,31727,31836,31967,33100,33706,34007)
      AND my.source_record_id NOT IN (2677060741201186,904262600000101186,5224891516401186,3776361056801186,780870060000101186)
      AND mmk_bases.id IS NOT NULL
      AND mp.basePrice <> 0
),
bad_groups AS (
    SELECT
        yacht_id,
        date,
        days
    FROM candidate
    GROUP BY yacht_id, date, days
    HAVING MIN(diff) > 0.2
)
SELECT DISTINCT
    CONCAT(
        'php yii synchronizer/prices/one ',
        c.yacht_api_id, ',', c.date, ',', c.days, ',', c.location_api_id, ',', c.location_api_id, ';'
    ) AS synch_cen
FROM candidate AS c
JOIN bad_groups AS bg
    ON bg.yacht_id = c.yacht_id
   AND bg.date = c.date
   AND bg.days = c.days
WHERE c.diff > 0.2
LIMIT 100000;
