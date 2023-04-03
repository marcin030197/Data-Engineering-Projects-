CREATE OR REPLACE VIEW project.average_life_expectancy
AS SELECT countries.country,
    avg(countries.healthy_life_expectancy_at_birth) AS avg_healthy_life_expectancy_at_birth
   FROM project.countries
  WHERE countries.healthy_life_expectancy_at_birth IS NOT NULL
  GROUP BY countries.country
  ORDER BY (avg(countries.healthy_life_expectancy_at_birth)) DESC;