-- DATA CLEANING

SELECT *
FROM layoffs;

-- 1 Removing duplicates
-- 2 Standardize the data
-- 3 Null values and blank values
-- 4 Removing any columns or row

-- 1 Removing duplicates
-- creating a duplicate table like layoff for data cleaning

CREATE TABLE layoffs_duplicate
LIKE  layoffs;

INSERT INTO layoffs_duplicates
SELECT *
FROM layoffs;

-- creating a row number to check out for duplicate in the dataset

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,location,total_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicates;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,location,total_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicates)
SELECT *
FROM duplicate_cte
WHERE row_num >1;
-- discover some duplicate in the data and we are going to delete them from the table
-- creating another table with the same data in other to delete the duplicate 


CREATE TABLE `layoffs_duplicates2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_duplicates2
SELECT *
FROM layoffs_duplicates;

-- filtering the layoffs_duplicate2 to check for duplicate and delete it

SELECT *
FROM layoffs_duplicates2
WHERE row_num >1;

DELETE
FROM layoffs_duplicates2
WHERE row_num >1;


-- 2 Standardize data

SELECT DISTINCT industry
FROM layoffs_duplicates2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_duplicates2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry
FROM layoffs_duplicates2;

SELECT DISTINCT country
FROM layoffs_duplicates2
WHERE country LIKE 'united%';

UPDATE layoffs_duplicates2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT company,
TRIM(company)
FROM layoffs_duplicates2;

UPDATE layoffs_duplicates2
SET company = TRIM(company);

-- Modifying the date columns from text to date 

SELECT date,
STR_TO_DATE(date,'%m/%d/%Y')
FROM layoffs_duplicates2;

UPDATE layoffs_duplicates2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_duplicates2
MODIFY COLUMN date  DATE;

-- 3 Null values and blank values

SELECT *
FROM layoffs_duplicates2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL
AND funds_raised_millions IS NULL;


SELECT *
FROM layoffs_duplicates2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_duplicates2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_duplicates2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_duplicates2 AS t1
JOIN layoffs_duplicates2 AS t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_duplicates2 AS t1
JOIN layoffs_duplicates2 AS t2
ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Removing any columns or row

SELECT *
FROM layoffs_duplicates2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_duplicates2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Dropping the row_num column
ALTER TABLE layoffs_duplicates2
DROP COLUMN row_num;


-- EXPLORATORY DATA ANALYSIS

SELECT *
FROM layoffs_duplicates2;

-- Determining total number of laid off per country

SELECT country,
SUM( total_laid_off) AS total
FROM layoffs_duplicates2
GROUP BY country
ORDER BY total DESC;

-- Determining the fund raised per country

SELECT country,
SUM( funds_raised_millions) AS total
FROM layoffs_duplicates2
GROUP BY country
ORDER BY total DESC;

-- Determining the maximum and minimum layoffs and percentage layoffs

SELECT MAX(total_laid_off) AS max_laid_offs,
MAX(percentage_laid_off) AS max_percent
FROM layoffs_duplicates2;

SELECT MIN(total_laid_off) AS min_laid_offs,
MIN(percentage_laid_off) AS min_percent
FROM layoffs_duplicates2;

-- Determining company per laid offs

SELECT company,
SUM( total_laid_off) AS total
FROM layoffs_duplicates2
GROUP BY 1
ORDER BY 2 DESC;

-- Determining the company that had 100 percent laidoff(pack up)

SELECT *
FROM layoffs_duplicates2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_duplicates2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Determining maximum date and minimum date and total laid off per year

SELECT MAX(date) AS max_date,
MIN(date) AS min_date
FROM layoffs_duplicates2;

SELECT YEAR(date) AS 'year',
SUM(total_laid_off)
FROM layoffs_duplicates2
GROUP BY YEAR(date);

-- Determining total laid off per month

SELECT SUM(total_laid_off) AS sum_total_laid_off,
SUBSTRING(date,6,2) AS month
FROM layoffs_duplicates2
GROUP BY month
ORDER BY sum_total_laid_off DESC;

-- Determining number of company that pack up and the number of company still in business

SELECT company,
percentage_laid_off,
(CASE 
WHEN percentage_laid_off = 1 THEN 'pack up'
ELSE 'still_in_business'
END) AS company_status
FROM layoffs_duplicates2
ORDER BY 2 DESC;

WITH company_calc AS
(SELECT company,
percentage_laid_off,
(CASE 
WHEN percentage_laid_off = 1 THEN 'pack up'
ELSE 'still_in_business'
END) AS company_status
FROM layoffs_duplicates2)
SELECT company_status,
COUNT(company_status) 
FROM company_calc
GROUP BY company_status;
-- 1876 company are still in business and 115 have pack up already

-- Determing the rank in industry per laid offs

WITH industry_rank(industry,years,sum_total_laid_offs) AS
(SELECT industry, YEAR(date) ,SUM(total_laid_off)
FROM layoffs_duplicates2
GROUP BY industry, YEAR(date) )
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY sum_total_laid_offs DESC) AS ranking
FROM industry_rank
WHERE years IS NOT NULL
ORDER BY ranking ;


