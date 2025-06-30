SELECT *
FROM layoffs_stagging;
-- REMOVING DUPLICATES

CREATE TABLE layoffs_stagging
LIKE layoffs;

INSERT layoffs_stagging
SELECT * 
FROM layoffs; 


# 1. Removing Duplicates 
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off) AS row_num 
FROM layoffs_stagging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country, funds_raised_millions) AS row_num 
FROM layoffs_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_stagging
WHERE company = "Casper";

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage,country, funds_raised_millions) AS row_num 
FROM layoffs_stagging;

SELECT *
FROM layoffs_stagging2
;

-- STANDARDIZING DATA
SELECT DISTINCT industry
FROM layoffs_stagging2
;

UPDATE layoffs_stagging2
SET company = TRIM(company)
;

UPDATE layoffs_stagging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";


UPDATE layoffs_stagging2
SET country = "United States"
WHERE country LIKE "%.";

SELECT DISTINCT country
FROM layoffs_stagging2
ORDER BY 1;

SELECT * 
FROM layoffs_stagging2
WHERE company  LIKE 'Bally%'
;

SELECT `date`
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` Date ;

SELECT t1.industry, t2.industry
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

-- NULL checking statement
SELECT *
FROM layoffs_stagging2
WHERE (industry = '' OR industry IS NULL);


SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;












