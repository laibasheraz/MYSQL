select * from layoffs;
Create table layoffs_staging Like layoffs;
select * from layoffs_staging;
insert layoffs_staging
select * from layoffs;

select *,
row_number() over(
partition by company, industry, location, total_laid_off, percentage_laid_off, 'date') AS row_num
from layoffs_staging;

with duplicate_cte AS
(
select *,
row_number() over(
partition by company, industry, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

with duplicate_cte AS
(
select *,
row_number() over(
partition by company, industry, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
delete from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from layoffs_staging2;

insert into layoffs_staging2 
select *,
row_number() over(
partition by company, industry, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num > 1;

Delete
from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;
update layoffs_staging2
set company = trim(company);
select distinct industry from layoffs_staging2
order by 1;
update layoffs_staging2
set industry = 'crypto' where industry like 'crypto%';
select distinct country from layoffs_staging2;
select distinct country, trim(trailing '.' from country) from layoffs_staging2
order by 1;
update layoffs_staging2
set country = trim(trailing '.' from country) 
where country like 'United%';


select * from layoffs_staging2
where total_laid_off is null AND percentage_laid_off is null;
select industry from layoffs_staging2;
select * from layoffs_staging2
where company = 'Airbnb';
select * from layoffs_staging2 t1
join layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
where (t1.industry is null or t1.industry = '')
AND t2.industry is not null;

update layoffs_staging2 t1
set industry = null
where industry = '';



update layoffs_staging2 t1
join layoffs_staging2 t2
ON t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
AND t2.industry is not null;

select * from layoffs_staging2;
select * from layoffs_staging2
where industry is null 
or industry = '';

select * from layoffs_staging2
where company like 'Bally%';

select * from layoffs_staging2
where total_laid_off is null
AND percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

delete from layoffs_staging2
where total_laid_off is null
AND percentage_laid_off is null;



