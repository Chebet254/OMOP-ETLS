--CREATE AND EXECUTE DDL TO CREATE OMOP TABLES 
--ALTER TABLES TO DROP CONSTRAINTS 
--REMEMBER TO ADD CONSTRAINTS AFTER ADDING SOURCE DATA
ALTER TABLE public.concept DROP CONSTRAINT fpk_concept_domain_id;
ALTER TABLE public.concept DROP CONSTRAINT fpk_concept_vocabulary_id;
ALTER TABLE public.concept DROP CONSTRAINT fpk_concept_concept_class_id;
ALTER TABLE public.concept_relationship DROP CONSTRAINT fpk_concept_relationship_relationship_id;
ALTER TABLE public.concept_relationship DROP CONSTRAINT fpk_concept_relationship_concept_id_2;
ALTER TABLE public.concept_relationship DROP CONSTRAINT fpk_concept_relationship_concept_id_1;
ALTER TABLE public.CONCEPT_ANCESTOR DROP CONSTRAINT fpk_concept_ancestor_ancestor_concept_id;
ALTER TABLE public.CONCEPT_ANCESTOR DROP CONSTRAINT fpk_concept_ancestor_descendant_concept_id;
ALTER TABLE public.CONCEPT_SYNONYM DROP CONSTRAINT fpk_concept_synonym_concept_id;

--LOAD OMOP VOCABS 
COPY PUBLIC.DRUG_STRENGTH FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\DRUG_STRENGTH.csv' 
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b';

COPY PUBLIC.CONCEPT FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\CONCEPT.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b';

COPY public.CONCEPT_RELATIONSHIP FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\CONCEPT_RELATIONSHIP.csv' 
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b';

COPY public.CONCEPT_ANCESTOR FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\CONCEPT_ANCESTOR.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b';

COPY public.CONCEPT_SYNONYM FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\CONCEPT_SYNONYM.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b' ;

COPY public.VOCABULARY FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\VOCABULARY.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b' ;

COPY public.RELATIONSHIP FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\RELATIONSHIP.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b' ;

COPY public.CONCEPT_CLASS FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\CONCEPT_CLASS.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b' ;

COPY PUBLIC.DOMAIN FROM 'D:\APHRC\LHS\OMOP ETL\vocabulary_download_v5\DOMAIN.csv'
WITH DELIMITER E'\t' 
CSV HEADER QUOTE E'\b' ;

--INSERT DATA TO OMOP FROM SOURCE DATA
--REMOVE CONSTRAINTS
ALTER TABLE public.person DROP CONSTRAINT fpk_person_location_id;

INSERT INTO public.person
(
person_id,
gender_concept_id,
year_of_birth,
month_of_birth,
day_of_birth,
birth_datetime,
race_concept_id,
ethnicity_concept_id,
location_id,
provider_id,
care_site_id,
person_source_value,
gender_source_value,
gender_source_concept_id,
race_source_value,
race_source_concept_id,
ethnicity_source_value,
ethnicity_source_concept_id
)
SELECT
DISTINCT ON (idno)
NEXTVAL('public.person_id_seq') AS person_id,
CASE WHEN public.residencies.sex::int = 1 THEN 8507
 WHEN public.residencies.sex::int = 2 THEN 8532
 WHEN public.residencies.sex::int = 9 THEN 0
 ELSE 0
END AS gender_concept_id,
EXTRACT (YEAR FROM public.residencies.dob:: DATE) AS year_of_birth,
EXTRACT(MONTH FROM public.residencies.dob:: DATE) AS month_of_birth,
EXTRACT(DAY FROM public.residencies.dob:: DATE) AS day_of_birth,
public.residencies.dob:: DATE AS birth_datetime,
38003600 AS race_concept_id,
38003564 AS ethnicity_concept_id,
CAST(public.residencies.hhold_id AS BIGINT) AS location_id,
NULL AS provider_id,
NULL AS care_site_id,
CAST(public.residencies.idno AS BIGINT) AS person_source_value,
public.residencies.sex::int AS gender_source_value,
0 AS gender_source_concept_id,
NULL AS race_source_value,
0 AS race_source_concept_id,
NULL AS ethnicity_source_value,
0 AS ethnicity_source_concept_id
FROM public.residencies
ORDER BY idno, entry_date DESC;
