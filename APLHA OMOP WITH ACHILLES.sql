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

--MAPPING RESIDENCIES DATA 
--CREATE RESIDENCIES TABLE 
CREATE TABLE public.Residencies (
  	idno BIGINT not NULL,
	sex VARCHAR,
	bdate VARCHAR,
	residence VARCHAR, entry_date VARCHAR,
	exit_date VARCHAR,
	entry_type VARCHAR,
	exit_type VARCHAR,
  	hhold_id BIGINT,
  	hhold_id_extra VARCHAR,
	study_name VARCHAR,
	start_date VARCHAR,
	dob VARCHAR)

--INSERT DATA TO RESIDENCIES TABLE 
COPY public.Residencies
FROM 'D:\APHRC\LHS\OMOP ETL\KISESA DATASETS-20230324T073138Z-001\KISESA DATASETS\Residency.csv'
DELIMITER ','
CSV HEADER
NULL 'NA';
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

--OBSERVATION PERIOD TABLE 
CREATE SEQUENCE public.observation_period_id_seq;
--alter not null constraint
ALTER TABLE public.observation_period ALTER COLUMN observation_period_end_date DROP NOT NULL;

INSERT INTO PUBLIC.observation_period
(
 observation_period_id,
person_id,
observation_period_start_date,
 observation_period_end_date,
 period_type_concept_id
)
SELECT
 NEXTVAL('public.observation_period_id_seq') AS observation_period_id,
 public.person.person_id AS person_id,
 public.residencies.entry_date::DATE AS observation_period_start_date,
 public.residencies.exit_date::DATE AS observation_period_end_date,
 44814723 AS period_type_concept_id
FROM public.residencies
INNER JOIN public.person
ON CAST(public.person.person_source_value AS BIGINT) = CAST(public.residencies.idno AS BIGINT);

--OBSERVATION TABLE 
CREATE SEQUENCE public.observation_id_seq;
--i)	Insert episode end/exit to convert to long format.
INSERT INTO public.observation
(
 observation_id,
 person_id,
 observation_concept_id,
 observation_date,
 observation_datetime,
 observation_type_concept_id,
 value_as_number,
 value_as_string,
 value_as_concept_id,
 qualifier_concept_id,
 unit_concept_id,
 provider_id,
 visit_occurrence_id,
 visit_detail_id,
 observation_source_value,
 observation_source_concept_id,
 unit_source_value,
 qualifier_source_value,
 observation_event_id,
 obs_event_field_concept_id
)
SELECT
 NEXTVAL('public.observation_id_seq') AS observation_id,
 public.person.person_id AS person_id,
4295659 AS observation_concept_id,
 public.residencies.entry_date::DATE AS observation_date,
 CASE WHEN public.residencies.entry_date::TIMESTAMP IS NULL THEN '9999-12-31 00:00:00'
 ELSE public.residencies.entry_date::TIMESTAMP
END AS observation_datetime,
 45905771 AS observation_type_concept_id,
 NULL AS value_as_number,
 NULL AS value_as_string,
 CASE WHEN public.residencies.entry_type = 'Baseline' THEN 8568
	WHEN public.residencies.entry_type = 'Birth' THEN 4216316
	WHEN public.residencies.entry_type = 'in-migration' THEN 45878942
ELSE 0
END AS value_as_concept_id,
 CASE WHEN public.residencies.entry_type = 'in-migraration' THEN 44804024
 ELSE NULL
END AS qualifier_concept_id,
 4299438 AS unit_concept_id,
 NULL AS provider_id,
 NULL AS visit_occurrence_id, -- Link it as foreign key to visit_occurence_id form the visit_occurence OMOP table
 NULL AS visit_detail_id,
public.residencies.entry_type AS observation_source_value,
 0 AS observation_source_concept_id,
 'Individual' AS unit_source_value,
 NULL AS qualifier_source_value,
 NULL AS observation_event_id,
 0 AS obs_event_field_concept_id
FROM public.residencies
INNER JOIN public.person
ON CAST(public.person.person_source_value AS BIGINT) = CAST(public.residencies.idno AS BIGINT);

--ii)	Insert episode end/exit to convert to long format.
ALTER TABLE public.observation ALTER COLUMN observation_date DROP NOT NULL;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_value_as_concept_id;

INSERT INTO public.observation
(
 observation_id,
 person_id,
 observation_concept_id,
 observation_date,
 observation_datetime,
 observation_type_concept_id,
 value_as_number,
 value_as_string,
 value_as_concept_id,
 qualifier_concept_id,
 unit_concept_id,
 provider_id,
 visit_occurrence_id,
 visit_detail_id,
 observation_source_value,
 observation_source_concept_id,
 unit_source_value,
 qualifier_source_value,
 observation_event_id,
 obs_event_field_concept_id
)
SELECT
 NEXTVAL('public.observation_id_seq') AS observation_id,
 public.person.person_id AS person_id,
4295659 AS observation_concept_id,
 residencies.exit_date::DATE AS observation_date,
 CASE WHEN public.residencies.exit_date::DATE IS NULL THEN '9999-12-31 00:00:00'
 ELSE public.residencies.exit_date::DATE
END AS observation_datetime,
 45905771 AS observation_type_concept_id,
 NULL AS value_as_number,
 NULL AS value_as_string,
 CASE WHEN public.residencies.exit_type = 'Present in study' THEN  4181412
 	WHEN public.residencies.exit_type = 'Death' THEN 4306655
	WHEN public.residencies.exit_type = 'out-migration' THEN 40377961
	WHEN public.residencies.exit_type = 'Lost to follow up' THEN 45618458
ELSE 0
END AS value_as_concept_id,
CASE WHEN public.residencies.exit_type = 'out-migration' THEN 44804024
 ELSE NULL
END AS qualifier_concept_id,
 4299438 AS unit_concept_id,
 NULL AS provider_id,
 NULL AS visit_occurrence_id, -- Link it as foreign key to visit_occurence_id form the visit_occurence OMOP table
 NULL AS visit_detail_id,
 public.residencies.exit_type AS observation_source_value,
 0 AS observation_source_concept_id,
 'Individual' AS unit_source_value,
 NULL AS qualifier_source_value,
 NULL AS observation_event_id,
 0 AS obs_event_field_concept_id
FROM public.residencies
INNER JOIN public.person
ON CAST(public.person.person_source_value AS BIGINT) = CAST(public.residencies.idno AS BIGINT);

--MAPPING HIV DATA 
--CREATE HIV TEST TABLE 
CREATE TABLE public.HIV_TEST(
	id_no BIGINT, 
	hiv_test_date VARCHAR,
	hiv_test_result VARCHAR,
	informed_of_result INT,
	source_of_test_information INT,
	test_report_date VARCHAR,
	test_assumption INT,
	survey_round_name VARCHAR,
	study_name VARCHAR);
	
--INSERT HIV DATA 
COPY public.HIV_TEST
FROM 'D:\APHRC\LHS\OMOP ETL\KISESA DATASETS-20230324T073138Z-001\KISESA DATASETS\HIV_test_csv.csv'
DELIMITER ','
CSV HEADER
NULL 'NA';

--VISIT OCCRRENCE TABLE
CREATE SEQUENCE public.visit_occurrence_id_seq;

INSERT INTO public.visit_occurrence
(
 visit_occurrence_id,
 person_id,
 visit_concept_id,
 visit_start_date,
 visit_start_datetime,
 visit_end_date,
 visit_end_datetime,
 visit_type_concept_id,
 provider_id,
 care_site_id,
 visit_source_value,
 visit_source_concept_id,
 admitted_from_concept_id,
 admitted_from_source_value,
 discharged_to_concept_id,
 discharged_to_source_value,
 preceding_visit_occurrence_id)
SELECT
NEXTVAL('public.visit_occurrence_id_seq') AS visit_occurrence_id,
 public.person.person_id AS person_id,
 4119839 AS visit_concept_id,
 public.hiv_test.hiv_test_date::DATE AS visit_start_date,
CASE WHEN public.hiv_test.hiv_test_date::DATE IS NULL THEN '9999-12-31'
ELSE public.hiv_test.hiv_test_date::DATE
END AS visit_start_datetime,
 public.hiv_test.hiv_test_date::DATE AS visit_end_date,
CASE WHEN public.hiv_test.hiv_test_date::DATE IS NULL THEN '9999-12-31'
ELSE public.hiv_test.hiv_test_date::DATE
END AS visit_end_datetime,
 44818519 AS visit_type_concept_id,
 NULL AS provider_id,
 NULL AS care_site_id,
 NULL AS visit_source_value,
 0 AS visit_source_concept_id,
 0 AS admitting_source_concept_id,
 NULL AS admitting_source_value,
 0 AS discharge_to_concept_id,
 NULL AS discharge_to_source_value,
 NULL AS preceding_visit_occurrence_id
FROM public.HIV_TEST
INNER JOIN public.person
ON CAST(public.person.person_source_value AS BIGINT) = CAST(public.hiv_test.id_no AS BIGINT)

--CONDITION OCC
CREATE SEQUENCE public.condition_occurrence_id_seq;

INSERT INTO public.condition_occurrence
(
 condition_occurrence_id,
 person_id,
 condition_concept_id,
 condition_start_date,
 condition_start_datetime,
 condition_end_date,
 condition_end_datetime,
 condition_type_concept_id,
 condition_status_concept_id,
 stop_reason,
 provider_id,
 visit_occurrence_id,
 visit_detail_id,
 condition_source_value,
 condition_source_concept_id,
 condition_status_source_value
)
SELECT
NEXTVAL('public.condition_occurrence_id_seq') AS condition_occurrence_id,
 public.person.person_id AS person_id,
 CASE WHEN public.hiv_test.hiv_test_result = 'Negative' THEN 4013105
 WHEN public.hiv_test.hiv_test_result = 'Positive' THEN 4013106
 WHEN public.hiv_test.hiv_test_result::INT = 9 THEN 4088484
 ELSE 0
 END AS condition_concept_id,
 public.hiv_test.hiv_test_date::DATE AS condition_start_date,
 CASE WHEN public.hiv_test.hiv_test_date::DATE IS NULL THEN '9999-12-31'
 ELSE public.hiv_test.hiv_test_date::DATE
 END AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 32809 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 public.hiv_test.hiv_test_result AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.hiv_test
INNER JOIN public.person
ON CAST(public.person.person_source_value AS BIGINT) = CAST(public.hiv_test.id_no AS BIGINT);

