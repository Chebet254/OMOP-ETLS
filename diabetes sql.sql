DROP TABLE public.Diabetes
CREATE TABLE IF NOT EXISTS public.Diabetes
(
		created_respondent_id INT NOT NULL, 
		interview_date DATE , 
		dss_id VARCHAR, 
		dob VARCHAR, 
		sex VARCHAR, 
		village VARCHAR, 
		data_collection_round VARCHAR, 
		diabetic VARCHAR, 
		diabetis_diagnosis_date VARCHAR,
		diabetes_diagnosis_place VARCHAR, 
		hypertensive VARCHAR, 
		hypertensive_diagnosis_date VARCHAR, 
		hbp_diagnosis_place VARCHAR, 
		hbptablets_by_health_worker VARCHAR, 
        hbptabs_drug_store_no_prescription VARCHAR,
		hbptabs_relatives VARCHAR, 
		hbp_herbal_medicine_and_tabs VARCHAR, 
		hbp_herbal_medicine_only VARCHAR, 
		hbptabs_other VARCHAR, 
        hbp_diagnosis VARCHAR, 
		year_hbp_diagnosed VARCHAR, 
		heart_attack VARCHAR, 
		year_heart_attack VARCHAR, 
		angina VARCHAR, 
		year_angina VARCHAR,
		other_heart_disease VARCHAR, 
		year_other_heart_disease VARCHAR,
        kidney_disease VARCHAR, 
		year_kidney_disease VARCHAR,
		stroke VARCHAR, 
		year_stroke VARCHAR, 
		liver_disease VARCHAR, 
		year_liver_disease VARCHAR, 
		any_cancer VARCHAR, 
		year_any_cancer VARCHAR, 
		TB VARCHAR, 
		year_TB VARCHAR, 
        peripheral_neuropathy VARCHAR,
		poor_vision VARCHAR, 
		amputation VARCHAR, 
		kidney_complications VARCHAR, 
		chest_pain VARCHAR, 
		body_swelling VARCHAR, 
		smoked_tobacco VARCHAR, 
		smokeless_tobacco VARCHAR, 
        alcohol VARCHAR, 
		anaemic VARCHAR, 
		dehydrated VARCHAR, 
		pedal_oedema VARCHAR, 
		level_of_oedema VARCHAR,
		blood_pressure_systolic VARCHAR, 
		blood_pressure_diastolic VARCHAR, 
		respondent_stand_up VARCHAR,
        height_reading1 VARCHAR,
		height_reading2 VARCHAR, 
		weight_kg VARCHAR, 
		waist_circumference VARCHAR, 
		hip_circumference VARCHAR, 
		blood_glucose VARCHAR, 
		hba1c_measurement VARCHAR)
		
-- INSERT DATA FROM DM 
DELETE FROM public.Diabetes
COPY public.Diabetes
FROM 'D:\APHRC\LHS\OMOP ETL\DM DATASET\clean_diabetes.csv'
DELIMITER ','
CSV HEADER
NULL 'NA';

--INSERT DATA TO PERSON TABLE
--alter not null constraint
ALTER TABLE public.person ALTER COLUMN year_of_birth DROP NOT NULL;
--REMOVE CONSTRAINTS
ALTER TABLE public.person DROP CONSTRAINT fpk_person_gender_concept_id;
ALTER TABLE public.person DROP CONSTRAINT fpk_person_race_concept_id;
ALTER TABLE public.person DROP CONSTRAINT fpk_person_ethnicity_concept_id;
ALTER TABLE public.person DROP CONSTRAINT fpk_person_gender_source_concept_id;
ALTER TABLE public.person DROP CONSTRAINT fpk_person_race_source_concept_id;
ALTER TABLE public.person DROP CONSTRAINT fpk_person_ethnicity_source_concept_id;

CREATE SEQUENCE public.PERSON_id_seq;

INSERT INTO public.PERSON
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
DISTINCT ON (created_respondent_id )
NEXTVAL('public.PERSON_id_seq') AS person_id,
CASE WHEN public.Diabetes.sex = 'Male' THEN 8507
 	 WHEN public.Diabetes.sex = 'Female' THEN 8532
 	ELSE 0
END AS gender_concept_id,
EXTRACT (YEAR FROM public.Diabetes.dob:: DATE) AS year_of_birth,
EXTRACT(MONTH FROM public.Diabetes.dob:: DATE) AS month_of_birth,
EXTRACT(DAY FROM public.Diabetes.dob:: DATE) AS day_of_birth,
public.Diabetes.dob:: DATE AS birth_datetime,
38003600 AS race_concept_id,
38003564 AS ethnicity_concept_id,
NULL AS location_id,
NULL AS provider_id,
NULL AS care_site_id,
public.Diabetes.created_respondent_id AS person_source_value,
public.Diabetes.sex AS gender_source_value,
0 AS gender_source_concept_id,
NULL AS race_source_value,
0 AS race_source_concept_id,
NULL AS ethnicity_source_value,
0 AS ethnicity_source_concept_id
FROM public.Diabetes
ORDER BY created_respondent_id DESC;


-- INSERT DATA INTO OBS PERIOD
CREATE SEQUENCE public.observation_period_seq
ALTER TABLE public.observation_period ALTER COLUMN observation_period_end_date DROP NOT NULL;
ALTER TABLE public.observation_period ALTER COLUMN observation_period_start_date DROP NOT NULL;
--REMOVE CONSTRAINTS
ALTER TABLE public.observation_period DROP CONSTRAINT fpk_observation_period_period_type_concept_id;

INSERT INTO public.observation_period
(
 observation_period_id,
person_id,
observation_period_start_date,
 observation_period_end_date,
 period_type_concept_id
)
SELECT
 NEXTVAL('PUBLIC.observation_period_seq') AS observation_period_id,
public.PERSON.person_id AS person_id,
CASE WHEN public.Diabetes.data_collection_round = 'Baseline' THEN public.Diabetes.interview_date
		END AS observation_period_start_date,
CASE WHEN public.Diabetes.data_collection_round = 'One year follow-up' THEN  public.Diabetes.interview_date
 	END AS observation_period_end_date,
 44814723 AS period_type_concept_id
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS BIGINT) = CAST(public.Diabetes.created_respondent_id AS BIGINT);

--CONDITION OCCURANCE TABLE 
CREATE SEQUENCE public.CONDITION_OCCURENCE_id_seq;
--insert data into cond occ table
ALTER TABLE public.condition_occurrence ALTER COLUMN condition_start_date DROP NOT NULL;
--REMOVE CONSTRAINTS
ALTER TABLE public.condition_occurrence DROP CONSTRAINT fpk_condition_occurrence_condition_concept_id;
ALTER TABLE public.condition_occurrence DROP CONSTRAINT fpk_condition_occurrence_condition_type_concept_id;
ALTER TABLE public.condition_occurrence DROP CONSTRAINT fpk_condition_occurrence_condition_status_concept_id;
ALTER TABLE public.condition_occurrence DROP CONSTRAINT fpk_condition_occurrence_condition_source_concept_id;

--DIABETIC--
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
NEXTVAL('PUBLIC.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.diabetes.diabetic = 'Yes' THEN 21498446
 ELSE 0
 END AS condition_concept_id,
 diabetis_diagnosis_date::DATE AS condition_start_date,
 diabetis_diagnosis_date::DATE AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.diabetes.diabetic = 'Yes' THEN 'DIABETIC' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--HYPERTENSIVE 
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
NEXTVAL('PUBLIC.CONDITION_OCCURENCE_id_seq')  AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.hypertensive  = 'Yes' THEN 40398391
 ELSE 0
 END AS condition_concept_id,
 hypertensive_diagnosis_date::DATE AS condition_start_date,
 hypertensive_diagnosis_date::DATE AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.hypertensive = 'Yes' THEN 'HYPERTENSIVE' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--HIGH BP
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
NEXTVAL('PUBLIC.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 PUBLIC.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.hbp_diagnosis = 'Yes' THEN 45883495
 ELSE 0
 END AS condition_concept_id,
 to_date(year_hbp_diagnosed::varchar, 'YYYY') AS  condition_start_date,  --CHANGE YEAR FORMAT TO DATE FORMAT 
 to_date(year_hbp_diagnosed::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.hbp_diagnosis = 'Yes' THEN 'HIGH BP' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--HEART ATTACK
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.heart_attack  = 'Yes' THEN 36210764
 ELSE 0
 END AS condition_concept_id,
 to_date(year_heart_attack::varchar, 'YYYY') AS condition_start_date,
 to_date(year_heart_attack::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.heart_attack = 'Yes' THEN 'HEART ATTACK' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

-- ANGINA 
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.angina  = 'Yes' THEN 40323866
 ELSE 0
 END AS condition_concept_id,
 to_date(year_angina::varchar, 'YYYY') AS condition_start_date,
 to_date(year_angina::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.angina = 'Yes' THEN 'ANGINA' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--OTHER HEART DISEASE
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.other_heart_disease  = 'Yes' THEN 45879053
 ELSE 0
 END AS condition_concept_id,
 to_date(year_other_heart_disease::varchar, 'YYYY') AS condition_start_date,
 to_date(year_other_heart_disease::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.other_heart_disease = 'Yes' THEN 'Other heart disease' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--KIDNEY DISEASE
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.kidney_disease = 'Yes' THEN 198124
 ELSE 0
 END AS condition_concept_id,
 to_date(year_kidney_disease::varchar, 'YYYY') AS condition_start_date,
 to_date(year_kidney_disease::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.kidney_disease = 'Yes' THEN 'Kidney disease' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

-- STROKE
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.stroke = 'Yes' THEN 36210384
 ELSE 0
 END AS condition_concept_id,
 to_date(year_stroke::varchar, 'YYYY') AS condition_start_date,
 to_date(year_stroke::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.stroke = 'Yes' THEN 'STROKE' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--LIVER DISEASE
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.liver_disease = 'Yes' THEN 21499348
	ELSE 0
 END AS condition_concept_id,
 to_date(year_liver_disease::varchar, 'YYYY') AS condition_start_date,
 to_date(year_liver_disease::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.liver_disease = 'Yes' THEN 'LIVER DISEASE' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--CANCER
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
NEXTVAL('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.any_cancer  = 'Yes' THEN 45877275
 ELSE 0
 END AS condition_concept_id,
 to_date(year_any_cancer::varchar, 'YYYY') AS condition_start_date,
 to_date(year_any_cancer::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.any_cancer = 'Yes' THEN 'ANY CANCER' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--TB
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
NEXTVAL ('public.CONDITION_OCCURENCE_id_seq') AS condition_occurrence_id,
 public.PERSON.person_id AS person_id,
 CASE WHEN public.Diabetes.TB  = 'Yes' THEN 434557
 ELSE 0
 END AS condition_concept_id,
 to_date(year_TB::varchar, 'YYYY') AS condition_start_date,
 to_date(year_TB::varchar, 'YYYY') AS condition_start_datetime,
 NULL AS condition_end_date,
 NULL AS condition_end_datetime,
 44804112 AS condition_type_concept_id,
 0 AS condition_status_concept_id,
 NULL AS stop_reason,
 NULL AS provider_id,
 NULL AS visit_occurrence_id,
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.TB = 'Yes' THEN 'TB' 
 --ELSE 'NO'
 END AS condition_source_value,
 0 AS condition_source_concept_id,
 NULL AS condition_status_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--MEASUREMENT TABLE 
--create sequence 
CREATE SEQUENCE public.measurement_id_seq;
ALTER TABLE public.measurement DROP CONSTRAINT fpk_measurement_measurement_concept_id;
ALTER TABLE public.measurement DROP CONSTRAINT fpk_measurement_measurement_type_concept_id;
ALTER TABLE public.measurement DROP CONSTRAINT fpk_measurement_operator_concept_id;
ALTER TABLE public.measurement DROP CONSTRAINT fpk_measurement_value_as_concept_id;

--level of oedema
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
CASE WHEN public.Diabetes.pedal_oedema = 'Yes' THEN 4010362
	ELSE 0
END AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
NULL AS value_as_number,
CASE WHEN public.Diabetes.level_of_oedema = 'Moderate' THEN 45877983
	WHEN public.Diabetes.level_of_oedema = 'Mild' THEN 45883535
	ELSE 0
	END AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
NULL AS unit_source_value,
public.Diabetes.level_of_oedema AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--Blodd pressure sys 
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4152194 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.blood_pressure_systolic::INT AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Blood Pressure Systolic'  AS unit_source_value,
public.Diabetes.blood_pressure_systolic AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--BLOOD PRESSURE DIAS
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4154790 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.blood_pressure_diastolic::INT AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Blood Pressure Diastolic'  AS unit_source_value,
public.Diabetes.blood_pressure_diastolic AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--height 
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
607590 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.height_reading1::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Height'  AS unit_source_value,
public.Diabetes.height_reading1::float AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--weight
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4099154 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.weight_kg::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Weight kg'  AS unit_source_value,
public.Diabetes.weight_kg AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--waist circumference
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4172830 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.waist_circumference::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Waist circumference'  AS unit_source_value,
public.Diabetes.waist_circumference AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);


--hip circumference 
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4111665 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.hip_circumference::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Hip circumference'  AS unit_source_value,
public.Diabetes.hip_circumference AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--blood glucose
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
37399654 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.blood_glucose::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Blood Glucose'  AS unit_source_value,
public.Diabetes.blood_glucose AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

--hba1c measurement
INSERT INTO public.measurement (
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number, 
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT 
NEXTVAL ('public.measurement_id_seq') as measurement_id,
public.PERSON.person_id AS person_id,
4184637 AS measurement_concept_id,
public.Diabetes.interview_date AS measurement_date,
NULL as measurement_datetime,
NULL AS measurement_time, 
32809 AS measurement_type_concept_id,
4172703 AS operator_concept_id, 
public.Diabetes.hba1c_measurement::float AS value_as_number,
NULL AS value_as_concept_id,
NULL AS unit_concept_id,
NULL AS range_low,
NULL AS range_high,
NULL AS provider_id,
NULL AS visit_occurrence_id,
NULL AS visit_detail_id, 
NULL AS measurement_source_value,
NULL AS measurement_source_concept_id,
'Haemoglobin measurement'  AS unit_source_value,
public.Diabetes.hba1c_measurement AS value_source_value
FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);


--observation TABLE 
--CREATE SEQ
CREATE SEQUENCE public.observation_id_seq;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_observation_concept_id;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_observation_type_concept_id;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_value_as_concept_id;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_unit_concept_id;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_observation_source_concept_id;
ALTER TABLE public.observation DROP CONSTRAINT fpk_observation_obs_event_field_concept_id;

---INSERT INTO DM OBS
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
 public.PERSON.person_id AS person_id,
 4295659 AS observation_concept_id,
 public.Diabetes.interview_date  AS observation_date,
 CASE WHEN public.Diabetes.interview_date is NULL THEN '9999-12-31 00:00:00'
 ELSE public.Diabetes.interview_date 
 END AS observation_datetime,
 45905771 AS observation_type_concept_id,
 NULL AS value_as_number,
 NULL AS value_as_string,
 CASE WHEN public.Diabetes.peripheral_neuropathy = 'Yes' THEN 35826737
 	WHEN public.Diabetes.poor_vision = 'Yes' THEN 45529282
	WHEN public.Diabetes.amputation = 'Yes' THEN 40768787
	WHEN public.Diabetes.kidney_complications = 'Yes' THEN 43529062
	WHEN public.Diabetes.chest_pain = 'Yes' THEN 36310512
	WHEN public.Diabetes.body_swelling = 'Yes' THEN 905026
	WHEN public.Diabetes.smoked_tobacco = 'Yes' THEN 42530793
	WHEN public.Diabetes.smokeless_tobacco = 'Yes' THEN 36308511
	WHEN public.Diabetes.alcohol = 'Yes' THEN 45883296
	WHEN public.Diabetes.anaemic = 'Yes' THEN 439777
	WHEN public.Diabetes.dehydrated = 'Yes' THEN 36308918
	WHEN public.Diabetes.respondent_stand_up = 'Yes' THEN 4105536
	ELSE 0
	END AS value_as_concept_id,
 NULL AS qualifier_concept_id,
 4299438 AS unit_concept_id,
 NULL AS provider_id,
 NULL AS visit_occurrence_id, 
 NULL AS visit_detail_id,
 CASE WHEN public.Diabetes.peripheral_neuropathy = 'Yes' THEN 'PERIPHERAL NEUROPATHY'
   	WHEN public.Diabetes.poor_vision = 'Yes' THEN 'POOR VISION'
	WHEN public.Diabetes.amputation = 'Yes' THEN 'HAD AMPUTATION'
	WHEN public.Diabetes.kidney_complications = 'Yes' THEN 'KIDNEY COMPLICATIONS'
	WHEN public.Diabetes.chest_pain = 'Yes' THEN 'CHEST PAIN'
	WHEN public.Diabetes.body_swelling = 'Yes' THEN 'HAVE BODY SWELLING'
	WHEN public.Diabetes.smoked_tobacco = 'Yes' THEN 'SMOKED TOBACCO'
	WHEN public.Diabetes.smokeless_tobacco = 'Yes' THEN 'SMOKELESS TOBACCO'
	WHEN public.Diabetes.alcohol = 'Yes' THEN 'DRINK ALCOHOL'
	WHEN public.Diabetes.anaemic = 'Yes' THEN 'IS ANAEMIC'
	WHEN public.Diabetes.dehydrated = 'Yes' THEN 'IS DEHYDRATED'
	WHEN public.Diabetes.respondent_stand_up = 'Yes' THEN 'CAN STAND UP'
	ELSE 'NO , DO NOT KNOW'
	END AS observation_source_value, 
0 AS observation_source_concept_id,
'Individual' AS unit_source_value,
 NULL AS qualifier_source_value,
 NULL AS observation_event_id,
 0 AS obs_event_field_concept_id
 FROM public.Diabetes
INNER JOIN public.PERSON
ON CAST(public.PERSON.person_source_value AS INT) = CAST(public.Diabetes.created_respondent_id AS INT);

 
 
 
 
 


