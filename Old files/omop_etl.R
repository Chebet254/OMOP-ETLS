#install.packages("RPostgres")
library('RPostgreSQL')
library(RPostgres)
library(lubridate)
library(dplyr)
library(tidyr) #nas
library(tidyverse)

Sys.timezone()
#create connection object
# con <- dbConnect(drv =PostgreSQL(), 
#                  user="postgres", 
#                  password="",
#                  host="localhost", 
#                  port=5432, 
#                  dbname="postgres")

con2 <- dbConnect(drv =Postgres(), 
                 user="postgres", 
                 password="",
                 host="localhost", 
                 port=5432, 
                 dbname="postgres",
                 timezone= "Africa/Nairobi")

dbGetQuery(con2, "SHOW TIMEZONE;")

dbListTables(con2)   #list all the tables 
#dbDisconnect(con)   #disconnect from database
#query the database and store the data in dataframe
observation<- dbGetQuery(con2, "SELECT * from cdmobservation")
person <- dbGetQuery(con2, "SELECT * from person")
condition_occurrence <- dbGetQuery(con2, "SELECT * from condition_occurrence")
fact_relationship <- dbGetQuery(con2, "SELECT * from fact_relationship")
hiv_test <- dbGetQuery(con2, "SELECT * from hiv_test")
location_history <- dbGetQuery(con2, "SELECT * from location_history")
observation_period <- dbGetQuery(con2, "SELECT * from observation_period")
survey_conduct <- dbGetQuery(con2, "SELECT * from survey_conduct")
visit_occurrence <- dbGetQuery(con2, "SELECT * from visit_occurrence")
residencies <- dbGetQuery(con2, "SELECT * from residencies")

# write.csv(observation, "D:/APHRC/LHS project/Kisesa datasets/observation.csv")
# write.csv(person, "D:/APHRC/LHS project/Kisesa datasets/person.csv")
# write.csv(condition_occurrence, "D:/APHRC/LHS project/Kisesa datasets/condition_occurrence.csv")
# write.csv(location_history, "D:/APHRC/LHS project/Kisesa datasets/location_history.csv")
# write.csv(observation_period, "D:/APHRC/LHS project/Kisesa datasets/observation_period.csv")
# write.csv(visit_occurrence, "D:/APHRC/LHS project/Kisesa datasets/visit_occurrence.csv")
#write.csv(observation_period, "D:/APHRC/LHS project/Kisesa datasets/observation_period.csv")

n <- unique(residencies$idno)
un <- residencies %>% distinct(idno, sex)
males <- un %>% filter(sex == 1)
females <- un %>% filter(sex == 2)





#CREATING MORTALITY SPEC FROM CONDITION OCCURRENCE AND PERSON TABLE
#calculate ages
person_details <- person %>% select(person_id,gender_concept_id, birth_datetime, death_datetime)  
end.date <- as.Date("2016/12/12")
person_details <- person_details %>% mutate(age = round((birth_datetime %--% end.date) / years(1)))
#select ages 15-49 and group them
person2 <- person_details %>% filter(age > 14 & age < 50)
person2 <- person2 %>% mutate(agegrp =
        case_when(age >= 15 & age <= 19 ~ '3',
                  age >= 20 & age <= 24 ~ '4',
                  age >= 25 & age <= 29 ~'5',                 
                  age >= 30 & age <= 34 ~ '6', 
                  age >= 35 & age <= 39 ~ '7', 
                  age >= 40 & age <= 44 ~ '8', 
                  age >= 45 & age <= 49 ~ '9')) 

#get hiv status from condition occ table and join
#get person data from person table- dat2
#get visit occ from visit table -dat3
#get hiv data from condition occ and join them- dat4
#visit <- visit_occurrence %>% select(person_id,visit_concept_id, visit_start_date, visit_end_date)
#filter clinic visits from home visits
#visit <- visit %>% filter(visit_concept_id == 4119839)
status <- condition_occurrence %>% select(person_id, condition_concept_id, condition_start_date)
# create a sequence variable and pick up maximum number of tests to use later on in
# looping through test results
dat <- status %>%   dplyr::group_by(person_id) %>%
  dplyr::arrange(condition_start_date) %>%
  dplyr::mutate(test_sequence=row_number())
maxtests <- max(dat$test_sequence)

# FIRST & LAST TESTS

## find first and last test dates (all tests)
# any test
dat <- dat %>% group_by(person_id) %>%
          mutate(firsttest= min(condition_start_date),
                lasttest = max(condition_start_date))


write.csv(dat, "D:/APHRC/LHS project/Kisesa datasets/dat.csv")
# negative
dat <- dat[1:40000, ]
dat <- dat %>% group_by(person_id) %>%
    mutate(first_negative = min(condition_start_date[condition_concept_id==4013105]),
           last_negative = max(condition_start_date[condition_concept_id==4013105 & !is.na(condition_start_date)]))
           

# install.packages("pryr")
# library(pryr)
# mem_used()
    # dat <- dat %>% group_by(person_id) %>%
#   mutate(first_negative = case_when(condition_concept_id==4013105 ~ min(condition_start_date))) %>% 
#   mutate(last_negative = case_when(condition_concept_id==4013105 ~ max(condition_start_date)))
#            

# positive
dat <- dat %>% group_by(person_id) %>%
    mutate(first_positive = min(condition_start_date[condition_concept_id==4013106]),
           last_positive = max(condition_start_date[condition_concept_id==4013106]))
           
              
dat$condition_start_date[condition_concept_id == 0]
  
#dat5 <- inner_join(visit, person2, by = "person_id") %>% unique()

#bring back former code?
#dat6 <- inner_join(dat5, status, by = c("person_id"))
person_status <- inner_join(person2, status, by = c("person_id"))

#dat6 %>% group_by(person_id) %>% mutate(time_in = min(age))

#join with obs period to find episodes(time in and time out)
episode_period <- observation_period %>% select(person_id, observation_period_start_date, observation_period_end_date)
person_status <- inner_join(person_status, episode_period, by = ("person_id")) #, "visit_start_date" = "observation_period_start_date"))

#filter dates between obs period start & end
person_status <- person_status %>% filter(condition_start_date >= observation_period_start_date & condition_start_date <= observation_period_end_date)

#identifying duplicates
# dups <- person_status %>%
#   dplyr::group_by(gender_concept_id, birth_datetime, death_datetime, age, agegrp, condition_concept_id, observation_period_start_date, observation_period_end_date, person_id) %>%
#   dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
#   dplyr::filter(n > 1L) 

mortality_spec <- person_status %>% mutate(timein = round(( birth_datetime %--% observation_period_start_date) / years(1))) %>% 
  mutate(timeout = round(( birth_datetime%--% observation_period_end_date) / years(1)))
  
#seroconversion
mortality_spec <- mortality_spec %>% mutate(fail =
                          case_when(condition_concept_id == 4013105 | condition_concept_id == 0  ~'0',
                                    condition_concept_id == 4013106 ~'1'))

mortality_spec1 <- mortality_spec %>% select(-c(birth_datetime, death_datetime, age, condition_start_date, 
                                               observation_period_start_date, observation_period_end_date))
  
  
#CREATING INCIDENCE SPEC
#pivot wider

#age at last neg
incidence <- mortality_spec %>% mutate(age_last_neg=
             ifelse(condition_concept_id == '4013105', (birth_datetime %--% condition_start_date)/ years(1), 'NA')) %>% 
              mutate(age_first_pos = ifelse(condition_concept_id == '4013106', (birth_datetime %--% condition_start_date)/ years(1), 'NA'))
incidence$age_first_pos <- as.character(as.numeric(incidence$age_first_pos))
incidence$age_last_neg <- as.character(as.numeric(incidence$age_last_neg)) 
  
