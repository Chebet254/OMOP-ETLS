#install.packages('RStata')
#install.packages("gmodels")
library(easypackages)
libraries("tidyverse", "readxl", "readr", "writexl", "openxlsx", "haven",
          "dplyr", "RStata", "lubridate", "gmodels")
library(epiR)
library(ggplot2)
library(scales)
library(haven)

#set wd
#alpha_metadata <- read_dta("alpha_metadata.dta")%>% as_factor()
hiv_tests <-  read_dta("D:/APHRC/LHS project/Kisesa datasets/hiv_tests_Kisesa.dta")#%>% as_factor()
#write.csv(hiv_tests, file = "D:/APHRC/LHS project/Kisesa datasets/HIV_test_csv.csv")
residency_kisesa <- read_dta("D:/APHRC/LHS project/Kisesa datasets/residency_Kisesa.dta")#%>% as_factor()

#calculating dob- days since 1960 
#add start date column
residency_kisesa <- residency_kisesa %>% add_column(start_date = as.Date('1960/01/01', format=("%Y/%m/%d")))
colnames(residency_kisesa)[3] = "bdate"
residency_kisesa <- within(residency_kisesa, {
  #start_date <- start_date
dob <- start_date + residency_kisesa$bdate })
#write.csv(residency_kisesa, file = "D:/APHRC/LHS project/Kisesa datasets/Residency_csv.csv")

#calculate age
end.date <- as.Date("2016/12/12")
residency_kisesa <- residency_kisesa %>% mutate(age = round((dob %--% end.date) / years(1)))

unique(residency_kisesa$sex)
sum(is.na(residency_kisesa$sex))
#DEMOGRAPHICS 
#sex

SEX <- residencies %>% distinct(idno, sex, .keep_all = TRUE)

table1 <-residencies %>% group_by(sex) %>% summarise(value = n()) 
str(sex)

gender <- table(residency_kisesa$sex)
gender
prop.table(gender)

#OMOP DATA FILES
#gender
table(person$gender_concept_id)

#STATUS
#tested positive & negative
status <- table(hiv_test$hiv_test_result)
status
prop.table(status)
hiv_tests %>% group_by(hiv_test_result) %>% summarise(value = n() ,Percentage=n()/nrow(.)*100)
#OMOP status
condition_occurrence %>% group_by(condition_concept_id) %>% summarise(value = n() ,Percentage=n()/nrow(.)*100)

#ENTRY & TYPE
residency_kisesa %>% group_by(entry_type) %>% summarise(value = n() ,Percentage=n()/nrow(.)*100)
residency_kisesa %>% group_by(exit_type) %>% summarise(value = n() ,Percentage=n()/nrow(.)*100)
observation %>% group_by(value_as_concept_id) %>% summarise(value = n() ,Percentage=n()/nrow(.)*100)

#exit type by sex
ftable(residency_kisesa$sex,residency_kisesa$exit_type, residency_kisesa$entry_type)

#requires merged person and observation
idno <- person %>% select(person_id, gender_concept_id)
idno$gender_concept_id <- as.integer(idno$gender_concept_id)
obs <- observation %>% select(value_as_concept_id, person_id)

obs_idno <- left_join(obs, idno, by = "person_id")
str(idno)
ftable(obs_idno$gender_concept_id, obs_idno$value_as_concept_id)
#mortality by status
ftable(hiv_test_result, exit_type)


#HIV VS SEX
table(hiv_test_result, sex)
#create age groups


#Create categories
merged <- merged %>% 
  mutate(
age_group <-  case_when(
  age <= 14            ~ "0-14",
  age > 14 & age <= 44 ~ "15-44",
  age > 44 & age <= 64 ~ "45-64",
  age > 64             ~ "> 64"
),
# Convert to factor
age_group = factor(
  age_group,
  level = c("0-14", "15-44","45-64", "> 64")
))

#mortality by age groups
 merged%>% group_by( age_group)%>% summarise(value = n() ,Percentage=n()/nrow(.)*100)

#residence 
residency_kisesa%>% group_by(residence)%>% summarise(value = n() ,Percentage=n()/nrow(.)*100)
# Draw barchart with ggplot2 package
ggplot(merged, aes(age_group)) +                             
  geom_bar(aes(y = (..count..)/sum(..count..))) + 
  scale_y_continuous(labels = percent)

