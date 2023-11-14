#DQD dashboard for ALPHA MAGU HIV DATA. 

#install.packages("remotes")
remotes::install_github("OHDSI/DataQualityDashboard")
library(DatabaseConnector)
library(DataQualityDashboard)
 #CONNECTION DETAILS TO POSTGRES DB
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = "postgres",
  password = "aphrc",
  server = "localhost/magu_cdm",
  port = "5432",
  pathToDriver = "c:/temp/jdbcDrivers"
)
#check if the connection is working
conn <- connect(connectionDetails)
conn
#set up variable params
cdmDatabaseSchema <- "public" # database schema name of the CDM
resultsDatabaseSchema <- "magu_results" # database schema name of the results 
cdmSourceName <- "CDM ALPHA MAGU 2023" # a human readable name for your CDM source
cdmVersion <- "5.4" # the CDM version you are targeting. Currently supports 5.2, 5.3, and 5.4

# determine how many threads (concurrent SQL sessions) to use 
numThreads <- 1 

# specify if you want to execute the queries or inspect them 
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries
sqlOnlyIncrementalInsert <- FALSE # set to TRUE if you want the generated SQL queries to calculate DQD 
sqlOnlyUnionCount <- 1 

# where should the results and logs go?
outputFolder <- "D:/APHRC/LHS/OMOP ETL/OMOP-ETLS github/output"
outputFile <- "results.json"

# logging type 
verboseMode <- TRUE # set to FALSE if you don't want the logs to be printed to the console

# write results to table? 
writeToTable <- TRUE # set to FALSE if you want to skip writing to a SQL table in the results schema

# specify the name of the results table (used when writeToTable = TRUE and when sqlOnlyIncrementalInser
writeTableName <- "dqdashboard_results"
writeToCsv <- FALSE # set to FALSE if you want to skip writing to csv file
csvFile <- "" # only needed if writeToCsv is set to TRUE

# which DQ check levels to run 
checkLevels <- c("TABLE", "FIELD", "CONCEPT")
# which DQ checks to run? 
checkNames <- c() # Names can be found in inst/csv/OMOP_CDM_v5.3_Check_Descriptions.csv
# which CDM tables to exclude? 
tablesToExclude <- c("CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", "CONCEPT_CLASS")
                     
#--------------------------------------------------------------------------------------
# run the job 
DataQualityDashboard::executeDqChecks(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      resultsDatabaseSchema = resultsDatabaseSchema,
                                      cdmSourceName = cdmSourceName,
                                      cdmVersion = cdmVersion,
                                      numThreads = numThreads,
                                      sqlOnly = sqlOnly,
                                      sqlOnlyUnionCount = sqlOnlyUnionCount,
                                      sqlOnlyIncrementalInsert = sqlOnlyIncrementalInsert,
                                      outputFolder = outputFolder,
                                      outputFile = outputFile,
                                      verboseMode = verboseMode,
                                      writeToTable = writeToTable,
                                      writeToCsv = writeToCsv,
                                      csvFile = csvFile,
                                      checkLevels = checkLevels,
                                      tablesToExclude = tablesToExclude,
                                      checkNames = checkNames)

#INSPECT LOGS 
ParallelLogger::launchLogViewer(
  logFileName = file.path(outputFolder, cdmSourceName, 
  sprintf('log_DqDashboard_Synthea synthetic health database.txt', cdmSourceName)))

#VIEW RESULTS
#Launching Dashboard as Shiny App
DataQualityDashboard::viewDqDashboard("D:/APHRC/LHS/OMOP ETL/OMOP-ETLS github/output/results.json")

#VIEWING CHECKS 
#To see description of checks using R, execute the command below:
checks <- DataQualityDashboard::listDqChecks(cdmVersion = "5.4") # Put the version of the CDM you are using
  