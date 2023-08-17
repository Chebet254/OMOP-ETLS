library(remotes)  #achilles
library(DatabaseConnector)
library(ETLSyntheaBuilder) #for vocabulary loading 
#set JDBC drivers
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "c:/temp/jdbcDrivers")
downloadJdbcDrivers("postgresql")

#create connection
connectiondetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/dm",
  user = "postgres",
  password = "",
  port = 5432,
  pathToDriver = "c:/temp/jdbcDrivers"
)

outputFolder <- "D:/APHRC/LHS project/OMOP ETL/dm_output"


Achilles::achilles(connectionDetails = connectiondetails,
                   cdmDatabaseSchema = "public",
                   resultsDatabaseSchema = "achillesresults",  #no capital letters- brings issues with postgres
                   sourceName = "cdm",
                   createTable = TRUE,
                   smallCellCount = 5,
                   cdmVersion = 5.4,
                   numThreads = 1,
                   outputFolder = outputFolder)

#load vocabularies 
cdmSchema      <- "vocabulary"
cdmVersion     <- "5.4"
vocabFileLoc   <- "/Vocabulary_download_v5"

CreateCDMTables(connectionDetails = connectiondetails, cdmSchema = cdmSchema, cdmVersion = cdmVersion)
LoadVocabFromCsv(connectionDetails = connectiondetails, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc)

#export to json
Achilles::exportToJson(connectionDetails = connectiondetails,
                       cdmDatabaseSchema = "public",
                       resultsDatabaseSchema = "achillesresults",
                       outputPath = outputFolder,
                       vocabDatabaseSchema = "vocabulary",
                       compressIntoOneFile = FALSE)

Achilles::getAnalysisDetails()
Achilles::exportResultsToCSV(
           connectionDetails = connectiondetails,
          resultsDatabaseSchema = 'achillesresults',
          analysisIds = c(),
          minCellCount = 5,
          exportFolder= outputFolder
)
