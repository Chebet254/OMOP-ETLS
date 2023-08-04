#install.packages("remotes")
if(!require(DatabaseConnector)){
  install.packages("DatabaseConnector")
  library(DatabaseConnector)
}
remotes::install_github("OHDSI/Achilles")
library(remotes)

#set JDBC drivers
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "c:/temp/jdbcDrivers")
downloadJdbcDrivers("postgresql")

#create connection
connectiondetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/postgres",
  user = "postgres",
  password = "",
  port = 5432,
  pathToDriver = "c:/temp/jdbcDrivers"
)

outputFolder <- "D:/APHRC/LHS project/OMOP ETL/output"


Achilles::achilles(connectionDetails = connectiondetails,
                   cdmDatabaseSchema = "public",
                   resultsDatabaseSchema = "achillesresults",  #no capital letters- brings issues with postgres
                   sourceName = "cdm",
                   createTable = TRUE,
                   smallCellCount = 5,
                   cdmVersion = 5.4,
                   numThreads = 1,
                   outputFolder = outputFolder)


Achilles::exportToJson(connectionDetails = connectiondetails,
                       cdmDatabaseSchema = "public",
                       resultsDatabaseSchema = "achillesresults",
                       outputPath = outputFolder,
                       vocabDatabaseSchema = "cdm",
                       compressIntoOneFile = FALSE)



cdmSchema      <- "public"
cdmVersion     <- "5.4"
vocabFileLoc   <- "/Vocabulary_download_v5"

CreateCDMTables(connectionDetails = connectiondetails, cdmSchema = cdmSchema, cdmVersion = cdmVersion)
LoadVocabFromCsv(connectionDetails = connectiondetails, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc)
