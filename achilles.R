install.packages("remotes")
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
  server = "localhost/POSTGRESQL 14",
  user = "postgres",
  password = "",
  port = 5432
)

outputFolder <- "D:/APHRC/LHS project/OMOP ETL/output"

Achilles::validateSchema(connectionDetails = connectiondetails,
                         cdmDatabaseSchema = "cdm",
                         resultsDatabaseSchema = "cdm",
                         cdmVersion = 5.4,
                         runCostAnalysis = FALSE,
                         outputFolder = outputFolder,
                         sqlOnly = FALSE)

Achilles::achilles(connectionDetails = connectiondetails,
                   cdmDatabaseSchema = "cdm",
                   resultsDatabaseSchema = "achilles_results",
                   sourceName = "cdm",
                   createTable = TRUE,
                   smallCellCount = 5,
                   cdmVersion = 5.4,
                   numThreads = 1,
                   outputFolder = outputFolder
)


Achilles::exportToJson(connectionDetails = connectiondetails,
                       cdmDatabaseSchema = "cdm",
                       resultsDatabaseSchema = "achilles_results",
                       outputPath = "D:/APHRC/LHS project/OMOP ETL/output/cdm",
                       vocabDatabaseSchema = "cdm",
                       compressIntoOneFile = FALSE)

