#First, install the package from GitHub
install.packages("devtools")
devtools::install_github("OHDSI/CommonDataModel")
#List the currently supported SQL dialects
CommonDataModel::listSupportedDialects()
#List the currently supported CDM versions
CommonDataModel::listSupportedVersions()
#This function will generate the text files in the dialect you choose, putting the output files in the folder you specify.
CommonDataModel::buildRelease(cdmVersions = "5.4",
                              targetDialects = "postgresql",
                              outputfolder = "/Output_DM")  #setwd
#If you have an empty schema ready to go, the package will connect and instantiate the tables for you. 
#To start, you need to download DatabaseConnector in order to connect to your database.

install.packages("DatabaseConnector")
library(DatabaseConnector)

#set JDBC drivers
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "c:/temp/jdbcDrivers")
downloadJdbcDrivers("postgresql")

cd <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                 server = "localhost/DM",   #DATABASE NAME 
                                                 user = "postgres",
                                                 password = "",
                                                 pathToDriver = "c:/temp/jdbcDrivers"
)

CommonDataModel::executeDdl(connectionDetails = cd,
                            cdmVersion = "5.4",
                            cdmDatabaseSchema = "public"
)
