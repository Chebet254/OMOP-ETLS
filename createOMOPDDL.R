# Generating the DDLs
# This module will demonstrate two different ways the CDM R package can be used to create the CDM tables in your environment. 
# First, it uses the buildRelease function to create the DDL files on your machine, intended for end users that wish to 
# generate these scripts from R without the need to clone or download the source code from github. 
# The SQL scripts that are created through this process are available as zip files as part of the latest release. 
# They are also available on the master branch here.
# 
# Second, the script shows the executeDdl function that will connect up to your SQL client directly
#(assuming your dbms is one of the supported dialects) and instantiate the tables through R.

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
                              outputfolder = "/Output")  #setwd
#If you have an empty schema ready to go, the package will connect and instantiate the tables for you. 
#To start, you need to download DatabaseConnector in order to connect to your database.

install.packages("DatabaseConnector")
library(DatabaseConnector)
#set JDBC drivers
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "c:/temp/jdbcDrivers")
downloadJdbcDrivers("postgresql")


extraSettings <- ";databaseName=alpha;integratedSecurity=false;encrypt=false;trustServerCertificate=true;sslProtocol=TLSv1"

cd <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                 server = "localhost/alpha",   #DATABASE NAME / CREATE DB USING PSQL SHELL
                                                 user = "postgres",
                                                 password = "aphrc",
                                                 pathToDriver = "c:/temp/jdbcDrivers",
                                                 extraSettings = extraSettings
)
conn <- connect(cd)

CommonDataModel::executeDdl(connectionDetails = cd,
                            cdmVersion = "5.4",
                            cdmDatabaseSchema = "public"
)
 

 