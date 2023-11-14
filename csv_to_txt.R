# Install and load the required packages
install.packages("DBI")
install.packages("dplyr")

library(DBI)
library(dplyr)

# Define your database connection details
db_connection <- dbConnect(RSQLite::SQLite(), dbname = "your_database.sqlite")

# List of tables you want to extract
tables_to_extract <- c("table1", "table2", "table3")

# Loop through each table and extract data
for (table_name in tables_to_extract) {
  query <- paste("SELECT * FROM", table_name)
  table_data <- dbGetQuery(db_connection, query)
  
  # Write data to a CSV file (adjust file name as needed)
  write.csv(table_data, paste0(table_name, ".csv"), row.names = FALSE)
}

# Close the database connection
dbDisconnect(db_connection)
