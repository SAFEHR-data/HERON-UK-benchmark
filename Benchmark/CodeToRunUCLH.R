# to restore renv environment
renv::restore()

# omop_reservoir version
# beware dbName identifies outputs, dbname is UCLH db
dbName <- "UCLH-from-2019"
cdmSchema <- "data_catalogue_007" #from 2019
user <- Sys.getenv("user")
host <- Sys.getenv("host")
port <- Sys.getenv("port")
dbname <- Sys.getenv("dbname")
pwd <- Sys.getenv("pwd")
writeSchema <- "_other_andsouth"

if("" %in% c(user, host, port, dbname, pwd, writeSchema))
  stop("seems you don't have (all?) db credentials stored in your .Renviron file, use usethis::edit_r_environ() to create")

#pwd <- rstudioapi::askForPassword("Password for omop_db")

con <- DBI::dbConnect(RPostgres::Postgres(),user = user, host = host, port = port, dbname = dbname, password=pwd)

#you get this if not connected to VPN
#Error: could not translate host name ... to address: Unknown host

#list tables
DBI::dbListObjects(con, DBI::Id(schema = cdmSchema))
DBI::dbListObjects(con, DBI::Id(schema = writeSchema))

# created tables will start with this prefix
prefix <- "heron_benchmark"

# minimum cell counts used for suppression
minCellCount <- 5

# to create the cdm object
cdm <- CDMConnector::cdmFromCon(
  con = con,
  cdmSchema = cdmSchema,
  writeSchema =  writeSchema,
  writePrefix = prefix,
  cdmName = dbName#,
  #.softValidation = TRUE
)


# run study code
source("R/RunBenchmark.R")
