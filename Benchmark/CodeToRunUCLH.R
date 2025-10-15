# to restore renv environment
renv::restore()

#install RPostgres for UCLH
#renv::install("RPostgres")

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
  cdmName = dbName,
  .softValidation = TRUE
)

#without soft validation get
# Error in `validateCdmReference()`:
#   ! There is overlap between observation_periods, 1224607 overlaps detected for person ID 1, 3, 6, 7, and 10

# fix observation_period that got messed up in latest extract
op2 <- cdm$visit_occurrence |>
  group_by(person_id) |>
  summarise(minvis = min(coalesce(date(visit_start_datetime), visit_start_date), na.rm=TRUE),
            maxvis = max(coalesce(date(visit_end_datetime), visit_end_date), na.rm=TRUE)) |>
  left_join(select(cdm$death,person_id,death_date), by=join_by(person_id)) |>
  #set maxvisit to death_date if before
  #mutate(maxvis=min(maxvis, death_date, na.rm=TRUE))
  mutate(maxvis = if_else(!is.na(death_date) & maxvis > death_date, death_date, maxvis))

cdm$observation_period <- cdm$observation_period |>
  left_join(op2, by=join_by(person_id)) |>
  select(-observation_period_start_date) |>
  select(-observation_period_end_date) |>
  rename(observation_period_start_date=minvis,
         observation_period_end_date=maxvis)

# run study code
source("R/RunBenchmark.R")
