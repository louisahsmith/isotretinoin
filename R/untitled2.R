# Packages
library(CohortIncidence)

# ==== setup ===================================================================

# Credentials
usr <- keyring::key_get("lab_user")
pw <- keyring::key_get("lab_password")

# DB Connections
atlas_url <- "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema <- "omop_cdm_53_pmtx_202203"
my_schema <- paste0("work_", usr)

# Create the connection
con <- connect(
  dbms = "redshift",
  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
  port = 5439,
  user = usr,
  password = pw
)

# ddl <- SqlRender::render(getResultsDdl(),
#                          "schemaName.incidence_summary" = 
#                            paste0(my_schema, ".isotretinoin_incidence_summary"))
# 
# dbRemoveTable(con, 
#                                  DBI::Id(schema = my_schema,
#                                     table = "isotretinoin_incidence_summary"))
# executeSql(con, ddl)
# dbRemoveTable(con, 
#               DBI::Id(schema = "public",
#                       table = "work_usr9isotretinoin_incidence_summary"))

t1 <- createCohortRef(id = 2511, name = "Reproductive age")

o1 <- createOutcomeDef(id = 1, name = "Isotretinoin", 
                       cohortId = 2510, cleanWindow = 9999)

tar1 <- createTimeAtRiskDef(id = 1, startWith = "start", endWith = "end")

# Note: c() is used when dealing with an array of numbers, 
# later we use list() when dealing with an array of objects
analysis1 <- createIncidenceAnalysis(targets = c(t1$id),
                                     outcomes = c(o1$id),
                                     tars = c(tar1$id))

# subgroup1 <- createCohortSubgroup(id=1, name="Subgroup 1", 
#                                   cohortRef = createCohortRef(id=300))


# Create Design (note use of list() here):
irDesign <- createIncidenceDesign(targetDefs = list(t1),
                                                   outcomeDefs = list(o1),
                                                   tars=list(tar1),
                                  # subgroups = list(subgroup1),
                                                   analysisList = list(analysis1),
                                  strataSettings = createStrataSettings(
                                    byYear = TRUE,
                                    byGender = TRUE
                                  ))


ir <- executeAnalysis(connection = con,
                incidenceDesign = irDesign,
                buildOptions = buildOptions(cohortTable = "work_usr9.repro_age",
                                            cdmDatabaseSchema = cdm_schema,
                                            resultsDatabaseSchema = my_schema,
                                            refId = 1)
                )

library(tidyverse)
ggplot(filter(ir, GENDER_NAME %in% c("MALE", "FEMALE"), !is.na(START_YEAR)),
       aes(START_YEAR, INCIDENCE_RATE_P100PY, color = GENDER_NAME)) +
  geom_line()


targetDialect <- attr(con, "dbms")
# 
# tempDDL <- SqlRender::translate(CohortIncidence::getResultsDdl(useTempTables=T), 
#                                 targetDialect = targetDialect)
# DatabaseConnector::executeSql(con, tempDDL)

#execute analysis
irDesign_chr <- as.character(irDesign$asJSON())


analysisSql <- CohortIncidence::buildQuery(incidenceDesign =  irDesign_chr,
                                           buildOptions = buildOptions(cohortTable = "work_usr9.repro_age",
                                                                       cdmDatabaseSchema = cdm_schema,
                                                                       resultsDatabaseSchema = my_schema,
                                                                       refId = 1))

analysisSql <- SqlRender::translate(analysisSql, targetDialect = targetDialect)

writeLines(analysisSql, "incidence_sql.sql")



analysisSqlQuries <- SqlRender::splitSql(analysisSql)

DatabaseConnector::executeSql(con, analysisSql)

#download results into dataframe
results <- DatabaseConnector::querySql(con, SqlRender::translate("select * from #incidence_summary", targetDialect = targetDialect))

# use the getCleanupSql to fetch the DROP TABLE expressions for the tables that were created in tempDDL.
cleanupSql <- SqlRender::translate(CohortIncidence::getCleanupSql(useTempTables=T), targetDialect)  
DatabaseConnector::executeSql(con, cleanupSql)