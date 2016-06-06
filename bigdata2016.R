



# Set this to where Spark is installed
Sys.setenv(SPARK_HOME="/usr/lib/spark")
Sys.getenv()

#Load Library and initialize spark context
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

#Get local environment info
Sys.info()
library("parallel", lib.loc="/usr/lib64/R/library")
detectCores(all.tests = FALSE, logical = TRUE)

#Load Library 
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

#Initialize Spark Contect
sc <- sparkR.init(master = "yarn-client", sparkPackages="com.databricks:spark-csv_2.11:1.4.0",sparkEnvir = list(spark.num.executors='5', spark.executor.cores='5',spark.executor.memory="2g", spark.driver.memory="8g"))

#Create SQLContext
sqlContext <- sparkRSQL.init(sc)

#create rdd data frame
loan <- read.df(sqlContext, "/data/clean/loan.txt",source = "com.databricks.spark.csv", header="true", inferSchema = "true", delimiter = "|")

#see head
head(loan)
take(loan, 10)

#information on a column
typeof(take(loan, 2) [["loan_amnt"]])

# [1] "double"

#rows in the data set
count(loan)
nrow(loan)


printSchema(loan)

#Register dataframe as Table
registerTempTable(loan, "loanTemp")


#Test TempTable
print(head(sql(sqlContext, "Select * from loanTemp limit 5")))


print(head(sql(sqlContext, "Select count(*) from loanTemp")))


#collect the data locally
loans_loan_amt <- collect(select(loan, "loan_amnt"))


#boxplot loan amount
boxplot(loans_loan_amt)

#disable scientific notation
options(scipen=999)

#plot
hist(loans_loan_amt$loan_amnt)


head(summarize(groupBy(loan, loan$loan_status), count = n(loan$loan_status)))

# loan_status                                            count
# 1 Does not meet the credit policy. Status:Charged Off    761
# 2                                         Charged Off  45248
# 3                                          Fully Paid 207723
# 4                                     In Grace Period   6253
# 5  Does not meet the credit policy. Status:Fully Paid   1988
# 6                                             Current 601779

print(head(sql(sqlContext, "Select distinct(loan_status) from loanTemp")))


#Create data set of charged off loans

chargedOff <- filter(loan, loan$loan_status %in% c("Does not meet the credit policy. Status:Charged Off", "Charged Off"))


take(chargedOff, 15)
count(chargedOff)
# [1] 46009

print(head(sql(sqlContext, "Select distinct(loan_status) from loanTemp where loan_status like '%Charged Off%'")))

#   loan_status
# 1 Does not meet the credit policy. Status:Charged Off
# 2                                         Charged Off



# cache data set in memory
cache(chargedOff)
count(chargedOff)

chargedOff_collect <- collect(chargedOff)
loans_loan_amt <- collect(select(loan, "loan_amnt"))

#Selected features


#Test TempTable
print(head(sql(sqlContext, "Select * from loanTemp limit 5")))

##Split data
training <- sql(sqlContext, "Select * from loanTemp 
                where issue_d like '%2015%'or issue_d like '%2014%'")

head(training)


#get data from Hive table
#create hive context
hiveContext <- sparkRHive.init(sc)

#Query hive table
results = collect(sql(hivecontext, "From bd2016.loans SELECT * limit 15"))






