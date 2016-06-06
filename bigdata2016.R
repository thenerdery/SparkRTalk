

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


##get data from Hive table
#create hive context
hiveContext <- sparkRHive.init(sc)


#Query hive table
results = collect(sql(hiveContext, "From bd2016.loans SELECT * limit 15"))

print(results)

#collect the data locally
loans_loan_amt <- collect(select(loan, "loan_amnt"))


#boxplot loan amount
boxplot(loans_loan_amt, main="Loan Amount")

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
##plot something plot(chargedOff$grade ~ chargedOff$loan_amnt)



#Selected features
features <- select(loan, "id", "member_id", "loan_amnt", "term", "int_rate" , "installment" , "grade", 
                           "emp_length", "home_ownership", "annual_inc", "verification_status", "issue_d", 
                           "loan_status", "purpose", "title", "zip_code", "addr_state", "inq_last_6mths", 
                           "open_acc", "total_acc",   "application_type", "annual_inc_joint", "dti_joint", 
                           "open_il_12m", "open_il_24m", "mths_since_rcnt_il", "total_bal_il", "il_util", 
                           "open_rv_12m", "open_rv_24m", "all_util", "inq_last_12m")

take(features, 5)
printSchema(features)
count(features)
# [1] 887379

#Register dataframe as Table
registerTempTable(features, "featTemp")

#Test TempTable
print(head(sql(sqlContext, "Select * from featTemp limit 5")))

##Split data
##prepare test data set
test <- filter(features, features$issue_d %in% c('Oct-2015', 'Nov-2015', 'Dec-2015')  )
head(test)
cache(test)
count(test)
# [1] 130503

test1 <- dropna(select(test, "loan_status", "loan_amnt", "term", "int_rate" , 
                       "installment" ,"emp_length","home_ownership", "annual_inc"),
                how = c("any"),minNonNulls = NULL, cols = NULL) 
cache(test1)
count(test1)
# [1] 130503

##prepare training data set

training <-sql(sqlContext, "Select * from featTemp where lower(issue_d)
               not in ('oct-2015', 'nov-2015', 'dec-2015')")



head(training)
cache(training)
count(training)
# [1] 756876

train1 <- dropna(select(training, "loan_status", "loan_amnt", "term", "int_rate" , 
                        "installment" ,"emp_length","home_ownership", "annual_inc"),
                 how = c("any"),minNonNulls = NULL, cols = NULL) 
cache(train1)
count(train1)
# [1] 756872
printSchema(train1)


# loan_status                                            count
# 1 Does not meet the credit policy. Status:Charged Off    761
# 2                                         Charged Off  45248
# 3                                          Fully Paid 207723
# 4                                     In Grace Period   6253
# 5  Does not meet the credit policy. Status:Fully Paid   1988
# 6                                             Current 601779

model<- glm(loan_status ~ loan_amnt + term + int_rate + emp_length + home_ownership + 
              installment + annual_inc, data=train1, family = "gaussian")

summary(model)
 
# $devianceResiduals
# Min       Max     
# -2.381049 7.762452
# 
# $coefficients
# Estimate         Std. Error       t value     Pr(>|t|)            
# (Intercept)             -0.06578767      0.625183         -0.1052295  0.9161938           
# loan_amnt               -0.000009816721  0.0000008824796  -11.12402   0                   
# term_ 36 months         0.1696507        0.005868095      28.9107     0                   
# int_rate                0.03686407       0.0003264953     112.9084    0                   
# emp_length_10+ years    -0.02872056      0.005444261      -5.275382   0.0000001325159     
# emp_length_2 years      0.02347974       0.006173394      3.803376    0.0001427489        
# emp_length_3 years      0.02142397       0.006294302      3.403709    0.0006648095        
# emp_length_< 1 year     0.05688831       0.006307703      9.018863    0                   
# emp_length_1 year       0.03884607       0.006530863      5.948076    0.000000002714315   
# emp_length_5 years      0.04569605       0.006539677      6.987509    0.000000000002800427
# emp_length_4 years      0.03327813       0.006620052      5.026868    0.0000004986693     
# emp_length_7 years      0.02052854       0.006792503      3.022235    0.002509237         
# emp_length_6 years      0.07078124       0.006864213      10.31163    0                   
# emp_length_8 years      -0.01264405      0.006885066      -1.836445   0.06629225          
# emp_length_n/a          -0.08827391      0.006934716      -12.72927   0                   
# home_ownership_MORTGAGE 0.03093405       0.6250988        0.04948666  0.9605315           
# home_ownership_RENT     0.0250549        0.6250992        0.04008148  0.9680282           
# home_ownership_OWN      -0.01576471      0.6251058        -0.02521926 0.9798801           
# home_ownership_OTHER    1.721965         0.6285235        2.739699    0.006149699         
# home_ownership_NONE     0.6739233        0.6385426        1.055409    0.2912388           
# installment             0.0001741435     0.00002801952    6.215078    0.0000000005132577  
# annual_inc              0.00000007451345 0.00000001854771 4.017393    0.00005885118       

predictions <- predict(model, newData = test1)
preds <- select(predictions, "loan_status", "prediction")
head(preds)
take(preds, 50)
