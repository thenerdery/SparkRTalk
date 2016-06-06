# SparkRTalk

## Table of Contents
  1. [Overview](#overview)
  2. [Files](#files)

## <a name="overview"><a/>Overview

#### Conference
MinneAnalytics Big Data Tech 2016

#### Presenter
Chad Dvoracek

Data Engineer, The Nerdery

#### Title
R Studio Server on Amazon EMR

#### Abstract
Explore the convenience of the popular IDE for R while harnessing 
the power of SparkR (R on Spark) for distributed processing. See how to 
quickly set up R Studio Server on an EMR cluster and access the IDE via any 
web browser.  

#### Keywords: 
R Studio, R Studio Server on EMR, Distributed Data Frames, Machine Learning, SQL Context, fast aggregation. 
 

## <a name="files"><a/>Files

#### boot_all.sh
Use for bootstrap step to initialize rstudio user on each node in the cluster

#### prepare_r.sh
Bash script example on how to load data, install R Studio Server and scripts.

#### bigdata2016.R
R script used for tutorial.

#### loan.hql
Hive script used to create an external table on the cleaned data set.


