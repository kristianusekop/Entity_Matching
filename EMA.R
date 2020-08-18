
#Naive matching for Telco & PDAM end customer

#initiate the library
library(DBI)
library(bigrquery)
library(dplyr)

#set up the BigQuery authentication
bq_auth(path=Sys.getenv("bigQueryProd"))

#define project ID
projectID<-"data-platform-bq"

#query for Telco and PDAM using notelp as matcher
query<-"select a.customer_id as Customer_Number,
       a.operator_code as Operator_Code,
       extract(month from a.created_at) as month,
       extract(year from a.created_at) as year,
       sum(a.sell_price) as Total_GMV_Monthly,
       count(distinct a.transaction_id) as Telco_Transaction_Monthly,
       sum(a.sell_price)/count(distinct a.transaction_id) as Average_Telco_GMV_Basket_Size_Monthly,
       b.nosamb as Nomor_Pelanggan_PDAM,
       sum(b.total) as PDAM_Billing_Amount_Total_Monthly
from   (select customer_id,operator_code,sell_price,transaction_id,created_at from `data-platform-bq.kraken_norm.kraken_merge_transaction` where DATE(created_at) between '2020-01-01' and '2020-07-30') as a
       inner join 
       ((select nosamb, total from `data-platform-bq.bsa_raw.transaction_drd` where DATE(tglupload) between '2020-01-01' and '2020-07-30') as b inner join (select nosamb,notelp from `data-platform-bq.bsa_raw.master_pelanggan` where notelp !='' and notelp!='-') as c 
on b.nosamb=c.nosamb) 
on a.customer_id=c.notelp
group by a.customer_id,extract(month from a.created_at),extract(year from a.created_at),a.operator_code,c.notelp,b.nosamb;"


#retrieve the query result
query_res <- bq_table_download(bq_project_query(projectID,query))


#query for Telco and PDAM using nohp as matcher
queryB<-"select a.customer_id as Customer_Number,
       a.operator_code as Operator_Code,
       extract(month from a.created_at) as month,
       extract(year from a.created_at) as year,
       sum(a.sell_price) as Total_GMV_Monthly,
       count(distinct a.transaction_id) as Telco_Transaction_Monthly,
       sum(a.sell_price)/count(distinct a.transaction_id) as Average_Telco_GMV_Basket_Size_Monthly,
       b.nosamb as Nomor_Pelanggan_PDAM,
       sum(b.total) as PDAM_Billing_Amount_Total_Monthly
from   (select customer_id,operator_code,sell_price,transaction_id,created_at from `data-platform-bq.kraken_norm.kraken_merge_transaction` where DATE(created_at) between '2020-01-01' and '2020-07-30') as a
       inner join 
       ((select nosamb, total from `data-platform-bq.bsa_raw.transaction_drd` where DATE(tglupload) between '2020-01-01' and '2020-07-30') as b inner join (select nosamb,nohp from `data-platform-bq.bsa_raw.master_pelanggan` where nohp !='' and nohp!='-') as c 
on b.nosamb=c.nosamb) 
on a.customer_id=c.nohp
group by a.customer_id,extract(month from a.created_at),extract(year from a.created_at),a.operator_code,b.nosamb;"

#retrieve the query result
query_res2 <- bq_table_download(bq_project_query(projectID,queryB))


#cleanse the duplicated record on the 2nd dataset
query_agg<-anti_join(query_res2,query_res,by="Customer_Number")


#bind the cleansed 2nd dataset with 1st dataset
query_agg<-rbind(query_res,query_agg)

#obtain latest id to be inserted to new records
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )

maxIdQuery<-dbSendQuery(con,"select max(id) from public.naive_EMR")

maxId<-as.data.frame(dbFetch(maxIdQuery))

ids<-maxId$max+1
ide<-maxId$max+nrow(query_agg)
ide-ids


#add id
query_agg$id<-seq(ids, ide,by=1)

#rearrange the dataframe
query_agg<-query_agg[,c(10,c(1:9))]


#insert data frame into target database
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )

db_insert_into(con,"naive_EMR",value=query_agg)

dbDisconnect(con)
