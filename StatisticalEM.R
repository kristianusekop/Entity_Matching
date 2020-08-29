#Statistical Entity Matching using record linkage library from R
 #Motivation
  #Identify single entity across Alterra Bills & PDAM business data to gather unified user profile view
 #Outcome
  #Entity map table which consist of phone number & PDAM ID data to be used as master table to unify transaction data from Alterra Bills & BSA data
 #Input
  #BSA user data and Alterra Bills transaction data from Jan-Jul 2020
 
#Setup the required library
library(dplyr)
library(reclin)
library(RPostgreSQL)
library(dbplyr)

#Gather PDAM user data
 #From notelp field
query1<-"select nosamb as PDAMID,notelp as phone from `data-platform-bq.bsa_raw.master_pelanggan` where notelp!='-' and notelp!='';"
PDAMUser<-bq_table_download(bq_project_query(projectID,query1))

 #From nohp field
query1.1<-"select nosamb as PDAMID,nohp as phone from `data-platform-bq.bsa_raw.master_pelanggan` where nohp!='-' and nohp!='';"
PDAMUser1<-bq_table_download(bq_project_query(projectID,query1.1))

 #Bind the data
PDAMUserA<-rbind(PDAMUser,PDAMUser1)
 #dedup the combined data
PDAMUserA<-unique(PDAMUserA)
 #cleanse data which contained invalid phone data
PDAMUserA<-filter(PDAMUserA,phone!='0')

#Gather Alterra Bills user data
 #January data
query2<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='1' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser<-bq_table_download(bq_project_query(projectID,query2))

 #February data
query3<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='2' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser2<-bq_table_download(bq_project_query(projectID,query3))

 #March data
query4<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='3' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser3<-bq_table_download(bq_project_query(projectID,query4))

 #April data
query5<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='4' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser4<-bq_table_download(bq_project_query(projectID,query5))

 #May data
query6<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='5' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser5<-bq_table_download(bq_project_query(projectID,query6))

 #June data
query7<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='6' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser6<-bq_table_download(bq_project_query(projectID,query7))

 #July data
query8<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where cast(extract(year from created_at)as string)='2020' and cast(extract(month from created_at)as string)='7' and product_type in ('mobile','mobile_postpaid','data');"
BPAUser7<-bq_table_download(bq_project_query(projectID,query8))

#Combine the user data
BPAUserA<-rbind(BPAUser,BPAUser2,BPAUser3,BPAUser4,BPAUser5,BPAUser6,BPAUser7)
#Dedup the combined user data
BPAUserA1<-unique(BPAUserA)%>%data_frame()
 #Rename the customer ID field to be phone
colnames(BPAUserA1)<-"phone"

#Add operator field in PDAM user data as linkage pair key
PDAMUserA<-merge(x=PDAMUserA,y=BPAUserA1,by="phone",all.x=T)
 #Replace NA value with blank
PDAMUserA[is.na(PDAMUserA)]<-''

#add id to Alterra Bills user data & PDAM user data

BPAUserA1$id<-seq(1,nrow(BPAUserA1),by=1)
BPAUserA1<-BPAUserA1[,c(3,1:2)]
PDAMUserA$id<-seq(1,nrow(PDAMUserA),by=1)
PDAMUserA<-PDAMUserA[,c(4,1,2,3)]

#record linkage
 #use phone as blocking parameter
pb<-pair_blocking(BPAUserA1,PDAMUserA,"phone",large=F)

#compare pairs
 #Binary comparison using oparator as linkage pair key
pb<-compare_pairs(pb,by="operator")
 #Distance-based comparison using `jaro_winkler` with 90% CI
pb<-compare_pairs(pb,by="operator",default_comparator = jaro_winkler(0.9),overwrite = T)
 #Score pairs
pb<-score_simsum(pb,var="simsum")
 #probabilistic linkage scoring
mp<-problink_em(pb)
 #scoring using the m & u probability model
pb<-score_problink(pb,model=mp,var="weight")
 #select pairs using threshold (0.02)
pb<-select_threshold(pb,"weight",var="threshold",threshold=0.02)
 #add original pair id to evaluate the model
pb<- add_from_x(pb,id_x="id")
pb<-add_from_y(pb,id_y="id")
 #add ground truth label
pb$true<-pb$id_x==pb$x
 #form confusion matrix
table(as.data.frame(pb[c("true","threshold")]))
 #One to one linkage mapping using greeedy selection and n to m method
pb<-select_greedy(pb,"weight",var="greedy",threshold=0)
table(as.data.frame(pb[c("true","greedy")]))
pb<-select_n_to_m(pb,"weight",var="ntom",threshold=0)
table(as.data.frame(pb[c("true","ntom")]))

#Obtain linked dataset
linkedData<-link(pb)

#Subset linked-only data
linkedDataF<-filter(linkedData,is.na(linkedData$id.x)==F&is.na(linkedData$id.y)==F)

#Rearrange the data
linkedDataF<-linkedDataF[,c(2,3,6)]
linkedDataF$id<-seq(1,nrow(linkedDataF),by=1)
linkedDataF<-linkedDataF[,c(4,1:3)]
colnames(linkedDataF)<-c("id","phone","operator","PDAM_ID")

#Insert to database
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )

dbWriteTable(con,"EMStat",value=linkedDataF,overwrite=T,append=F,row.names=FALSE)
#db_insert_into(con,"transactionhistory",value=telcoHistDIndosat)

dbDisconnect(con)
