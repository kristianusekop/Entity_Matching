#Statistical Entity Matching using record linkage library from R
 #Motivation
  #Identify single entity across Alterra Bills & PDAM business data to gather unified user profile view
 #Outcome
  #Entity map table which consist of phone number & PDAM ID data to be used as master table to unify transaction data from Alterra Bills & BSA data
 #Input
  #BSA user data and Alterra Bills monthly transaction data 
 #Statistics
  #Jaro-Winkler distance
 
#Setup the required library
library(dplyr)
library(reclin)
library(RPostgreSQL)
library(dbplyr)
library(DBI)
library(bigrquery)

#Database access setup
bq_auth(path=Sys.getenv("bigQueryProd"))
projectID<-"data-platform-bq"


#Gather PDAM user data
 #From notelp field
query1<-"select nosamb as PDAMID,notelp as phone from `data-platform-bq.bsa_raw.master_pelanggan` where notelp!='-' and notelp!='' and 
extract(year from created)=extract(year from current_date()) and extract(month from created)=extract(month from (date_sub(current_date(),interval 1 month)));"
PDAMUser<-bq_table_download(bq_project_query(projectID,query1))

 #From nohp field
query1.1<-"select nosamb as PDAMID,nohp as phone from `data-platform-bq.bsa_raw.master_pelanggan` where nohp!='-' and nohp!='' and 
extract(year from created)=extract(year from current_date()) and extract(month from created)=extract(month from (date_sub(current_date(),interval 1 month)));"
PDAMUser1<-bq_table_download(bq_project_query(projectID,query1.1))

 #Bind the data
PDAMUserA<-rbind(PDAMUser,PDAMUser1)
 #dedup the combined data
PDAMUserA<-unique(PDAMUserA)
 #cleanse data which contained invalid phone data
PDAMUserA<-filter(PDAMUserA,phone!='0')

#Gather Alterra Bills user data
 #Last one month data
query2<-"select customer_id as phone, operator_code as operator from `data-platform-bq.kraken_norm.kraken_merge_transaction` where extract(year from created_at)=extract(year from current_date()) and extract(month from created_at)=extract(month from (date_sub(current_date(),interval 1 month))) and product_type in ('mobile','mobile_postpaid','data');"
BPAUser<-bq_table_download(bq_project_query(projectID,query2))

#Dedup the combined user data
BPAUserA1<-unique(BPAUser)%>%data_frame()

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
pb<-pair_blocking(PDAMUserA,BPAUserA1,"phone",large=F)

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
pb<-select_threshold(pb,"operator",var="threshold",threshold=0.7)
 #add original pair id to evaluate the model
pb<- add_from_x(pb,id_x="id")
pb<-add_from_y(pb,id_y="id")
 #add ground truth label
pb$true<-pb$id_x==pb$x
 #form confusion matrix
table(as.data.frame(pb[c("true","threshold")]))
 #One to one linkage mapping using greeedy selection and n to m method
pb<-select_greedy(pb,"operator",var="greedy",threshold=0.7)
table(as.data.frame(pb[c("true","greedy")]))
pb<-select_n_to_m(pb,"operator",var="ntom",threshold=0.7)
table(as.data.frame(pb[c("true","ntom")]))

#Obtain linked dataset
linkedData<-link(pb) 

#Subset linked-only data
linkedDataF<-filter(linkedData,is.na(linkedData$id.x)==F&is.na(linkedData$id.y)==F)

#Add created at field
linkedDataF$created_at<-as.POSIXct(Sys.time(),"%Y-%m-%d %H:%M:%S")

#Rearrange the data
linkedDataF<-linkedDataF[,c(2,3,7,8)]

#Obtain latest id
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )

maxIdHQuery<-dbSendQuery(con,'select max(id) from public."EMStat"')
maxIdH<-as.data.frame(dbFetch(maxIdHQuery))
dbDisconnect(con)

idsH<-maxIdH$max+1
ideH<-maxIdH$max+nrow(linkedDataF)
ideH-idsH
#Add id to linked data frame
linkedDataF$id<-seq(idsH,ideH,by=1)

linkedDataF<-linkedDataF[,c(5,1:4)]
colnames(linkedDataF)[c(1:4)]<-c("id","phone","operator","PDAM_ID")

#Insert to database
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )
db_insert_into(con,"transactionhistory",value=telcoHistDIndosat)

dbDisconnect(con)
