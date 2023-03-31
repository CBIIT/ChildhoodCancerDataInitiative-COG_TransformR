#!/usr/bin/env Rscript

#COG_TransformR.R


##################
#
# USAGE
#
##################

#This script takes a directory of COG JSON files and transforms them into a table.

#Run the following command in a terminal where R is installed for help.

#Rscript --vanilla COG_TransformR.R --help

##################
#
# Env. Setup
#
##################

#List of needed packages
list_of_packages=c("dplyr","tidyr","jsonlite","stringi","readr","xlsx","optparse","tools")

#Based on the packages that are present, install ones that are required.
new.packages <- list_of_packages[!(list_of_packages %in% installed.packages()[,"Package"])]
suppressMessages(if(length(new.packages)) install.packages(new.packages, repos = "http://cran.us.r-project.org"))

#Load libraries.
suppressMessages(library(dplyr,verbose = F))
suppressMessages(library(tidyr,verbose = F))
suppressMessages(library(jsonlite,verbose = F))
suppressMessages(library(stringi,verbose = F))
suppressMessages(library(readr,verbose = F))
suppressMessages(library(optparse,verbose = F))
suppressMessages(library(tools,verbose = F))


#remove objects that are no longer used.
rm(list_of_packages)
rm(new.packages)


##################
#
# Arg parse
#
##################

#Option list for arg parse
option_list = list(
  make_option(c("-d", "--directory"), type="character", default=NULL, 
              help="Model file yaml", metavar="character")
)


#create list of options and values for file input
opt_parser = OptionParser(option_list=option_list, description = "\nCOG_TransformR.R v1.0.0\n\nThis script takes a directory of COG JSON files and transforms them into a table.\n")
opt = parse_args(opt_parser)

directory_path=file_path_as_absolute(opt$directory)

#Make sure that the directory ends with a "/"
if (substr(x = directory_path,start = nchar(directory_path), stop = nchar(directory_path))!="/"){
  directory_path=paste(directory_path,"/",sep = "")
}


####################
#
# Ingest Data from Directory
#
####################

cat(paste("\nReading in COG JSON files.\n",sep = "")) 

#pull list of cog json files
json_files=list.files(directory_path)

#ensure to only pull json files
json_files=json_files[grepl(pattern = ".json",x = json_files)]

#capture the setup of the JSON to create one long df that contains all the data from each JSON.
df=fromJSON(paste(directory_path,json_files[1],sep = "")) %>% as.data.frame

forms=unique(df$forms.form_name)

df_all_cols=c(colnames(df),colnames(df$forms.data[[1]]))
df_all_cols=df_all_cols[-grep(pattern = "forms.data",x = df_all_cols)]

df_all=data.frame(matrix(nrow=1, ncol=length(df_all_cols)))
df_all[1,]=""
colnames(df_all)<-df_all_cols

#Setup for progress bar
pb=txtProgressBar(min=0,max=length(json_files),style = 3)
x=0

#go through each participant JSON and pull out the information that pertains to the column names and place each piece of information per row.
for (json in json_files){
  
  #progress bar
  setTxtProgressBar(pb,x)
  x=x+1
  
  df=fromJSON(paste(directory_path,json,sep = "")) %>% as.data.frame
  forms=unique(df$forms.form_name)
  
  for (form in forms){
    form_loc=grep(pattern = TRUE,x = df$forms.form_name %in% form)
    df_out=df$forms.data[form_loc][[1]]
    df_out=mutate(df_out,upi=df$upi[form_loc],date_of_extraction=df$date_of_extraction[form_loc],version=df$version[form_loc],index_date_type=df$index_date_type[form_loc],forms.form_name=df$forms.form_name[form_loc],forms.form_id=df$forms.form_id[form_loc]) 
    df_out= mutate(df_out, across(everything(), as.character))
    as.character(df_out)
    df_all=bind_rows(df_all,df_out)
  }
  
}

cat(paste("\n\nTransforming the COG JSON files into a data frame.\n",sep = "")) 

#remove first empty row that was made to ensure it could be added to the data frame
df_all=df_all[-1,]

#create a new column this is the form and field id, somewhat like a node and property.
df_all=mutate(df_all, forms.field_id=paste(forms.form_id,".",form_field_id,sep=""))


#Make into flat data frame on a per participant setup.
form_fields=unique(df_all$forms.field_id)

#set up a new data frame to spread out to based on the form field ids that are present.
df_spread_base=data.frame(matrix(nrow=1, ncol=length(form_fields)+1))
colnames(df_spread_base)<-c("upi",form_fields)
df_spread_add=df_spread_base
df_spread_add_reset=df_spread_base
df_spread=df_spread_base[0,]

#for each participant
for (participant in unique(df_all$upi)){
  df_all_filter=filter(df_all, upi==participant)
  df_spread_add$upi=participant
  #for each property in the json
  for (property in unique(df_all_filter$forms.field_id)){
    #find the value of the property 
    value_add=df_all_filter$value[grep(pattern = TRUE, x = df_all_filter$forms.field_id %in% property)]
    if (length(value_add)>1){
      value_add=paste(value_add,collapse = ";")
    }
    #and add it to the add data frame
    df_spread_add[property]=value_add
  }
  #Then add that single participant data frame back into the data frame for all participants.
  df_spread=rbind(df_spread, df_spread_add)
  df_spread_add=df_spread_add_reset
}


#####################
#
# Data frame manipulation
#
#####################

# At this point in the script, this section will be used to take elements and convert them into a more usable version for CCDI data ingestion. 

#A simple setup will be made to demonstrate the setup of the code as well as create a template.

df_out=df_spread %>%
  
  #mutate column change names
  mutate(
         participant_id=upi,
         sex=DEMOGRAPHY.DM_SEX,
         race=DEMOGRAPHY.DM_CRACE,
         ethnicity=DEMOGRAPHY.DM_ETHNIC,
         #days_from_enrollment with days_from_birth date to get age at diagnosis
         days_from_enrollment=COG_UPR_DX.DX_DT, 
         days_from_birth=DEMOGRAPHY.DM_BRTHDAT,
         primary_site=COG_UPR_DX.TOPO_TEXT
         ) %>%
  
  #followed by a deselect to remove redundant information if needed
  select(
         -upi,
         -DEMOGRAPHY.DM_SEX,
         -DEMOGRAPHY.DM_CRACE,
         -DEMOGRAPHY.DM_ETHNIC,
         -COG_UPR_DX.DX_DT,
         -DEMOGRAPHY.DM_BRTHDAT,
         -COG_UPR_DX.TOPO_TEXT
         ) %>%
  
  #mutates that allow for a new property to be formed
  mutate(
         age_at_diagnosis=(abs(as.numeric(days_from_birth))-abs(as.numeric(days_from_enrollment))),
         diagnosis_id=paste(participant_id,COG_UPR_DX.ADM_DX_CD_SEQ, sep = "_" ),
         diagnosis_icd_o=paste(COG_UPR_DX.MORPHO_ICDO, " : ",COG_UPR_DX.MORPHO_TEXT,sep = "")
        ) %>%
  
  #clean up the diagnosis_icd_o by removing the C values and extra info
  separate(
    diagnosis_icd_o, c("diagnosis_icd_o","Cs"),sep = " [(]C", extra="drop", fill="right"
        )%>%
  
  select(
         -Cs
        )%>%
  
  #finally followed by another select to bring properties of interest to the front of the file
  select(
         participant_id,
         sex,
         race,
         ethnicity,
         days_from_birth,
         days_from_enrollment,
         age_at_diagnosis,
         diagnosis_id,
         diagnosis_icd_o,
         everything()
         )


####################
#
# Write Out
#
####################

path=paste(dirname(directory_path),"/",sep = "")

#Output file name based on input file name and date/time stamped.
output_file=paste("COG_participant_table_",
                  stri_replace_all_fixed(
                    str = Sys.Date(),
                    pattern = "-",
                    replacement = ""),
                  sep="")

write_tsv(x = df_out, file = paste(path,output_file,".tsv", sep=""), na="")

cat(paste("\n\nProcess Complete.\n\nThe output file can be found here: ",path,"\n\n",sep = "")) 
