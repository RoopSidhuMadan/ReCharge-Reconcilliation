#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[84]:


import numpy as np
import pandas as pd
import fsspec
import time
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import datetime as dt
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta


# In[138]:


dates = d.today()
times = datetime.now()



key_path='C:/Users/roop.sidhu_spicemone/Downloads/roop-sidhu.json'
#f = open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'a+')
#f.close()

def main():
    
    date = d.today()-timedelta(1)
    current_date5 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date2 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(2)
    current_date3 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date4 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date6 = date.strftime('%Y%m%d')

    current_date = date.today()-timedelta(1)

    date = date.today()
    current_year = date.strftime('%Y')

    date = date.today()
    current_month = date.strftime('%m')

    date = date.today()
    current_day = date.strftime('%d')

    date = date.today()
    current_mon = date.strftime('%b')
    
    date = date.today()
    current_yr = date.strftime('%y')
    
    credentials = service_account.Credentials.from_service_account_file(key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
    project_id = 'spicemoney-dwh'
    client = bigquery.Client(credentials=credentials, project=project_id, location='asia-south1')
   
    #fa=open('/home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt', 'w')
    #fa.close()
    
    #Specifying the path of the external file
    file_path = [str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/Axis_Axis-bank miis/SPICEMONEY_CDM_MIS'
                +str(current_date6)+'*.xlsx']
    
    # file_path= 'D:/axis-bank-mis-July-22.csv'

    #---------------------------------------------------------------------------------------------------------------------
    #Loading theThink WalletRechargeLogs into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema = [{'name':'mdn','type':'STRING'},
            {'name':'status','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'amount_deducted','type':'FLOAT'},
            {'name':'rollback_amount','type':'FLOAT'},
            {'name':'txn_id','type':'STRING'},
            {'name':'client_txn_id','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'},
            {'name':'request_timestamp','type':'TIMESTAMP'},
            {'name':'response_timestamp','type':'TIMESTAMP'},
            {'name':'operator','type':'STRING'},
            {'name':'service','type':'STRING'},
            {'name':'response','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list = ['mdn',
                    'status',
                    'amount',
                    'amount_deducted',
                    'rollback_amount',
                    'txn_id',
                    'client_txn_id',
                    'operator_txn_id',
                    'request_timestamp',
                    'response_timestamp',
                    'operator',
                    'service',
                    'response']
    
    list1= ['mdn',
            'status',
            'txn_id',
            'client_txn_id',
            'operator_txn_id',
            'operator',
            'service',
            'response']
    list2=['amount',
            'amount_deducted',
            'rollback_amount']
    

    print('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/Think WalletRechargeLogs/logs.csv')
    # Reading data from excel to dataframe
    #df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+'11'+'/Think WalletRechargeLogs/logs.csv',skiprows=1,names=header_list,storage_options={"token": key_path},header=None,parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)             
    df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/Think WalletRechargeLogs/logs.csv',skiprows=1,names=header_list,storage_options={"token": key_path},header=None,parse_dates = (['request_timestamp',
'response_timestamp']), low_memory=False)  
    #df = pd.read_csv('axis-bank-mis-July-22.csv', skiprows=1, names=header_list, parse_dates = (['transaction_date', 'fncl_posted_date']), low_memory=False )
   
    df['client_txn_id']=df['client_txn_id'].str.replace(r"'",'')
  
    df[list1]=df[list1].astype(str)
    df[list2]=df[list2].astype(float)
    
    print(df)
    df.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_log table")
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the NJRI Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri = [{'name':'order_no.','type':'STRING'},
            {'name':'order_date','type':'DATE'},
            {'name':'order_time','type':'STRING'},
            {'name':'mobile_dth_no','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'recharge_status','type':'STRING'},
            {'name':'nick_name','type':'STRING'},
            {'name':'service_name','type':'STRING'},
            {'name':'service_provider','type':'STRING'},
            {'name':'service_type','type':'STRING'},
            {'name':'reason','type':'STRING'},
            {'name':'system_ref_no','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list_njri = ['order_no',
                'order_date',
                'order_time',
                'mobile_dth_no',
                'amount',
                'recharge_status',
                'nick_name',
                'service_name',
                'service_provider',
                'service_type',
                'reason',
                'system_ref_no',
                'operator_txn_id']
    
    list1_njri= ['order_no',
            'mobile_dth_no',
            'recharge_status',
            'nick_name',
            'service_name',
            'service_provider',
            'service_type',
            'reason',
            'system_ref_no',
            'operator_txn_id']
    
    list2_njri=['amount']
   

   
   # df_njri = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+'11'+'/NJRIRechargeTransactionReport/TransactionReport.xlsx',skiprows=1,names=header_list_njri,storage_options={"token": key_path},header=None)             
    df_njri = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/NJRIRechargeTransactionReport/TransactionReport.xlsx',skiprows=1,names=header_list_njri,storage_options={"token": key_path},header=None)             
   
    df_njri['order_date'] = pd.to_datetime(df_njri['order_date'])
  
    df_njri['order_date']=pd.to_datetime(df_njri['order_date'].dt.strftime('%d-%m-%Y'))
  
    
    df_njri['system_ref_no']=df_njri['system_ref_no'].str.replace(r"'",'')
    df_njri[list1_njri]=df_njri[list1_njri].astype(str)
    df_njri[list2_njri]=df_njri[list2_njri].astype(float)
   
    df_njri.to_gbq(destination_table='sm_recon.ts_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_njri,credentials=credentials)
    print("Data moved to ts_recharge_njri_transaction_log table")
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
  
   #---------------------------------------------------------------------------------------------------------------------
    #Loading the JIO Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_jio =[ {'name':'circle_id','type':'STRING'},
        {'name':'amount','type':'FLOAT'},
        {'name':'plan_id','type':'STRING'},
        {'name':'transaction_time','type':'FLOAT'},
        {'name':'trans_id','type':'STRING'},
        {'name':'refill_id','type':'STRING'},
        {'name':'result_code','type':'STRING'},
        {'name':'result_description','type':'STRING'},
        {'name':'source_agent_code','type':'STRING'},
        {'name':'source_agent_name','type':'STRING'},
        {'name':'payment_mode','type':'STRING'},
        {'name':'source_opening_balance','type':'FLOAT'},
        {'name':'source_closing_balance','type':'FLOAT'},
        {'name':'trans_type','type':'STRING'},
        {'name':'ci_status','type':'STRING'},
                 {'name':'real_time','type':'DATETIME'}
                ]
    
    
     #Specifying the header column            
    header_list_jio = ['circle_id',
                        'amount',
                        'plan_id',
                        'transaction_time',
                        'trans_id',
                        'refill_id',
                        'result_code',
                        'result_description',
                        'source_agent_code',
                        'source_agent_name',
                        'payment_mode',
                        'source_opening_balance',
                        'source_closing_balance',
                        'trans_type',
                        'ci_status','real_time']
    
    list1_jio= ['circle_id',
                'plan_id',
                'trans_id',
                'refill_id',
                'result_code',
                'result_description',
                'source_agent_code',
                'source_agent_name',
                'payment_mode',
                'trans_type',
                'ci_status']
    
    list2_jio=['amount',
                'source_opening_balance',
                'source_closing_balance','transaction_time']
    
  
    
    df_jio=pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/EmailReports/JIO_JIO/Spice Money_'+'10'+' '
                         + 'Aug'+' '+str(current_yr) +'.xlsxb', dtype=object,storage_options={"token": key_path},
                        skiprows=1,names=header_list_jio,header=None)
    df_jio['real_time'] = pd.TimedeltaIndex(df_jio['transaction_time'], unit='d') + dt.datetime(1899, 12, 30)
    
    df_jio['real_time']=pd.to_datetime(df_jio['real_time'].dt.strftime('%Y-%m-%d %H:%M:%S'))
    df_jio[list1_jio]=df_jio[list1_jio].astype(str)
    df_jio[list2_jio]=df_jio[list2_jio].astype(float)
    df_jio['refill_id']=df_jio['refill_id'].str.replace(r"'",'')
    print(df_jio.info())
    print(df_jio)
    df_jio.to_gbq(destination_table='sm_recon.ts_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='replace', table_schema = schema_jio,credentials=credentials)
    print("Data moved to ts_recharge_jio_spice_money_log table")
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    
     #---------------------------------------------------------------------------------------------------------------------
    #Loading the NJRI Reversal Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri_rev = [{'name':'order_no.','type':'STRING'},
            {'name':'order_date','type':'DATETIME'},
            {'name':'mobile_dth_no','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'service_name','type':'STRING'},
            {'name':'service_provider','type':'STRING'},
            {'name':'transaction_status','type':'STRING'},
            {'name':'system_ref_no','type':'STRING'},
            {'name':'final_status_change_date','type':'DATETIME'},
            {'name':'nick_name','type':'STRING'}       
              
              ]
                
    #Specifying the header column            
    header_list_njri_rev = ['order_no',
                'order_date',
                'mobile_dth_no',
                'amount',
                'service_name',
                'service_provider',
                'transaction_status',
                'system_ref_no',
                'final_status_change_date',
                'nick_name']
    
    list1_njri_rev= [
           'order_no',
            'mobile_dth_no',
            'service_name',
            'service_provider',
            'transaction_status',
            'system_ref_no',
            'nick_name']
    
    list2_njri_rev=['amount']
   

    df_njri_rev = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/EmailReports/NJRI_Recharge/TransactionReversalReport-283'+"-"+'11' +"-"+'08'+"-"+str(current_year) +"*.xlsx",skiprows=1,names=header_list_njri_rev ,storage_options={"token": key_path},header=None,
                                parse_dates = (['order_date','final_status_change_date']))             
   
    
    df_njri_rev [list1_njri_rev]=df_njri_rev[list1_njri_rev].astype(str)
    df_njri_rev [list2_njri_rev]=df_njri_rev[list2_njri_rev].astype(float)
    df_njri_rev['system_ref_no']=df_njri_rev['system_ref_no'].str.replace(r"'",'')
   
   
    df_njri_rev.to_gbq(destination_table='sm_recon.ts_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_njri_rev ,credentials=credentials)
    print("Data moved to ts_recharge_njri_rev_transaction_log table")
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the ThinkWallet Refund Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_tw_ref = [{'name':'mdn','type':'STRING'},
            {'name':'status','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'amount_deducted','type':'FLOAT'},
            {'name':'rollback_amount','type':'FLOAT'},
            {'name':'txn_id','type':'STRING'},
            {'name':'client_txn_id','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'},
            {'name':'request_timestamp','type':'TIMESTAMP'},
            {'name':'response_timestamp','type':'TIMESTAMP'},
            {'name':'operator','type':'STRING'},
            {'name':'service','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list_tw_ref = ['mdn',
                    'status',
                    'amount',
                    'amount_deducted',
                    'rollback_amount',
                    'txn_id',
                    'client_txn_id',
                    'operator_txn_id',
                    'request_timestamp',
                    'response_timestamp',
                    'operator',
                    'service']
    
    list1_tw_ref= ['mdn',
            'status',
            'txn_id',
            'client_txn_id',
            'operator_txn_id',
            'operator',
            'service']
    list2_tw_ref=['amount',
            'amount_deducted',
            'rollback_amount']
  

    df_tw_ref = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/Think WalletRechargeRefund file/refund.csv',skiprows=1,names=header_list_tw_ref ,storage_options={"token": key_path},header=None,
                                parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)             
   
   
   
    df_tw_ref[list1_tw_ref]=df_tw_ref[list1_tw_ref].astype(str)
    df_tw_ref[list2_tw_ref]=df_tw_ref[list2_tw_ref].astype(float)
    df_tw_ref['client_txn_id']=df_tw_ref['client_txn_id'].str.replace(r"'",'')
   
   
   
    df_tw_ref.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_tw_ref,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_refund_log table")
   
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the ReversedCommissionReport  ( NJRI) into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri_revcom = [{'name':'order_no','type':'STRING'},
                        {'name':'order_date','type':'DATETIME'},
                        {'name':'service_name','type':'STRING'},
                        {'name':'service_provider','type':'STRING'},
                        {'name':'mobile_dth_datacard','type':'STRING'},
                        {'name':'gst_type','type':'STRING'},
                        {'name':'recharge_type','type':'STRING'},
                        {'name':'system_reference_no','type':'STRING'},
                        {'name':'nick_name','type':'STRING'},
                        {'name':'recharge_amount','type':'FLOAT'},
                        {'name':'commission_percentage','type':'FLOAT'},
                        {'name':'commission_amount','type':'FLOAT'},
                        {'name':'reversal_date','type':'DATETIME'}
              ]
                
    #Specifying the header column            
    header_list_njri_revcom = ['order_no',
                            'order_date',
                            'service_name',
                            'service_provider',
                            'mobile_dth_datacard',
                            'gst_type',
                            'recharge_type',
                            'system_reference_no',
                            'nick_name',
                            'recharge_amount',
                            'commission_percentage',
                            'commission_amount',
                             'reversal_date']
    
    list1_njri_revcom= ['order_no',
                     'service_name',
                    'service_provider',
                    'mobile_dth_datacard',
                    'gst_type',
                    'recharge_type',
                    'system_reference_no',
                    'nick_name']
    list2_njri_revcom=['recharge_amount',
                    'commission_percentage',
                    'commission_amount']
  
    df_njri_revcom = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'12'+'/EmailReports/NJRI_Recharge/ReversedCommissionReport_*.xlsx',skiprows=1,names=header_list_njri_revcom ,storage_options={"token": key_path},
                                 header=None,sheet_name='Report', parse_dates = (['order_date','reversal_date']))             
   
   
    
    df_njri_revcom[list1_njri_revcom]=df_njri_revcom[list1_njri_revcom].astype(str)
    df_njri_revcom[list2_njri_revcom]=df_njri_revcom[list2_njri_revcom].astype(float)
    df_njri_revcom['system_reference_no']=df_njri_revcom['system_reference_no'].str.replace(r"'",'')
   
   
   
    df_njri_revcom.to_gbq(destination_table='sm_recon.ts_recharge_njri_reversed_commission_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_revcom,credentials=credentials)
    print("Data moved to ts_recharge_njri_reversed_commission_report table")
   
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the CommissionCalculateReport  ( NJRI) into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri_com = [{'name':'order_no','type':'STRING'},
                        {'name':'order_date','type':'DATETIME'},
                        {'name':'service_name','type':'STRING'},
                        {'name':'service_provider','type':'STRING'},
                        {'name':'mobile_dth_datacard','type':'STRING'},
                        {'name':'gst_type','type':'STRING'},
                        {'name':'recharge_type','type':'STRING'},
                        {'name':'system_reference_no','type':'STRING'},
                        {'name':'nick_name','type':'STRING'},
                        {'name':'recharge_amount','type':'FLOAT'},
                        {'name':'commission_percentage','type':'FLOAT'},
                        {'name':'commission_amount','type':'FLOAT'}
        
    ]
    
    header_list_njri_com =['order_no',
                            'order_date',
                            'service_name',
                            'service_provider',
                            'mobile_dth_datacard',
                            'gst_type',
                            'recharge_type',
                            'system_reference_no',
                            'nick_name',
                            'recharge_amount',
                            'commission_percentage',
                            'commission_amount']
    
    
    list1_njri_com= ['order_no',
                     'service_name',
                    'service_provider',
                    'mobile_dth_datacard',
                    'gst_type',
                    'recharge_type',
                    'system_reference_no',
                    'nick_name']
    
    
    list2_njri_com=['recharge_amount',
                    'commission_percentage',
                    'commission_amount']
    
    
    df_njri_com = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+'08'+'/'+'11'+'/EmailReports/NJRI_Recharge/CommissionCalculateReport_*.xlsx',sheet_name='Report',skiprows=1,names=header_list_njri_com ,storage_options={"token": key_path},
                                 header=None , parse_dates = (['order_date']))             
   
   
   
    df_njri_com[list1_njri_com]=df_njri_com[list1_njri_com].astype(str)
    df_njri_com[list2_njri_com]=df_njri_com[list2_njri_com].astype(float)
    df_njri_com['system_reference_no']=df_njri_com['system_reference_no'].str.replace(r"'",'')
   
   
   
   
    df_njri_com.to_gbq(destination_table='sm_recon.ts_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_njri_com,credentials=credentials)
    print("Data moved to ts_recharge_njri_commission_calculate_report table")
   
    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
    #print("Data moved to prod_wallet_ybl_bank_statement_log table")
    
    ################################################################################################################
    #
    ###############################################################################################################
    
    
    sql_query='''
    select Transaction_Id,trans_ref_no as Trans_Ref_No,SDL_COMMENTS,Spice_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount,AGG_NAME,
        AGG_STATUS,AGG_AMOUNT,
        coalesce(SDL_Trans_Amount,0)-coalesce(AGG_AMOUNT,0) as Difference from 
        (select Transaction_Id,
        case 
                when SDL_Refund_Amount is null then 'SUCCESS' else 'REFUND' end as SDL_STATUS,
                SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount,Spice_Status,trans_ref_no,
        case 
                when  substr(Transaction_Id,0,2)='TW' then 'THINKWALNUT'
                 when  substr(Transaction_Id,0,4)='NJRI' then 'NJRI'
                 when  substr(Transaction_Id,0,3)='JIO' then 'JIO'
                 end as SDL_COMMENTS       
        from
        (select trans_id as Transaction_Id,sum(trans_amt) as SDL_Trans_Amount,trans_status as Spice_Status,trans_date as Transaction_Date from prod_dwh.wallet_trans  where date(trans_date)="2022-08-10" and comments in ('Recharge_Mobile')
        group by Transaction_Id,trans_status,trans_date 
        ) as spice_wallet_output
        LEFT OUTER JOIN
        (  select refund_type,trans_id,refund_amt as SDL_Refund_Amount,refund_date,a.client_id,wallet_id,opening_bal,closing_bal,device_no,trans_date,trans_ref_no , comments, c.client_wallet_id as DistributorWalletId,
                from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and date(trans_date)="2022-08-10"
                UNION ALL
                select a.refund_type,a.trans_id, a.refund_amt, a.refund_date, a.client_id,a.client_id, a.opening_bal, a.closing_bal, a.device_no, a.trans_date, a.trans_ref_no , a.comments,null,
                from prod_dwh.client_refund a,prod_dwh.recharge b where date(a.trans_date)="2022-08-10" and
                a.TRANS_ID=b.trans_id and  a.refund_type="Recharge"
        ) as refund_report_output
        ON spice_wallet_output.Transaction_Id=refund_report_output.trans_id
        )
        LEFT OUTER JOIN
        (
            select client_txn_id as TRANS_ID,status as AGG_STATUS,sum(amount) as AGG_AMOUNT,date(request_timestamp) as AGG_DATE,
           case 
                when  substr(client_txn_id,0,2)='TW' then 'TW'
                 end as AGG_NAME       
            from `sm_recon.ts_recharge_think_wallet_log`
          where date(request_timestamp)="2022-08-10"
          group by status,client_txn_id,date(request_timestamp)
          UNION ALL
          select system_ref_no,recharge_status,sum(amount),order_date,
           case 
                 when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                 end as AGG_NAME
                  from `sm_recon.ts_recharge_njri_transaction_log`
          where date(order_date)="2022-08-10" 
          group by system_ref_no,order_date,recharge_status
          UNION ALL
          select refill_id,result_description,sum(amount),date(real_time),
           case 
                 when  substr(refill_id,0,3)='JIO' then 'JIO'
                 end as AGG_NAME from `sm_recon.ts_recharge_jio_spice_money_log`
          where date(real_time)="2022-08-10"
            group by refill_id,result_description,date(real_time)
        ) as aggregator_output
        ON Transaction_Id= aggregator_output.TRANS_ID
    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_spice_vs_tw_njri_jio', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    #job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_spice_vs_tw_njri_jio', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    ################################################################################################################
    #5-6 Commission Validation query
    ###############################################################################################################
    
    
    
    sql_query='''
            select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,AGG_AMOUNT,Difference_AmntTransfer_ComAmount,round(Calculated_Commission,4) as Calculated_Commission ,ACTUAL_COMMISSION as Actual_Commission,round(Diff_Cal_Vs_Actual_Commission,4) as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE,
        Cal_Commission_Percentage,round(Diff_Cal_Vs_Actual_Charges_Percentage,4) as Diff_Cal_Vs_Actual_Charges_Percentage,
        round(AGG_COMMISSION_PERCENTAGE,4) as AGG_COMMISSION_PERCENTAGE from 


        (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,AGG_AMOUNT,round(DIFFERENCE,4) as Difference_AmntTransfer_ComAmount,Calculated_Commission,ACTUAL_COMMISSION as Actual_Commission,
        Calculated_Commission-ACTUAL_COMMISSION as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE,
        round(Cal_Commission_Percentage,4) as Cal_Commission_Percentage,
        (ACTUAL_COMMISSION_PERCENTAGE-Cal_Commission_Percentage) as Diff_Cal_Vs_Actual_Charges_Percentage,
        AGG_COMMISSION_PERCENTAGE from 
        (
        select TRANSFER_DATE,CLIENT_ID,round(AMOUNT_TRANSFERRED,4) as AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,round(COMMISSION_AMOUNT,4) as COMMISSION_AMOUNT,round(AGG_AMOUNT,4) as AGG_AMOUNT,
        AMOUNT_TRANSFERRED-COMMISSION_AMOUNT as DIFFERENCE,
        AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
        (AGG_AMOUNT*ACTUAL_COMMISSION_PERCENTAGE)/100 as  ACTUAL_COMMISSION,
        (Calculated_Commission/AMOUNT_TRANSFERRED)*100 as Cal_Commission_Percentage,
        ACTUAL_COMMISSION_PERCENTAGE, 
        (COMMISSION_AMOUNT/AGG_AMOUNT)*100 as AGG_COMMISSION_PERCENTAGE from 
        (
        select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,AGG_AMOUNT,AMOUNT_TRANSFERRED-COMMISSION_AMOUNT as DIFFERENCE,
        AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
        CASE
          when OPERATOR_TITLE IN ('AIRTEL','RELIANCE_JIO') then 0.50
          when device_type="Mobile" and AGG_AMOUNT <200 then 0.75
          when device_type="Mobile" and AGG_AMOUNT >=200 then 2.00
          when device_type="DTH" then 1.50
          end as ACTUAL_COMMISSION_PERCENTAGE
         from 
        (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,
        CASE
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Commission_Amount
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Commission_Amount
          end as COMMISSION_AMOUNT,
        CASE
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Amount
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Amount
          end as AGG_AMOUNT
        from
        (select date(t1.transfer_date) as TRANSFER_DATE,
        t2.retailer_id AS CLIENT_ID,
        sum(t1.amount_transferred) as AMOUNT_TRANSFERRED,
        t1.comments as COMMENTS,t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
        t1.trans_type as TRANS_TYPE,
        case 
                when  substr(t1.unique_identification_no,0,2)='TW' then 'THINKWALNUT'
                 when  substr(t1.unique_identification_no,0,4)='NJRI' then 'NJRI'
                 when  substr(t1.unique_identification_no,0,3)='JIO' then 'JIO'
                 end as AGG_ID       
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
                JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
                where t1.comments IN ('IRCTC Recharge Discount','RECHARGE-Discount-Mobile') and 
                DATE(t1.transfer_date) = "2022-08-10"
                GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID,COMMENTS,TRANS_TYPE,TRANSFER_DATE,AGG_ID
        ) as FTR_revoke_output
        LEFT OUTER JOIN
        (
         select t1.trans_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id
        ) as recharge_output
        ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
        LEFT OUTER JOIN
        (
          select commission_amount as NJRI_Commission_Amount,recharge_amount as NJRI_Amount,system_reference_no from `sm_recon.ts_recharge_njri_commission_calculate_report`
        ) as njri_commission_output
        ON njri_commission_output.system_reference_no=FTR_UNIQUE_IDENTIFICATION_NO
        LEFT OUTER JOIN
        (
          select amount as TW_Amount,amount_deducted,client_txn_id,amount-amount_deducted as TW_Commission_Amount from `sm_recon.ts_recharge_think_wallet_log` where status in ('Success','Pending','Rollback')
        ) as tw_commision_output
        ON tw_commision_output.client_txn_id=FTR_UNIQUE_IDENTIFICATION_NO
        )
        )
        )
        )
    
    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_vs_njri_tw_commission_validation', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    #job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_vs_njri_tw_commission_validation', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()
    
    
    
    ################################################################################################################
    #Recharge Commission Summary
    ###############################################################################################################
    
    sql_query='''
            select COMMENTS,TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,device_type,OPERATOR_TITLE
        from
        (select date(t1.transfer_date) as TRANSFER_DATE,
        t2.retailer_id AS CLIENT_ID,
        sum(t1.amount_transferred) as AMOUNT_TRANSFERRED,
        t1.comments as COMMENTS,t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
        case 
                when  substr(t1.unique_identification_no,0,2)='TW' then 'THINKWALNUT'
                 when  substr(t1.unique_identification_no,0,4)='NJRI' then 'NJRI'
                 when  substr(t1.unique_identification_no,0,3)='JIO' then 'JIO'
                 end as AGG_ID       
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
                JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
                where t1.comments IN ('IRCTC Recharge Discount','RECHARGE-Discount-Mobile','RECHARGE-Discount-Mobile-Reversal') and 
                DATE(t1.transfer_date) = "2022-08-10"
                GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID,COMMENTS,TRANS_TYPE,TRANSFER_DATE,AGG_ID
        ) as FTR_revoke_output
        LEFT OUTER JOIN
        (
         select t1.trans_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id
        ) as recharge_output
        ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
    '''
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_commission_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    #job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_commission_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()
main()


# In[ ]:





# In[ ]:




