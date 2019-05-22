#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 15:13:20 2019

@author: gianniconceicao
"""

import mysql.connector
import datetime


def run_time_med():
    mydb = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        passwd = "abc123!@#",
        database = "elt_control_log"
    )
    mycursor = mydb.cursor(dictionary=True)
    
    sql = "select job_name from etl_control_log group by job_name;"
    mycursor.execute(sql)
    
    all_job_names = mycursor.fetchall()
    run_time_med = {}
    
    for all_job_name in all_job_names:
    
        #### NÃO PEGAR SÓ OS COMPLETOS, CASO SÓ TENHA NENHUM COMPLETO, TEM QUE VER O QUE VAI FAZER
        sql = "select start_date, end_date, status from etl_control_log where job_name = '" + all_job_name["job_name"] + "' limit 10"
        mycursor.execute(sql)
        
        aux = mycursor.fetchall()
        dif_total = datetime.timedelta()
        
        for a in aux:  # CONVERTE DATA DE STRING  PARA DATETIME
            if a["status"]  == "COMPLETED":
                
                if isinstance(a["start_date"], datetime.datetime) == False:
                    a["start_date"] = datetime.datetime.strptime(a["start_date"], '%Y-%m-%d %H:%M:%S.%f')
                if isinstance(a["end_date"], datetime.datetime) == False:
                    a["end_date"] = datetime.datetime.strptime(a["end_date"], '%Y-%m-%d %H:%M:%S.%f')
                    
                dif_atual = a["end_date"] - a["start_date"]
                dif_total += dif_atual
       
        media = dif_total/len(a)
        
        run_time_med[all_job_name["job_name"]] = media
    
    mycursor.close()
    return(run_time_med)
    
    
# =============================================================================
## MAIN PROGRAM
# =============================================================================  
mydb = mysql.connector.connect(
    host = "127.0.0.1",
    user = "root",
    passwd = "abc123!@#",
    database = "elt_control_log"
)

mycursor = mydb.cursor(dictionary=True)
sql = "select job_name, last_process_date, status from etl_control;"
mycursor.execute(sql)
etl_control = mycursor.fetchall()

sql = "select job_name from etl_control group by job_name;"
mycursor.execute(sql)
job_name_control = mycursor.fetchall()


all_job_names = []  # NOME SÓ DOS QUE ESTÃO NO CONTTROL, NÃO PRECISA DO LOG DOS QUE NÃO ESTÃO RODANDO
for jnc in job_name_control:
    all_job_names.append(jnc["job_name"])
    
for jnc in job_name_control:
    if jnc["job_name"] not in all_job_names:
        all_job_names.append(jnc["job_name"])
mycursor.close()

all_run_time = run_time_med()  # LISTA DO RUN_TIME DE TODOS QUE ESTAO NO LOG
run_time = {}  # LISTA run_time POSSUI O RUN TIME MEDIO DOS JOBNAMES PRESENTES NO LOG
no_run_time = []  # LISTA POSSUIU O JOB_NAME DAQUELES QUE NAO ESTAO NO LOG, POREM ESTAO NO CONTROL
job_name = []  # LISTA DOS job_name QUE POSSUEM RUN_TIME


for all_job_name in all_job_names:  
    try:
        run_time[all_job_name] = all_run_time[all_job_name]
        job_name.append(all_job_name)
    except KeyError:
        no_run_time.append(all_job_name)


########### INICIO DA AVALIAÇÃO

current_time = datetime.datetime.now()
for etl_c in etl_control:
    try:
        if etl_c["status"] == "RUNNING":
            time_diff = current_time - datetime.datetime.strptime(etl_c["last_process_date"], '%Y-%m-%d %H:%M:%S.%f')
            if time_diff > run_time[etl_c["job_name"]]:
                print("Tempo de execuçao maior que a média, e o programa continua em operação")
                print("Job_name: ", etl_c["job_name"], "Diferença entre o tempo de operação e a média: ",time_diff,"\n")
       
    except KeyError:
        print("O Job name ",etl_c["job_name"],"não possuiu log sobre o tempo de operação.\n e foi executado por: ", time_diff,"\n")






