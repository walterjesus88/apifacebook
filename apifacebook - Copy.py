#!/usr/bin/env python
# coding: utf-8
#https://discuss.streamlit.io/t/running-etl-jobs-using-streamlit/1618/8
# In[9]:

#estoy en develop
#import boto3 
import sys
import sys
import os
import requests, json, datetime, csv, time
import random
from random import randint
from datetime import datetime,timedelta
from datetime import date
import pandas as pd
from collections import defaultdict
from pandas import ExcelWriter
#from pandas.io.json import json_normalize
from pandas import json_normalize
import numpy as np
from textblob import TextBlob
import time
import datetime
import pyodbc
import re
import string


#from sqlalchemy import create_engine
#pyodbc.connect('Driver={FreeTDS};SERVER='+server+’;DATABASE=’+database+’;UID=’+username+’;PWD=’+password)

''' PARAMETROS DE LA CONEXION A SQL '''
# params = urllib.parse.quote_plus(   
#     'Driver={SQL Server Native Client 17.0};'
#                         'Server=119.8.153.140;'
#                         'Database=IPE.DW;'
#                         'Trusted_Connection=no;'
#                         'uid=UserData;'
#                         'pwd=1P32020'     
#                          )

#conn = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
# sql='''SELECT PEDIDO,EMAIL,PRODUCTO,CATEGORIA2 FROM [dbo].[NESTLE_CORRELACIONES]'''
# MyData = pd.read_sql_query(sql, db)


pd.options.mode.chained_assignment = None

conn = pyodbc.connect(
	'Driver={SQL Server Native Client 11.0};'
	                      'Server=DESKTOP-FKSQHGG\MSSQLSERVER2016;'
	                      'Database=IPE.DW;'
                          'Trusted_Connection=yes;'
                          #'uid=UserData;'
                          #'pwd=1P32020'
)

#ayer = datetime.date(2020,5,1)
#ahora = datetime.date(2020,11,30)
#print(ayer)
#print(ahora)

ahora = datetime.datetime.utcnow() - datetime.timedelta(days=5)
print(ahora)

ayer = ahora - datetime.timedelta(days=2)
print(ayer.date())

unixtime1 = time.mktime(ayer.date().timetuple())
#unixtime1 = time.mktime(ayer.timetuple())
print('uu')
print(unixtime1)


unixtime2 = time.mktime(ahora.date().timetuple())
#unixtime2 = time.mktime(ahora.timetuple())
print('uu2')
print(unixtime2)



import json
from dateutil.parser import parse
from pathlib import Path  # Python 3.6+ only
env_path = Path('.') / 'data.json'


def clean_text(text):
    text = re.sub(r'^RT[\s]+', '', text)
    text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r':', '', text)
    # text = re.sub(r':", "", text)
    text = re.sub(r'@[A-Za-z0-9]+', '', text)
    # print(text)
    text = text.lower().strip()
    text = text.translate(str.maketrans('', '', string.punctuation))
    return text

def formatear(strings):
    tildes = ['á', 'é', 'í', 'ó', 'ú']
    vocales = ['a', 'e', 'i', 'o', 'u']
    # tildes
    for idx, vocal in enumerate(vocales):
        strings = strings.str.replace(tildes[idx], vocal)
    # caracteres especiales menos la ñ
    strings = strings.str.replace('[^a-zñA-Z ]', "")
    # todo a minusculas
    strings = pd.Series(list(map(lambda x: x.lower(), strings)))
    return strings

def save_cursor(df,q,c):
    currDate = datetime.datetime.now()
    cursor = conn.cursor()
    Cliente=c
    try:
        conn.autocommit = False
        for index,row in df.iterrows(): 
    
            try:
                if q=='post':
                    query = "INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_post] (id,cod_postid,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,ARCHIVO,Flag_Postexterno,full_picture,reactions) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    print('ibsert post')
                    cursor.execute(query,row.cod_postid,row.cod_postid,Cliente,row.message,'','Facebook',row.status_type,0,row['from']['name'],'','','',row.permalink_url,0,row.Fecha,row.Hora,row['shares']['count'],'','','','','','','','','','','','',row['from']['id'],'','',currDate,row.likes['summary']['total_count'],'','','',row.id,'','','Facebook-Api','1',row.full_picture,row.reactions['summary']['total_count'])
                elif q=='comment' :
                    query = "INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_comments] (id,cod_comment_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,postid_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    cursor.execute(query,row.cod_comment_id,row.cod_comment_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','',row.permalink_url,0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,'','','',row.id,'','','Facebook-Api',row.cod_postid_comment)
                elif q=='reply':
                    query = "INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_reply_comments] (id,cod_reply_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,cod_comment_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    cursor.execute(query,row.cod_reply_id,row.cod_reply_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','','https://facebook.com' + str(row.id),0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,row.inreplyto,'','',row.id,'','','Facebook-Api',row.cod_commentid_replycomment)

            except Exception as e:
                print(e)

    except pyodbc.DatabaseError as err:
        conn.rollback()
    else:
        conn.commit()                
    finally:
        conn.autocommit = True
                      
    cursor.close()

class ApiFacebook:
    def __init__(self, cliente=None, access=None, cuenta=None):
        self.cliente = cliente
        self.access = access  
        self.cuenta = cuenta

    def get_inbox(self):
        Cliente = self.cliente
        access_token=self.access
        cuenta = self.cuenta

        base='https://graph.facebook.com/v8.0/'

        # url=base+cuenta+'/conversations?fields=link,messages{created_time,message,id}&access_token=' + str(access_token)

        url=base+cuenta+'/conversations?fields=message_count,updated_time,link,name,messages.limit(60){created_time,message,from,attachments},subject,participants&access_token='+ str(access_token)
        #print(url)
        request = requests.get(url).json()

        if 'data' in request:

            data = requests.get(url).json()
            inbox = []
            message = []       
            cursor = conn.cursor()
            try:
                conn.autocommit = False
                a = 1
                while(True):
                    try:
                        for datos in data['data']:                    
                            #if datos['id']=='t_10221571468668041':
                            part = []                           
                            for p in datos['participants']['data']:
                                if p['name'] != 'Profuturo AFP':
                                    print(p['name'])
                                    part.append({'name':p['name'],'email':p['email'],'id':p['id']})
                            

                            dt = parse(datos['updated_time']) - datetime.timedelta(hours=5)                   
                            try:
                                cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe_Facebook_inbox] (id,message_count,fecha,hora,link,participants_id,participants_name,participants_email) VALUES(?,?,?,?,?,?,?,?)",
                                    datos['id'],datos['message_count'],dt.date(),dt.time(),datos['link'],part[0]['id'],part[0]['name'],part[0]['email'])
                            except Exception as e:
                                print('se esta actualizando los mensajes')                            
                                cursor.execute("UPDATE [IPE.DW].[dbo].[Informe_Facebook_inbox] SET message_count = ?,fecha=?,hora=? WHERE id = ?", datos['message_count'],dt.date(),dt.time(),datos['id'])

                            i =0
                            for msg in datos['messages']['data']:
                                print(i) 
                                i=i+1;

                                multimedia = []
                                im=''

                                if 'attachments' in msg:    
                                    #print(msg['attachments'])
                                    for m in msg['attachments']['data']:   
                                        print(m)
                                        #print(m['image_data'])
                                        if 'image_data' in m:
                                            #print
                                            multimedia.append({'url':m['image_data']['url']})
                                        elif 'video_data' in m:
                                            multimedia.append({'url':m['video_data']['url']})


                                if multimedia==[]:
                                    im = ''  
                                else:
                                    im=multimedia[0]['url']
                                print(im)
                                dt_created = parse(msg['created_time']) - datetime.timedelta(hours=5)
                             
                                try:
                                    cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe_Facebook_message] (id,created_time,fecha,hora,attachments,message,from_id,from_name,from_email,inbox_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                                            msg['id'],msg['created_time'],dt_created.date(),dt_created.time(),im,msg['message'],msg['from']['id'],msg['from']['name'],msg['from']['email'],datos['id'])
                                except Exception as e:
                                    print('ya existe ese mensaje')
                                    print(e)
                        
                        time.sleep(1)
                        url = data['paging']['next'].encode('utf-8')              
                        
                        print(url)
                        data = requests.get(url).json()
                        a+=1
                    except KeyError:                  
                        break
            except pyodbc.DatabaseError as err:
                conn.rollback()
            else:
                conn.commit()
            finally:
                conn.autocommit = True        
            cursor.close()

            print('termino esta -------------> carga ')

    def get_procesar(self):
        Cliente = self.cliente
        access_token=self.access
        cuenta = self.cuenta

        base='https://graph.facebook.com/v8.0/'
        #access_token='EAAFLWQphDb4BAOfw4GP5Ol3ZCvL407twiKC5Edd4ugs8mhoM7kAv7Ma0HI10fSTCLrEZBEm1KHJZAvVfc6I8usXNqmhYKlcKMeBT7pEYXvtaGYgy7SYEYvNQO0svOIbZA9BgpOLK4p4iXfPRONiuyl2uF5SxxZCz9AoZBXKZACtuwZDZD'
        #url=base+cuenta+'conversations?fields=messages.limit(100){message,created_time,from}&limit=500&access_token=' + str(access_token)
        #url=base+cuenta+'?fields=posts{likes.summary(true),created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,message,comments{id,created_time,from,like_count,message,comments{id,from,created_time,like_count,message}}}&access_token=' + str(access_token)
        url=base+cuenta+'?fields=posts.since('+str(unixtime1)+').until('+str(unixtime2)+'){status_type,created_time,id,from,is_popular,is_published,picture,shares.limit(0).summary(true),story,story_tags,subscribed,permalink_url,message,comments.limit(0).summary(true),likes.limit(0).summary(true),full_picture,reactions.limit(0).summary(true)}&access_token=' + str(access_token)

        print('access_token')
        print(url)
        # return access_token
        request = requests.get(url).json()
        post_status=False
        if 'posts' in request:
            print('exists post')            
        
            data = requests.get(url).json()['posts']
            post = []
            comentarios = []

            a = 1
            while(True):
                try:
                    for datos in data['data']:
                        post.append(datos)
                        # Attempt to make a request to the next page of data, if it exists.
                    #print(data['conversations']['paging']['next'])
                    #time.sleep(1)

                    url = data['paging']['next'].encode('utf-8')
                    
                    print(url)
                    data = requests.get(url).json()
                    #print(data)
                    a+=1
                except KeyError:
                    # When there are no more pages (['paging']['next']), break from the
                    # loop and end the script.
                    break
                #print(a)

            df_facebok_post = pd.DataFrame(post)

            history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,ayer,ahora),conn)
            #                                    and Fecha Between " % Cliente,conn)
            #history_facebook_post.shape

            df_facebok_post['id'] = df_facebok_post.id.apply(lambda x: x if not pd.isnull(x) else 0)
            df_facebok_post['from'] = df_facebok_post['from'].apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['message'] = df_facebok_post.message.apply(lambda x: x if not pd.isnull(x) else '')
            #df_facebok_post['story'] = df_facebok_post.story.apply(lambda x: x if not pd.isnull(x) else '')
            #df_facebok_post['story_tags'] = df_facebok_post.story_tags.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['permalink_url'] = df_facebok_post.permalink_url.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['status_type'] = df_facebok_post.status_type.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['full_picture'] = df_facebok_post.full_picture.apply(lambda x: x if not pd.isnull(x) else '')

            print(df_facebok_post.shares)

            if not df_facebok_post.shares.empty:
                df_facebok_post['shares'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {'count':'0'})
            else:
                df_facebok_post['shares'] = '0'

            if not df_facebok_post.likes.empty:
                df_facebok_post['likes'] = df_facebok_post.likes.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})
            else:
                df_facebok_post['likes'] = '0'

            if not df_facebok_post.likes.empty:   
                df_facebok_post['reactions'] = df_facebok_post.reactions.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})
            else:
                df_facebok_post['reactions'] = '0'

            def cod_postid(id):
                subguion = '_' 
                index_guion = id.index(subguion)    
                longitud= len(id) 
                cod_postid=int(id[index_guion+1:longitud])
                return cod_postid

            def quitar_guion(fecha):
                fecha = str(fecha)
                fechanew = fecha.replace('-', '')
                return fechanew

            df_facebok_post['cod_postid'] = df_facebok_post['id'].apply(cod_postid)
            df_facebok_post['Fecha_resta'] = pd.to_datetime(df_facebok_post['created_time']) - datetime.timedelta(hours=5)
            df_facebok_post['Fecha'] = pd.to_datetime(df_facebok_post['Fecha_resta']).dt.date
            df_facebok_post['Fecha']= df_facebok_post['Fecha'].apply(quitar_guion)
            df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['Fecha_resta']).dt.time
            df_facebok_post['Hora'] = df_facebok_post['Hora'].apply(str)

            #df_facebok_post['z'] = pd.to_datetime(df_facebok_post['created_time'])
            #df_facebok_post['Hora'] = df_facebok_post['z'].dt.tz_convert('US/Central').dt.time
            #df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['Hora'], format='%H:%M:%S').dt.time
            #df_facebok_post['sa'].dt.tz_convert('US/Pacific')
            #df_facebok_post['sa'] = pd.to_datetime(df_facebok_post['sa'], format='%H:%M:%S')

            df_facebok_post.isnull().sum()
            df_facebok_post.dtypes
            #Sirve para insertar datos en la tabla FACEBOOK_POST
            currDate = datetime.datetime.now()

            #Insertar en Post
           
            save_cursor(df_facebok_post,q='post',c=Cliente)
            # cursor = conn.cursor()  
            # try:
            #     conn.autocommit = False
            #     for index,row in df_facebok_post.iterrows(): 
            #         #history_post_exist =  history_facebook_post[(history_facebook_post.cod_postid == row.cod_postid)]
            #         #if history_post_exist.empty:
            #         try:
            #             print(row.cod_postid)
            #             query_post = "INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_post] (id,cod_postid,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,ARCHIVO,Flag_Postexterno,full_picture,reactions) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row.cod_postid,row.cod_postid,Cliente,row.message,'','Facebook',row.status_type,0,row['from']['name'],'','','',row.permalink_url,0,row.Fecha,row.Hora,row['shares']['count'],'','','','','','','','','','','','',row['from']['id'],'','',currDate,row.likes['summary']['total_count'],'','','',row.id,'','','Facebook-Api','1',row.full_picture,row.reactions['summary']['total_count']
            #             cursor.execute(query_post)

            #         except Exception as e:
            #             print(e)
            #         #else:
            #         #    print(row.cod_postid)

            # except pyodbc.DatabaseError as err:
            #     conn.rollback()
            # else:
            #     conn.commit()
            #     post_status=True                
            # finally:
            #     conn.autocommit = True
                       
            # cursor.close()

            df_comments = pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message','from','url'])

            for posts in post:
                #print(posts['id'])    
                #url_comments=base+posts['id']+'?fields=comments{id,created_time,from,like_count,message}&access_token=' + str(access_token)
                #url_comments=base+posts['id']+'?fields=comments.limit(100000){id,created_time,like_count,message,from{name,id,username,link}}&access_token=' + str(access_token)
                #?fields=from{name,id,username,link}
                url_comments=base+posts['id']+'?fields=comments.limit(10000){id,created_time,like_count,message,from,permalink_url}&access_token=' + str(access_token)

                #graph.facebook.com/3771930046172217_3780537211978167?fields=from{name,id,username,link}

                comentarios =[]
                requests_comments = requests.get(url_comments).json()
                print(url_comments)

                if 'comments' in requests_comments:

                    try:
                        data2 = requests.get(url_comments).json()['comments']
                 
                    except KeyError:
                        continue
                    a = 1
                    while(True):
                        try:
                            for datos in data2['data']:
                                comentarios.append(datos)

                            # Attempt to make a request to the next page of data, if it exists.
                            #print(data['conversations']['paging']['next'])
                            #time.sleep(1)
                            url = data2['paging']['next'].encode('utf-8')
                            # print(url)

                            data2 = requests.get(url).json()
                            #print(data)
                            a+=1
                        except KeyError:
                            # When there are no more pages (['paging']['next']), break from the
                            # loop and end the script.
                            break
                        print(a)
                df_comments = pd.concat ([df_comments, pd.DataFrame(json_normalize(comentarios))]) 

            print('df_comments --->')
            print(df_comments)
            coment_status=False 
            if not df_comments.empty:
                #https://stackoverflow.com/questions/29152500/get-real-profile-url-from-facebook-graph-api-user
                df_comments['Nombre'] = df_comments['from.name']
                df_comments['UserID'] = df_comments['from.id']

                #history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
                history_facebook_comments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_comments] where Cliente = '%s' " % Cliente,conn)
                history_facebook_comments.shape

                def cod_comment_id(id):
                    subguion = '_' 
                    index_guion = id.index(subguion)    
                    longitud= len(id)  
                    cod_comment_id=int(id[index_guion+1:longitud])
                    return cod_comment_id

                def cod_postid_comment(id):
                    subguion = '_' 
                    index_guion = id.index(subguion)
                    cod_postid_comment=int(id[0:index_guion])
                    return cod_postid_comment

                df_comments['cod_comment_id'] = df_comments['id'].apply(cod_comment_id)
                df_comments['cod_postid_comment'] = df_comments['id'].apply(cod_postid_comment)
                df_comments['Fecha_resta'] = pd.to_datetime(df_comments['created_time']) - datetime.timedelta(hours=5)
                df_comments['Fecha'] = pd.to_datetime(df_comments['Fecha_resta']).dt.date
                df_comments['Fecha']= df_comments['Fecha'].apply(quitar_guion)
                df_comments['Hora'] = pd.to_datetime(df_comments['Fecha_resta']).dt.time
                df_comments['Hora'] = df_comments['Hora'].apply(str)

                #Insertando en facebook comments 
                # cursor = conn.cursor()
                # try:
                #     conn.autocommit = False
                #     for index,row in df_comments.iterrows():
                #         #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado

                #         try:
                #             print('vacio ' + str(row.cod_comment_id))
                #             cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_comments] (id,cod_comment_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,postid_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                #                 row.cod_comment_id,row.cod_comment_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','',row.permalink_url,0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,'','','',row.id,'','','Facebook-Api',row.cod_postid_comment)
                #         except Exception as e:
                #             print(e)

                # except pyodbc.DatabaseError as err:
                #     conn.rollback()
                # else:
                #     conn.commit()
                #     coment_status=True                    
                # finally:
                #     conn.autocommit = True
                # cursor.close()

                save_cursor(df_comments,q='comment',c=Cliente)

                print('termino comments')
                df_reply_comments= pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message'])

                for index, row in df_comments.iterrows(): 
                    url_comments=base+str(row.id)+'?fields=comments{id,created_time,like_count,message,from}&access_token=' + str(access_token)
                    comentarios =[]
                    print(url_comments)
                    try:
                        data3 = requests.get(url_comments).json()['comments']

                    except KeyError:
                        continue
                    a = 1
                    while(True):
                        try:
                            for datos in data3['data']:

                                datos['inreplyto'] = row.id
                                comentarios.append(datos)                

                            # Attempt to make a request to the next page of data, if it exists.
                            #print(data['conversations']['paging']['next'])
                            #time.sleep(1)
                            url = data3['paging']['next'].encode('utf-8')
                            data3 = requests.get(url).json()
                            a+=1
                        except KeyError:
                            # When there are no more pages (['paging']['next']), break from the
                            # loop and end the script.
                            break
                        print(a)

                    df_reply_comments = pd.concat ([df_reply_comments, pd.DataFrame(json_normalize(comentarios))])

                def cod_reply_id(id):    
                    subguion = '_' 
                    index_guion = id.index(subguion)    
                    longitud= len(id) 
                    cod_reply_id=int(id[index_guion+1:longitud])
                    return cod_reply_id

                def cod_commentid_replycomment(inreplyto):
                    subguion = '_' 
                    index_guion = inreplyto.index(subguion)    
                    longitud= len(inreplyto) 
                    cod_commentid_replycomment=int(inreplyto[index_guion+1:longitud])
                    print('----------------------------------->',cod_commentid_replycomment)
                    return cod_commentid_replycomment

                print('datos----',df_reply_comments)
                reply_status=False
                if not df_reply_comments.empty: 

                    df_reply_comments['cod_reply_id'] = df_reply_comments['id'].apply(cod_reply_id)
                    df_reply_comments['cod_commentid_replycomment'] = df_reply_comments['inreplyto'].apply(cod_commentid_replycomment)
                    df_reply_comments['Fecha_resta'] = pd.to_datetime(df_reply_comments['created_time']) - datetime.timedelta(hours=5)
                    df_reply_comments['Fecha'] = pd.to_datetime(df_reply_comments['Fecha_resta']).dt.date
                    df_reply_comments['Fecha']= df_reply_comments['Fecha'].apply(quitar_guion)
                    df_reply_comments['Hora'] = pd.to_datetime(df_reply_comments['Fecha_resta']).dt.time
                    df_reply_comments['Hora'] = df_reply_comments['Hora'].apply(str)

                    df_reply_comments['Nombre'] = df_reply_comments['from.name']
                    df_reply_comments['UserID'] = df_reply_comments['from.id']
                    #Insertando en facebook comments 
                    # cursor = conn.cursor()
                    # try:
                    #     conn.autocommit = False
                    #     for index,row in df_reply_comments.iterrows():
                    #             #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado
                    #         print(row.id)
                    #         try:
                    #             cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_reply_comments] (id,cod_reply_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,cod_comment_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    #                 row.cod_reply_id,row.cod_reply_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','','https://facebook.com' + str(row.id),0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,row.inreplyto,'','',row.id,'','','Facebook-Api',row.cod_commentid_replycomment)
                    #         except Exception as e:
                    #             print(e)

                    # except pyodbc.DatabaseError as err:
                    #     conn.rollback()
                    # else:
                    #     conn.commit()
                    #     reply_status=True                        
                    # finally:
                    #     conn.autocommit = True
                    # cursor.close()
                    save_cursor(df_reply_comments,q='reply',c=Cliente)
        print('termino' + Cliente)
        return str('termino  ' + Cliente)
        # print(post_status)
        # print(coment_status)
        # print(reply_status)
        # if post_status==True and coment_status==True and reply_status==True:
        #     status=True
        # else:
        #     status=False
        # return status

    def sentiment(self):
        hfacebook_comments =pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_Comments] WHERE  TextBlobSENTIMENT IS NULL and len(Mensaje)>3 ",conn)      

        print('Importando Jergas')
        #LLAMAMOS A JERGAS CSV
        df_jergas = pd.read_csv("jergas.csv",encoding = "utf-8",error_bad_lines=False,low_memory=False)

        #FUNCION PARA LIMPIAR TEXTO de # o RT @ #d = clean_text('hola # .haber  @jes a todos')
        def clean_text(text):
            text = re.sub(r'^RT[\s]+', '', text)
            text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
            text = re.sub(r'#', '', text)
            text = re.sub(r'@[A-Za-z0-9]+', '', text)
            text = text.lower().strip()
            text = text.translate(str.maketrans('','',string.punctuation))
            return text

        #FUNCION PARA CATEGORIZAR EL SENTIMIENTO DE ACUERDO A LA POLARIDAD DEL TEXTO
        def x_range(x):
            if x > 0:
                return 'Positivo'
            elif x == 0:
                return 'Neutro'
            else:
                return 'Negativo'

        #REEMPLAZANDO UNA JERGA POR SU EQUIVALENTE EN ESPANIOL
        def reemplazar(text):
            for index, row in df_jergas.iterrows():
                text = re.sub(row.palabras, row.significado, text)
            return text


        #LIMPIAMOS Y VERIFICAMOS QUE EL TEXTO TENGA MAS DE 3 CARACTERES, POR QUE HAY COMENTARIOS CON IMAGEN O GIFS
        pd.options.mode.chained_assignment = None  # default='warn'
        print('clean')
        datosfilter=hfacebook_comments
        datosfilter['clean_texto'] = datosfilter['Mensaje'].apply(str)
        datosfilter['clean_texto'] = formatear(datosfilter['clean_texto'])
        datosfilter =datosfilter[datosfilter['clean_texto'].str.len()>3]

        #REEMPLAZAMOS EL TEXTO
        datosfilter['clean_texto'] = datosfilter['clean_texto'].apply(reemplazar)
        print('iniciando la traduccion de comments')

        #FUNCION DE POLARIDAD Y TRADUCTOR
        def get_polarity(text):
            analysis = TextBlob(text)
            if text != '':
                if analysis.detect_language() == 'es':
                    try:
                        result = analysis.translate(from_lang = 'es', to = 'en').sentiment.polarity
                            #result = analysis.sentiment.polarity
                        print(result)
                    except:
                        print("An exception occurred")          
                        result = 0
                    time.sleep(1)
                    return result

        print('traduciendo y extraendo polaridad de comments')
        #APLICAMOS POLARIDAD 
        datosfilter['polarity'] = datosfilter['clean_texto'].apply(get_polarity)      

        print('aplicando rangos')
        #APLICAMOS LOS RANGOS 
        datosfilter['result'] = datosfilter['polarity'].apply(x_range)
        datosfilter = datosfilter.sort_values(by=['result'])

        #RESULTADOS CONTEO
        datosfilter['result'].value_counts()
        datosfilter =datosfilter[['cod_comment_id','Mensaje','clean_texto','result']]
        print('actualizando tabla coments campo TextblobSENTIMENT')

        # -*- coding: 850 -*-
        cursor = conn.cursor()
        for index, row in datosfilter.iterrows():
            print(row.result)
            cursor.execute("UPDATE [IPE.DW].[dbo].[Informe.Facebook_comments] SET TextBlobSENTIMENT = ? WHERE cod_comment_id = ?", row.result, row.cod_comment_id)
            cursor.commit()
        cursor.close()

        print('extraendo sentimiento de reply comments')
        #CONSULTAMOS CON UN INNERJ JOIN TODOS LOS COMENTARIOS CON SUS RESPECTIVOS POST'S
        replycomments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_reply_comments]  where  TextBlobSENTIMENT IS NULL ",conn)

        print('reply comments polaridad y traduccion')

        replycomments['clean_texto'] = replycomments['Mensaje'].apply(str)
        replycomments['clean_texto'] = formatear(replycomments['clean_texto'])
        replycomments =replycomments[replycomments['clean_texto'].str.len()>3]
        replycomments['clean_texto'] = replycomments['clean_texto'].apply(reemplazar)

        print('APLICAMOS POLARIDAD ')
        #APLICAMOS POLARIDAD 
        replycomments['polarity'] = replycomments['clean_texto'].apply(get_polarity)
  

        print('APLICAMOS LOS RANGOS ')
        #APLICAMOS LOS RANGOS 
        replycomments['result'] = replycomments['polarity'].apply(x_range)
        replycomments = replycomments.sort_values(by=['result'])

        #RESULTADOS CONTEO
        replycomments['result'].value_counts()
        replycomments =replycomments[['cod_reply_id','Mensaje','clean_texto','result']]

        #GUARDAMOS EN UN CSV DE SER NECESARIO
        #replycomments.to_excel('textblob_reply.xlsx')
        print('actualizando tabla reply coments campo NTLKSENTIMENT')

        cursor = conn.cursor()
        for index, row in replycomments.iterrows():
            print(row.result)
            cursor.execute("UPDATE [IPE.DW].[dbo].[Informe.Facebook_reply_comments] SET TextBlobSENTIMENT = ? WHERE cod_reply_id = ?", row.result, row.cod_reply_id)
            cursor.commit()
        cursor.close()

        print('consultando tabla sentiment POST')   


        #CONSULTAMOS CON UN INNERJ JOIN TODOS LOS COMENTARIOS CON SUS RESPECTIVOS POST'S
        post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where TextBlobSENTIMENT IS NULL ",conn)
    
        post['clean_texto'] = post['Mensaje'].apply(str)
        post['clean_texto'] = formatear(post['clean_texto'])
        post =post[post['clean_texto'].str.len()>3]
        post['clean_texto'] = post['clean_texto'].apply(reemplazar)

        print('APLICAMOS POLARIDAD Y TRADUCCION ')
        #APLICAMOS POLARIDAD 
        post['polarity'] = post['clean_texto'].apply(get_polarity)

        #APLICAMOS LOS RANGOS 
        post['result'] = post['polarity'].apply(x_range)

        post = post.sort_values(by=['result'])

        #RESULTADOS CONTEO
        post['result'].value_counts()
        post =post[['cod_postid','Mensaje','clean_texto','result']]

        print('actualizamos la tabla post campo ntlksentiment')

        cursor = conn.cursor()
        for index, row in post.iterrows():
            print(row.result)
            cursor.execute("UPDATE [IPE.DW].[dbo].[Informe.Facebook_post] SET TextBlobSENTIMENT = ? WHERE cod_postid = ?", row.result, row.cod_postid)
            cursor.commit()
        cursor.close()

        print('Actualizaciones realizadas ... carga terminada')


    def stop_words(self):

        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        import string
        from collections import Counter
        from _collections import OrderedDict

        xdata = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_comments] ", conn)

        xdata['Mensaje'] = xdata['Mensaje'].apply(str)
        xdata['Mensaje_nuevo'] = xdata['Mensaje'].apply(clean_text)
        xdata = xdata[xdata['Mensaje_nuevo'].str.len() > 3]
        xdata = xdata[['Mensaje', 'cod_comment_id', 'Mensaje_nuevo']]


        def words_frecuency(texto):
            kv = []
            stop_words = set(stopwords.words('spanish'))
            word_tokens = word_tokenize(texto)

            word_tokens = list(filter(lambda token: token not in string.punctuation, word_tokens))
            filtro = ""

            for palabra in word_tokens:
                if palabra not in stop_words:
                    filtro = filtro + " " + palabra

            c = Counter(filtro)
            cc = c.most_common(6)
            y = OrderedDict(cc)
            for k, v in y.items():
                kv.append({k, v})
            return filtro

        xdata['filtro_stopword'] = xdata['Mensaje_nuevo'].apply(words_frecuency)

        def new_format(texto):
            sum = ""
            for t in texto:
                sum = sum + " " + t
            # print(sum)
            filtro = ""

            stop_words_es = np.genfromtxt('stop_words_es.txt', dtype='str')
            stop_words_es = formatear(pd.Series(stop_words_es))

            stop_words_es = set(stop_words_es)

            word_tokens = word_tokenize(texto)

            for palabra in word_tokens:
                if palabra not in stop_words_es:
                    filtro = filtro + " " + palabra

            return filtro

        xdata['filtro_second'] = xdata['filtro_stopword'].apply(new_format)

        cursor = conn.cursor()
        for index, row in xdata.iterrows():
            print(row.cod_comment_id)
            cursor.execute(
                "UPDATE [IPE.DW].[dbo].[Informe.Facebook_comments] SET Mensaje_clean = ? WHERE cod_comment_id = ?",
                row.filtro_second, row.cod_comment_id)
            cursor.commit()
        cursor.close()

