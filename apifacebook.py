#!/usr/bin/env python
# coding: utf-8

# In[9]:


#estoy en develop
#import boto3 
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
import pyodbc
import numpy as np
import time
import datetime


# In[10]:


pd.options.mode.chained_assignment = None

#conn = pyodbc.connect(	
#	'Driver={SQL Server Native Client 11.0};'
#	                      'Server=localhost;'
#	                      'Database=IPE.DW;'
#	                      'Trusted_Connection=yes;'
#)


# In[11]:


pd.options.mode.chained_assignment = None

conn = pyodbc.connect(	
	'Driver={SQL Server Native Client 11.0};'
	                      'Server=localhost;'
	                      'Database=IPE.DW;'
	                      'Trusted_Connection=no;'
	                      'uid=UserData;'
                          'pwd=1P32020')


# In[12]:





# In[84]:


ahora = datetime.datetime.utcnow() + datetime.timedelta(days=2)
print(ahora)

ayer = ahora - datetime.timedelta(days=20)
print(ayer.date())

unixtime1 = time.mktime(ayer.date().timetuple())
print(unixtime1)


unixtime2 = time.mktime(ahora.date().timetuple())
print(unixtime2)


# In[52]:





# In[45]:


class ApiFacebook:
    def __init__(self, cliente, access, cuenta):
        self.cliente = cliente
        self.access = access  
        self.cuenta = cuenta
    
    def get_procesar(self):

        Cliente = self.cliente
        access_token=self.access
        cuenta = self.cuenta

        base='https://graph.facebook.com/v8.0/'
        #access_token='EAAFLWQphDb4BAOfw4GP5Ol3ZCvL407twiKC5Edd4ugs8mhoM7kAv7Ma0HI10fSTCLrEZBEm1KHJZAvVfc6I8usXNqmhYKlcKMeBT7pEYXvtaGYgy7SYEYvNQO0svOIbZA9BgpOLK4p4iXfPRONiuyl2uF5SxxZCz9AoZBXKZACtuwZDZD'

        #url=base+cuenta+'conversations?fields=messages.limit(100){message,created_time,from}&limit=500&access_token=' + str(access_token)
        #url=base+cuenta+'?fields=posts{likes.summary(true),created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,message,comments{id,created_time,from,like_count,message,comments{id,from,created_time,like_count,message}}}&access_token=' + str(access_token)
        url=base+cuenta+'?fields=posts.since('+str(unixtime1)+').until('+str(unixtime2)+'){status_type,created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,permalink_url,message,comments.limit(0).summary(true),likes.limit(0).summary(true)}&access_token=' + str(access_token)
        print(url)
        data = requests.get(url).json()['posts']
        #print(data)
        post = []
        comentarios = []

        a = 1
        while(True):
            try:
                for datos in data['data']:
                    post.append(datos)
                    print(datos)
                    # Attempt to make a request to the next page of data, if it exists.
                #print(data['conversations']['paging']['next'])
                #time.sleep(1)

                url = data['paging']['next'].encode('utf-8')
                print('url')
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
        #df_facebok_post
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
        df_facebok_post['shares'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {'count':'0'})
        df_facebok_post['likes'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})

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
        df_facebok_post['Fecha'] = pd.to_datetime(df_facebok_post['created_time']).dt.date
        df_facebok_post['Fecha']= df_facebok_post['Fecha'].apply(quitar_guion)
        df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['created_time']).dt.time
        df_facebok_post
        df_facebok_post.isnull().sum()
        df_facebok_post.dtypes
        #Sirve para insertar datos en la tabla FACEBOOK_POST
        currDate = datetime.datetime.now()

        #Insertar en Post
        cursor = conn.cursor()  
        try:
            conn.autocommit = False
            for index,row in df_facebok_post.iterrows(): 
                history_post_exist =  history_facebook_post[(history_facebook_post.cod_postid == row.cod_postid)]

                if history_post_exist.empty:
                    try:
                        print(row.cod_postid)
                        cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_post] (id,cod_postid,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,ARCHIVO,Flag_Postexterno) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           row.cod_postid,row.cod_postid,Cliente,row.message,'','Facebook',row.status_type,0,row['from']['name'],'','','',row.permalink_url,0,row.Fecha,row.Hora,row['shares']['count'],'','','','','','','','','','','','',row['from']['id'],'','',currDate,row['likes']['count'],'','','',row.id,'','','Facebook-Api','1')

                    except Exception as e:
                        print('e')
                        print(e)
                else:
                    print(row.cod_postid)

        except pyodbc.DatabaseError as err:
            conn.rollback()
        else:
            conn.commit()
        finally:
            conn.autocommit = True        
        cursor.close()

        df_comments = pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message','from','url'])

        for posts in post:
            #print(posts['id'])    
            #url_comments=base+posts['id']+'?fields=comments{id,created_time,from,like_count,message}&access_token=' + str(access_token)
            #url_comments=base+posts['id']+'?fields=comments.limit(100000){id,created_time,like_count,message,from{name,id,username,link}}&access_token=' + str(access_token)
            #?fields=from{name,id,username,link}
            url_comments=base+posts['id']+'?fields=comments.limit(10000){id,created_time,like_count,message,from,permalink_url}&access_token=' + str(access_token)

            #graph.facebook.com/3771930046172217_3780537211978167?fields=from{name,id,username,link}

            comentarios =[]

            print(url_comments)
            try:
                data2 = requests.get(url_comments).json()['comments']
                #print(data2)
            except KeyError:
                continue
            a = 1
            while(True):
                try:
                    for datos in data2['data']:
                        comentarios.append(datos)
                    #print(comentarios)
                    #print(Conversations)
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

        df_comments
        #https://stackoverflow.com/questions/29152500/get-real-profile-url-from-facebook-graph-api-user
        df_comments['Nombre'] = df_comments['from.name']
        df_comments['UserID'] = df_comments['from.id']
        df_comments

        #history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
        history_facebook_comments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_comments] where Cliente = '%s' " % Cliente,conn)
        history_facebook_comments.shape

        def cod_comment_id(id):
            subguion = '_' 
            index_guion = id.index(subguion)    
            longitud= len(id)
            #print(id)    
            cod_comment_id=int(id[index_guion+1:longitud])
            return cod_comment_id

        def cod_postid_comment(id):
            subguion = '_' 
            index_guion = id.index(subguion)    
            #longitud= len(id)
            #print(id)    
            cod_postid_comment=int(id[0:index_guion])
            return cod_postid_comment

        df_comments['cod_comment_id'] = df_comments['id'].apply(cod_comment_id)
        df_comments['cod_postid_comment'] = df_comments['id'].apply(cod_postid_comment)
        df_comments['Fecha'] = pd.to_datetime(df_comments['created_time']).dt.date
        df_comments['Fecha']= df_comments['Fecha'].apply(quitar_guion)
        df_comments['Hora'] = pd.to_datetime(df_comments['created_time']).dt.time

        df_comments

        #Insertando en facebook comments 
        cursor = conn.cursor()
        try:
            conn.autocommit = False
            for index,row in df_comments.iterrows():
                #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado

                try:
                    print('vacio ' + str(row.cod_comment_id))
                    cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_comments] (id,cod_comment_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,postid_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        row.cod_comment_id,row.cod_comment_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','',row.permalink_url,0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,'','','',row.id,'','','Facebook-Api',row.cod_postid_comment)
                except Exception as e:
                    print(e)

        except pyodbc.DatabaseError as err:
            conn.rollback()
        else:
            conn.commit()
        finally:
            conn.autocommit = True
        cursor.close()

        df_reply_comments= pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message'])
        df_reply_comments

        for index, row in df_comments.iterrows(): 
            url_comments=base+str(row.id)+'?fields=comments{id,created_time,like_count,message,from}&access_token=' + str(access_token)
            comentarios =[]
            #print(url_comments)
            try:
                data3 = requests.get(url_comments).json()['comments']
                #print(requests.get(url_comments).json())
            except KeyError:
                continue
            a = 1
            while(True):
                try:
                    for datos in data3['data']:
                        #print(datos)
                        datos['inreplyto'] = row.id
                        comentarios.append(datos)                
                        #print(comentarios)
                    #print(Conversations)
                    # Attempt to make a request to the next page of data, if it exists.
                    #print(data['conversations']['paging']['next'])
                    #time.sleep(1)

                    url = data3['paging']['next'].encode('utf-8')
                # print(url)

                    data3 = requests.get(url).json()
                    #print(data)
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
            return cod_commentid_replycomment

        df_reply_comments['cod_reply_id'] = df_reply_comments['id'].apply(cod_reply_id)
        df_reply_comments['cod_commentid_replycomment'] = df_reply_comments['inreplyto'].apply(cod_commentid_replycomment)
        df_reply_comments['Fecha'] = pd.to_datetime(df_reply_comments['created_time']).dt.date
        df_reply_comments['Fecha']= df_reply_comments['Fecha'].apply(quitar_guion)
        df_reply_comments['Hora'] = pd.to_datetime(df_reply_comments['created_time']).dt.time
        df_reply_comments['Nombre'] = df_reply_comments['from.name']
        df_reply_comments['UserID'] = df_reply_comments['from.id']
        df_reply_comments

        #Insertando en facebook comments 
        cursor = conn.cursor()
        try:
            conn.autocommit = False
            for index,row in df_reply_comments.iterrows():
                #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado
                print(row.id)
                try:
                    cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_reply_comments] (id,cod_reply_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,cod_comment_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        row.cod_reply_id,row.cod_reply_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','','https://facebook.com' + str(row.id),0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,row.inreplyto,'','',row.id,'','','Facebook-Api',row.cod_commentid_replycomment)
                except Exception as e:
                        #print('e')
                    print(e)

        except pyodbc.DatabaseError as err:
            conn.rollback()
        else:
            conn.commit()
        finally:
            conn.autocommit = True
        cursor.close()


# In[46]:


#url_comments=base+post[1]['id']+'?fields=comments{id,created_time,from,like_count,message}&access_token=' + str(access_token)
#print(url_comments)


# In[ ]:


#pd.read_sql("SELECT * FROM %s" % table_name, conn)
#created_time	picture	shares	subscribed	message	comments	likes	story	story_tags


# In[47]:


#history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
#history_reply_comments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_reply_comments] where Cliente = '%s' " % Cliente,conn)


# In[ ]:


#Cliente= input('Escriba el cliente Comex o Saca tu cuenta o Piensape')
#if Cliente=='Comex':
#    print('comex')
#    access_token='EAAFLWQphDb4BACPjUReLZB3TtW6Lyz41v3BArzZBNbj2rmzWODiiIAiRar53ljgFdP5Qe0N7aq6xwQZCnyZAt3saj61FrtDOmnP2C6SKc25zm9QlXqhhovzqu2sZCyGaSxrzdqTVcW5s9pTQnmbPmEWO37G3gf0MZByNbASerLqwZDZD'
#    cuenta='141796229342716'
#elif Cliente=='Saca tu cuenta':
#    print('saca')
#    access_token='EAAFLWQphDb4BAHRDWqZBOxUrojxcZAfHZBPIJMz9n6ddqj580c0ORh8q9aNBijajL9tmVoITQ6qXiyawZC36z9InUDeuZCq1KMkhb4HeOORIe3hnDMwfN4ZCBz1Jzq7EMQtaXqyvwZAM5XED8AHAziUAU4f1eTMyXu63t94LJg0uwZDZD'
#    cuenta='100330785132565'
#elif Cliente=='Piensape':
#    print('piensape')
#    access_token='EAAFLWQphDb4BAAZCOP6T4PeL4mbYgjxfCwO3Vy9lNLExvOXulgsZB1Oh2i56JUEslp3i6bYAMgyHY57bZCq7oFIsdUsA1ObkZBXb6cbpy9C9NZCbl0GmoaglSzwqKcrhP5U896tQEZBDdflKH4J5G8R0eqzqxBnUjmw6EoQ0qiswZDZD'
#    cuenta='101816778261420'
#else:
#    print('Escribe Comex, Piensape o Saca tu cuenta correctamente')
#    exit()

#Fecha_ini = input("Escribe la fecha de inicio")
#Fecha_fin = input("Escribe la Fecha Fin")
#dateTime = datetime.date(2020,10,27)
#import dateutil.parser
#dateTime_ini = datetime.date(2020,11,1)
#dateTime_fin = datetime.date(2021,12,31)

#today = date.today()
#datetime.date.today()
#print(dateTime_ini)

# dd/mm/YY
#ahora = today.strftime("%Y-%m-%d")
#print("d1 =", ahora)

