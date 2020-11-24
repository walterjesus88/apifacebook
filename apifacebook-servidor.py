#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


#pd.options.mode.chained_assignment = None

#conn = pyodbc.connect(	
#	'Driver={SQL Server Native Client 11.0};'
#	                      'Server=localhost;'
#	                      'Database=IPE.DW;'
#	                      'Trusted_Connection=yes;'
#)


# In[3]:


pd.options.mode.chained_assignment = None

conn = pyodbc.connect(	
	'Driver={SQL Server Native Client 11.0};'
	                      'Server=119.8.153.140;'
	                      'Database=IPE.DW;'
	                      'Trusted_Connection=no;'
	                      'uid=UserData;'
                          'pwd=1P32020')


# In[4]:


#import requests
#def get_fbid(fb_url):
#    URL = "https://findmyfbid.com/"
#    PARAMS = {'url': fb_url}
#    try:
#        r = requests.post(url = URL, params= PARAMS)
#        return r.json().get("id")
#    except Exception:
#        return 0


# In[5]:


#ss =get_fbid('https://www.facebook.com/walterjesus88/')
#print(ss)


# In[6]:


#import requests

#USER_URL = 'https://www.facebook.com/walterjesus88/' # your link
#ACCESS_TOKEN = 'EAAFLWQphDb4BAPfb93PPPhgIlOHoHWZCRMJm4yoFW5EHbFCUeGEM4gBImdE96YysG10uZB1RmFi9x92gcZCFYBjQGsLmtioo8euQm2aFFZCZAyxC9vlAG7BblXFjgAvny0ZA35UfnoefOyeHWumZBzMh7X1KjZAvBAT6TsA8ZAWBjXQZCtJ6fBYB5oHA7ZBHH8w2p83kiVGRemNZAuc244AYQhKC' # your credentials
#params= {'id': USER_URL,'access_token': ACCESS_TOKEN}
#fb_graph = "https://graph.facebook.com/v8/"

#r = requests.get(fb_graph, params=params)
#fb_id = r.json()

#print('Facebook User ID: %s' % fb_id)


# In[7]:


import time
import datetime

#Fecha_ini = input("Escribe la fecha de inicio")
#Fecha_fin = input("Escribe la Fecha Fin")
#dateTime = datetime.date(2020,10,27)
dateTime_ini = datetime.date(2020,1,1)
dateTime_fin = datetime.date(2020,12,31)
print(dateTime_ini)
unixtime1 = time.mktime(dateTime_ini.timetuple())
print(unixtime1)


unixtime2 = time.mktime(dateTime_fin.timetuple())
print(unixtime2)


#int(dateTime.strftime("%s"))  2020-09-22T14:50:52+0000  2020-09-17


# In[8]:


Cliente= input('Escriba el cliente Comex o Saca tu cuenta o Piensape: ')
if Cliente=='Comex':
    print('comex')
    access_token='EAAFLWQphDb4BACPjUReLZB3TtW6Lyz41v3BArzZBNbj2rmzWODiiIAiRar53ljgFdP5Qe0N7aq6xwQZCnyZAt3saj61FrtDOmnP2C6SKc25zm9QlXqhhovzqu2sZCyGaSxrzdqTVcW5s9pTQnmbPmEWO37G3gf0MZByNbASerLqwZDZD'
    cuenta='141796229342716'
elif Cliente=='Saca tu cuenta':
    print('saca')
    access_token='EAAFLWQphDb4BAHRDWqZBOxUrojxcZAfHZBPIJMz9n6ddqj580c0ORh8q9aNBijajL9tmVoITQ6qXiyawZC36z9InUDeuZCq1KMkhb4HeOORIe3hnDMwfN4ZCBz1Jzq7EMQtaXqyvwZAM5XED8AHAziUAU4f1eTMyXu63t94LJg0uwZDZD'
    cuenta='100330785132565'
elif Cliente=='Piensape':
    print('piensape')
    access_token='EAAFLWQphDb4BAAZCOP6T4PeL4mbYgjxfCwO3Vy9lNLExvOXulgsZB1Oh2i56JUEslp3i6bYAMgyHY57bZCq7oFIsdUsA1ObkZBXb6cbpy9C9NZCbl0GmoaglSzwqKcrhP5U896tQEZBDdflKH4J5G8R0eqzqxBnUjmw6EoQ0qiswZDZD'
    cuenta='101816778261420'
else:
    print('Escribe Comex, Piensape o Saca tu cuenta correctamente')
    exit()


# In[9]:


base='https://graph.facebook.com/v8.0/'
#access_token='EAAFLWQphDb4BAOfw4GP5Ol3ZCvL407twiKC5Edd4ugs8mhoM7kAv7Ma0HI10fSTCLrEZBEm1KHJZAvVfc6I8usXNqmhYKlcKMeBT7pEYXvtaGYgy7SYEYvNQO0svOIbZA9BgpOLK4p4iXfPRONiuyl2uF5SxxZCz9AoZBXKZACtuwZDZD'
access_token=access_token
#url=base+cuenta+'conversations?fields=messages.limit(100){message,created_time,from}&limit=500&access_token=' + str(access_token)
#url=base+cuenta+'?fields=posts{likes.summary(true),created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,message,comments{id,created_time,from,like_count,message,comments{id,from,created_time,like_count,message}}}&access_token=' + str(access_token)
url=base+cuenta+'?fields=posts.since(1577854800.0).until(1606712400.0){status_type,created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,permalink_url,message,comments.limit(0).summary(true),likes.limit(0).summary(true)}&access_token=' + str(access_token)
print(url)


# In[138]:


data = requests.get(url).json()['posts']
#print(data)


# In[ ]:





# In[ ]:





# In[139]:


#df_post = pd.DataFrame(columns=['created_time','id','from','is_popular','is_published','picture','shares','story','story_tags','subscribed','message','comments','summary','likes','summary'])


# In[140]:


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


# In[141]:


df_facebok_post = pd.DataFrame(post)
df_facebok_post


# In[142]:


#print (post) trae todos los datos desde inicio y fin de fecha


# In[143]:


#df_facebok_post.to_csv("post_comex.csv")


# In[144]:


#Cliente='Comex'


# In[145]:


#dateTime_ini = '2020-08-01'
#dateTime_fin = '2020-11-11'


# In[146]:


history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
#                                    and Fecha Between " % Cliente,conn)
history_facebook_post.shape
#pd.read_sql("SELECT * FROM %s" % table_name, conn)


# In[147]:


df_facebok_post.head()


# In[148]:


#df_facebok_post['from'][0]['name']


# In[149]:


#created_time	picture	shares	subscribed	message	comments	likes	story	story_tags

df_facebok_post['id'] = df_facebok_post.id.apply(lambda x: x if not pd.isnull(x) else 0)
df_facebok_post['from'] = df_facebok_post['from'].apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['message'] = df_facebok_post.message.apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['story'] = df_facebok_post.story.apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['story_tags'] = df_facebok_post.story_tags.apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['permalink_url'] = df_facebok_post.permalink_url.apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['status_type'] = df_facebok_post.status_type.apply(lambda x: x if not pd.isnull(x) else '')
df_facebok_post['shares'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {'count':'0'})
df_facebok_post['likes'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})
                                                  


# In[150]:


def cod_postid(id):
    subguion = '_' 
    index_guion = id.index(subguion)    
    longitud= len(id) 
    cod_postid=int(id[index_guion+1:longitud])
    return cod_postid
    


# In[151]:


def quitar_guion(fecha):
    fecha = str(fecha)
    fechanew = fecha.replace('-', '')
    return fechanew


# In[152]:


df_facebok_post['cod_postid'] = df_facebok_post['id'].apply(cod_postid)
#df_facebok_post['Nombre'] = df_facebok_post['from'][0]['name']
#df_facebok_post['UserID'] = df_facebok_post['from'][0]['id']
#df_facebok_post['FBShares'] = df_facebok_post['from'][0]['id']
df_facebok_post['Fecha'] = pd.to_datetime(df_facebok_post['created_time']).dt.date
df_facebok_post['Fecha']= df_facebok_post['Fecha'].apply(quitar_guion)
df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['created_time']).dt.time
df_facebok_post
#dates = pd.to_datetime(pd.Series(['20010101', '20010331']), format = '%Y%m%d')


# In[ ]:





# In[153]:


df_facebok_post.isnull().sum()


# In[154]:


df_facebok_post.dtypes


# In[155]:


#Sirve para insertar datos en la tabla FACEBOOK_POST
currDate = datetime.datetime.now()
 
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


# In[156]:


url_comments=base+post[1]['id']+'?fields=comments{id,created_time,from,like_count,message}&access_token=' + str(access_token)
print(url_comments)


# In[157]:


df_comments = pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message','from','url'])


# In[158]:


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
  


# In[159]:


df_comments
#https://stackoverflow.com/questions/29152500/get-real-profile-url-from-facebook-graph-api-user


# In[160]:


df_comments['Nombre'] = df_comments['from.name']
df_comments['UserID'] = df_comments['from.id']
df_comments


# In[161]:


#Cliente='Comex'
#history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
history_facebook_comments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_comments] where Cliente = '%s' " % Cliente,conn)
history_facebook_comments.shape
#pd.read_sql("SELECT * FROM %s" % table_name, conn


# In[162]:


def cod_comment_id(id):
    subguion = '_' 
    index_guion = id.index(subguion)    
    longitud= len(id)
    #print(id)    
    cod_comment_id=int(id[index_guion+1:longitud])
    return cod_comment_id


# In[163]:


def cod_postid_comment(id):
    subguion = '_' 
    index_guion = id.index(subguion)    
    #longitud= len(id)
    #print(id)    
    cod_postid_comment=int(id[0:index_guion])
    return cod_postid_comment


# In[164]:


df_comments['cod_comment_id'] = df_comments['id'].apply(cod_comment_id)
df_comments['cod_postid_comment'] = df_comments['id'].apply(cod_postid_comment)
df_comments['Fecha'] = pd.to_datetime(df_comments['created_time']).dt.date
df_comments['Fecha']= df_comments['Fecha'].apply(quitar_guion)
df_comments['Hora'] = pd.to_datetime(df_comments['created_time']).dt.time

df_comments


# In[165]:






#Insertando en facebook comments 
cursor = conn.cursor()
try:
    conn.autocommit = False
    for index,row in df_comments.iterrows():
        #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado

        history_comment_exist =  history_facebook_comments[(history_facebook_comments.postid == row.id)]
        #if history_comment_exist.empty:
        try:
            print('vacio ' + str(row.cod_comment_id))
            cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_comments] (id,cod_comment_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,postid_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row.cod_comment_id,row.cod_comment_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','',row.permalink_url,0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,'','','',row.id,'','','Facebook-Api',row.cod_postid_comment)
                #index,row.cod_postid,'comexprueba',row.message,'pruebaapi','Facebook',row.status_type,0,row['from']['name'],'','','',row.permalink_url,0,row.created_time,row.created_time,'0','','','','','','infl','','','','','','',row['from']['id'],'','',currDate,row['likes']['count'],'','','',row.id,'','','Facebook-Api','1')
        except Exception as e:
            print(e)
        #else:
        #    print('update')
            #cursor.execute("UPDATE [IPE.DW].[dbo].[Informe.Facebook_comments] SET UserID = ?,Nombre = ? WHERE cod_comment_id = ?", row.UserID,row.Nombre,cod_comment_id)

except pyodbc.DatabaseError as err:
    conn.rollback()
else:
    conn.commit()
finally:
    conn.autocommit = True
cursor.close()


# In[166]:


df_reply_comments= pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message'])
df_reply_comments


# In[167]:


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
    #inreplyto = []
    #inreplyto.append(row.id)
    #serie_comment_id = pd.Series(inreplyto)
    #df_reply_comments['inreplyto']=serie_comment_id
    
    print(df_reply_comments)


# In[168]:


xc =df_reply_comments[(df_reply_comments.id=='1465732346949091_1465943833594609')]
xc


# In[169]:


#Cliente='Comex'
#history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
history_reply_comments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_reply_comments] where Cliente = '%s' " % Cliente,conn)
history_reply_comments.shape
#pd.read_sql("SELECT * FROM %s" % table_name, conn


# In[170]:


history_reply_comments.head(10000)


# In[171]:


#history_reply_comments.to_csv('cmm.csv')


# In[172]:


history_reply_e=history_reply_comments[(history_reply_comments.postid == '1465732346949091_1465943833594609')]
history_reply_e


# In[173]:


def cod_reply_id(id):    
    subguion = '_' 
    index_guion = id.index(subguion)    
    longitud= len(id) 
    cod_reply_id=int(id[index_guion+1:longitud])
    return cod_reply_id


# In[174]:


def cod_commentid_replycomment(inreplyto):
    subguion = '_' 
    index_guion = inreplyto.index(subguion)    
    longitud= len(inreplyto) 
    cod_commentid_replycomment=int(inreplyto[index_guion+1:longitud])
    return cod_commentid_replycomment


# In[175]:


df_reply_comments['cod_reply_id'] = df_reply_comments['id'].apply(cod_reply_id)
df_reply_comments['cod_commentid_replycomment'] = df_reply_comments['inreplyto'].apply(cod_commentid_replycomment)
df_reply_comments['Fecha'] = pd.to_datetime(df_reply_comments['created_time']).dt.date
df_reply_comments['Fecha']= df_reply_comments['Fecha'].apply(quitar_guion)
df_reply_comments['Hora'] = pd.to_datetime(df_reply_comments['created_time']).dt.time
df_reply_comments['Nombre'] = df_reply_comments['from.name']
df_reply_comments['UserID'] = df_reply_comments['from.id']
df_reply_comments


# In[176]:


xc =df_reply_comments[(df_reply_comments.id=='1465732346949091_1465943833594609')]
xc


# In[178]:


#Insertando en facebook comments 
cursor = conn.cursor()
try:
    conn.autocommit = False
    for index,row in df_reply_comments.iterrows():
        #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado
        print(row.id)

        #history_reply_comments_exist =  history_reply_comments[(history_reply_comments.postid == row.id)]
        #print(history_reply_comments)
        #if history_reply_comments_exist.empty:
        #    print('vacio ' +str(row.id))
        try:
            cursor.execute("INSERT INTO [IPE.DW].[dbo].[Informe.Facebook_reply_comments] (id,cod_reply_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,cod_comment_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row.cod_reply_id,row.cod_reply_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','','https://facebook.com' + str(row.id),0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,row.inreplyto,'','',row.id,'','','Facebook-Api',row.cod_commentid_replycomment)
        except Exception as e:
                #print('e')
            print(e)
        #else:
            #print('update ' +str(row.id))
            #cursor.execute("UPDATE [IPE.DW].[dbo].[Informe.Facebook_comments] SET UserID = ?,Nombre = ? WHERE cod_comment_id = ?", row.UserID,row.Nombre,cod_comment_id)

except pyodbc.DatabaseError as err:
    conn.rollback()
else:
    conn.commit()
finally:
    conn.autocommit = True
cursor.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




