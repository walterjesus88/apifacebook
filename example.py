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
      
        conn = pyodbc.connect(  
    'Driver={SQL Server Native Client 11.0};'
                          'Server=119.8.153.140;'
                          'Database=IPE.DW;'
                          'Trusted_Connection=no;'
                          'uid=UserData;'
                           'pwd=1P32020'
                           )


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


