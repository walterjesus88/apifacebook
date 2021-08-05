import sys#pip install -U python-dotenv
import apifacebook
import os
from dotenv import load_dotenv
#load_dotenv()
import streamlit as st 

# OR, the same with increased verbosity
load_dotenv(verbose=True)

# OR, explicitly providing path to '.env'
from pathlib import Path  # Python 3.6+ only
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

TOKEN_COMEX = os.getenv("TOKEN_COMEX")
TOKEN_SACATUCUENTA =  os.getenv("TOKEN_SACATUCUENTA")
TOKEN_PIENSAPE =  os.getenv("TOKEN_PIENSAPE")
TOKEN_IPE =  os.getenv("TOKEN_IPE")
TOKEN_PROFUTURO =  os.getenv("TOKEN_PROFUTURO")

Clientes = [
			{'Cliente':'Comex','access_token':TOKEN_COMEX,'cuenta':'141796229342716'},
            {'Cliente':'Saca tu cuenta', 'access_token':TOKEN_SACATUCUENTA,'cuenta':'100330785132565'},
            {'Cliente':'Piensape','access_token':TOKEN_PIENSAPE,'cuenta':'101816778261420'},
            {'Cliente':'IPE','access_token':TOKEN_IPE,'cuenta':'112755042067857'},
            #{'Cliente':'Profuturo AFP','access_token':TOKEN_PROFUTURO,'cuenta':'133522228271'}
            ]

def clientes():
	for i in Clientes:
		calldata = apifacebook.ApiFacebook(i['Cliente'], i['access_token'],i['cuenta'])
		res = calldata.get_procesar()
		st.write('Result Completado: %s' % res)


def inbox():
    for i in Clientes:
        if i['Cliente']=='Profuturo AFP':
            calldata = apifacebook.ApiFacebook(i['Cliente'], i['access_token'],i['cuenta'])
            res = calldata.get_inbox()
            st.write('Result Profuturo Completado: %s' % res)

def sentiment():
    calldata = apifacebook.ApiFacebook()
    res = calldata.sentiment()
    st.write('Result Sentiment Completado: %s' % res)

def stopwords():
    calldata = apifacebook.ApiFacebook()
    res = calldata.stop_words()
    st.write('Result stopwords Completado: %s' % res)

#for i in Clientes:
#	cl = clientes(i)

#if st.button('add'):
#    result = add(1, 2)
#    st.write('result: %s' % result)


# def fun():
#     st.write('fun click')
#     return


if st.button('Ejecutar ApiFacebook'):
    cl =clientes()
    st.write('ha terminado de ejecutar el ApiFacebook')

if st.button('Ejecutar Sentiment'):
    cl =sentiment()
    st.write('ha terminado ejecutar sentiment ipe')

if st.button('Limpiar Coments'):
    cl =stopwords()
    st.write('ha terminado limpiar comentarios ipe')
