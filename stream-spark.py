import streamlit as st
import requests
import pandas  as pd
import json

def post_spark_job(user, repo, job, token):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
      "event_type": job
    }

    headers = {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app
    st.write(response)

st.title("Adrian Morales - A01738047 - Reto - Semana Tec")

st.header("Datos de spark-submit Job")

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        st.write(response.json())


github_user  =  st.text_input('Github user', value='Amorale05')
github_repo  =  st.text_input('Github repo', value='semanaTecArteProgra')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='XXX')

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token)

st.header("spark-submit results: Tabla de animalitos")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/Amorale05/semanaTecArteProgra/refs/heads/main/results/animals_endangered.json')

if st.button("GET spark results"):
    get_spark_results(url_results)
