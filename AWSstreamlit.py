import streamlit as st
import pandas as pd
import io
import requests
from datetime import datetime
import uuid

file_path_X = "updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx"
df = pd.read_excel(file_path_X, engine='openpyxl')
file_path_Y = "Y_New_Processed.xlsx"
df1 = pd.read_excel(file_path_Y, engine='openpyxl')
# file_path_Y="/Users/atharvabapat/airflow/Y_New_Processed.xlsx"
# df1=pd.read_excel(file_path_Y, engine='openpyxl')
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Client Profile')
    processed_data = output.getvalue()
    return processed_data

def convert_df_to_excel_1(df1):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df1.to_excel(writer, index=False, sheet_name='Family Members')
    processed_data = output.getvalue()
    return processed_data

def get_dags(url):
    username = 'airflow'
    password = 'airflow'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()

def run_dag(dag_id):
    url = f"http://ec2-3-131-35-239.us-east-2.compute.amazonaws.com:8080/api/v1/dags/compute_engine_dag.py/dagRuns"
    username = 'airflow'
    password = 'airflow'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    response = requests.post(url, headers=headers, auth=auth)
    if response.status_code == 200:
        st.success("DAG run successfully triggered! on May 10")
    else:
        st.error("Failed to trigger DAG run.")


def trigger_dag(url):
    username = 'airflow'
    password = 'airflow'
    auth = (username, password)
    headers = {"Content-type": "application/json"}
    dag_run_id = str(uuid.uuid4())
    logical_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "conf": {},
        "dag_run_id": dag_run_id,
        "data_interval_end": logical_date,
        "data_interval_start": logical_date,
        "logical_date": logical_date,
        "note": "string"
    }
    response = requests.post(url, headers=headers, auth=auth, json=payload)
    if response.status_code == 200:
        st.success("DAG run successfully triggered! on May 10")
    else:
        st.error("Failed to trigger DAG run.")

def main():
    st.title("DAGs Dashboard")

    show_dags = st.button("Show DAGs")
    if show_dags:
        url = "http://ec2-18-224-192-110.us-east-2.compute.amazonaws.com:8080/api/v1/dags?limit=100&only_active=true&dag_id_pattern=compute_engine_dag"
        dags = get_dags(url)
        st.write("## Available DAGs:")
        for dag in dags["dags"]:
            st.write(f"**DAG ID:** {dag['dag_id']}")

    

# Create columns for the checkboxes
st.title("Famiology Compute Engine")
col1, col2 = st.columns(2)

with col1:
    x_dataset = st.checkbox("Connect to X dataset")
    y_dataset = st.checkbox("Connect to Y dataset")

with col2:
    x_file= st.checkbox("Processed X file")
    y_file = st.checkbox("Processed Y file")

    show = st.button("Generate Results")
    if show:
        if x_file and x_dataset:
            data = convert_df_to_excel(df)
            print("Inside X dataset")
            url = "http://ec2-18-224-192-110.us-east-2.compute.amazonaws.com:8080/api/v1/dags/compute_engine_dag.py/dagRuns"
            trigger_dag(url)
            st.success("DAG triggered successfully!")
            st.markdown("<h2 style='text-align: center;'>Download Updated CSV</h2>", unsafe_allow_html=True)
            st.download_button(
                label="Press to Download X processed file",
                data=data,
                file_name="Processed_X_file.xlsx",  # Update the file name here
            # key='download-csv'
            # mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        if y_file and y_dataset:
            data = convert_df_to_excel_1(df1)
            print("Inside Y dataset")
            url = "http://ec2-18-224-192-110.us-east-2.compute.amazonaws.com:8080/api/v1/dags/compute_engine_dag.py/dagRuns"
            trigger_dag(url)
            st.download_button(
                label="Press to Download Y processed file",
                data=data,
                file_name="Processed_Y_file.xlsx",  # Update the file name here
            )
            st.success("DAG 'Processed_Y_file' triggered successfully!")
    # st.title("Processed_Y_file")
    # show_processed = st.button("Run Processed_Y_file")
    # if show_processed:
        

if __name__ == "__main__":
    main()
