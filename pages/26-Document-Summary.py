import os
import re
import pandas as pd
import streamlit as st
import json
from streamlit_file_browser import st_file_browser
from mydbtools import *

from oci.config import from_file
from oci.object_storage import ObjectStorageClient
from oci.exceptions import ServiceError

import oci
import mysql.connector


import globalvar

# Constants
# ml_generate_options = {'max_tokens', 'temperature', 'top_k', 'top_p', 'repeat_penalty', 'frequency_penalty', 'presence_penalty', 'stop_sequences' }

ml_generate_options = {'max_tokens', 'temperature', 'top_k', 'top_p', 'repeat_penalty', 'frequency_penalty', 'presence_penalty'  }


# MySQL Connectoin Profile
myconfig = globalvar.myconfig


def iff(cond, tvalue, fvalue) :
    return tvalue if cond else fvalue


# Used to connect to MySQL
def connectMySQL(myconfig) :
    cnx = mysql.connector.connect(**myconfig)
    return cnx


# OCI-LLM: Used to prompt the LLM
def query_llm_with_prompt(cursor, prompt, allm, aoptions):

    myoptions = ""
    for myitem in ml_generate_options :
      if myitem in aoptions :
        myoptions = myoptions + ', "' + myitem + '",' + str(aoptions[myitem])

    newprompt = prompt.replace('"', "'")
    call_string = """
        select sys.ML_GENERATE("{query}", JSON_OBJECT("task", "generation", "model_id", "{myllm}" {options}) )
    """.format(query=newprompt,myllm=allm, options=myoptions)
    print(call_string)

    cursor.execute(call_string)

    data = cursor.fetchall()
    
    return data[0][0]



           
def vector_store_load(cursor, abucket,anamespace,afolder,aobjectnames, aschema, atable, amodel, adesc) :

    call_string = '''
      call sys.VECTOR_STORE_LOAD(
      'oci://{bucket}@{namespace}/{folder}/{objectnames}',  '
      '''.format(bucket=abucket,namespace=anamespace,folder=afolder,objectnames=aobjectnames ) + '{' + '''
      "schema_name": "{schema}", "table_name": "{table}", "description": "{desc}", "ocr": true 
      '''.format(schema=aschema, table=atable, desc=adesc) + "}')"

    myformat = aobjectnames.split('.')[1]

    call_string= """
CREATE TABLE {schema}.{tablename} (
  document_name varchar(1024) NOT NULL COMMENT 'RAPID_COLUMN=ENCODING=VARLEN',
  metadata json NOT NULL COMMENT 'RAPID_COLUMN=ENCODING=VARLEN',
  document_id int unsigned NOT NULL,
  segment_number int unsigned NOT NULL,
  segment longtext NOT NULL COMMENT 'RAPID_COLUMN=ENCODING=VARLEN',
  segment_embedding vector(384) NOT NULL COMMENT 'RAPID_COLUMN=ENCODING=VARLEN' /*!80021 ENGINE_ATTRIBUTE '<"model": "{model}">' */,
  PRIMARY KEY (`document_id`,`segment_number`)
) ENGINE=Lakehouse DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='uploaded for testing' SECONDARY_ENGINE=RAPID /*!80021 ENGINE_ATTRIBUTE='<"file": [<"bucket": "{bucket}", "region": "uk-london-1", "pattern": "{folder}.{objectnames}", "namespace": "{namespace}">], "dialect": <"ocr": true, "format": "{format}", "language": "en", "document_parser_model": "meta.llama-3.2-90b-vision-instruct">>' */
""".format(tablename=atable, bucket=abucket, schema=aschema,namespace=anamespace, folder=afolder, objectnames=aobjectnames, format=myformat,model=amodel).replace('<', '{').replace('>', '}')

    # print(call_string)
          
    cursor.execute( 'create database if not exists {dbname}'.format(dbname=aschema))
    cursor.execute( call_string )

    rs = cursor.execute('ALTER TABLE {schema}.{table} secondary_load'.format(schema=aschema, table=atable))


    return rs 


def delete_oci_objects(aprofile, afile, mybucketname, object_name):
    # Load the configuration from the default location (~/.oci/config)
    config = from_file(profile_name=aprofile)

    # Define namespace and bucket name
    mynamespace = config['namespace']  # Tenancy ID is used as the namespace

    # Create an ObjectStorageClient instance
    client = ObjectStorageClient(config)

    listfiles = client.list_objects(mynamespace,mybucketname, prefix=object_name)
    if not listfiles.data.objects:
       print('No files found to be deleted')
       return False    
    else:
       for filenames in listfiles.data.objects:
          print(f'File in Bucket "{mybucketname}" to be deleted: "{filenames.name}"')

       for filenames in listfiles.data.objects:
         client.delete_object(mynamespace, mybucketname,filenames.name)
         print(f'deleted "{filenames.name}" from bucket "{mybucketname}"')




def upload_to_oci_object_storage(aprofile, afile, bucket_name, object_name):
    # Load the configuration from the default location (~/.oci/config)
    config = from_file(profile_name=aprofile)

    # Define namespace and bucket name
    mynamespace = config['namespace']  # Tenancy ID is used as the namespace

    # Create an ObjectStorageClient instance
    client = ObjectStorageClient(config)

    try:
        with afile as file:
            # Upload the file
            response = client.put_object(mynamespace, bucket_name, object_name, file)
            # print(f"Upload successful. ETag: {response.etag}")
            return True
    except ServiceError as e:
        print(f"Service error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False



# Perform RAG 
def summarize(aschema, atable ,  myllm, aprompt):

    print("Summarize...")
           
    with connectMySQL(myconfig)as db:
        
        cursor = db.cursor()
        cursor.execute("set group_concat_max_len=600000")

        # call_string = """
        #   select group_concat(segment order by segment_number) from {schema}.{table} 
        # """.format(schema=aschema, table=atable)

        call_string = """
           select segment from {schema}.{table}  order by segment_number
        """.format(schema=aschema, table=atable)

        cursor.execute(call_string)
        mydata = cursor.fetchall()
        # content = mydata[0][0].replace("'", "")

        content = '\n'.join(str(x[0].replace("'", "")) for x in mydata)

        pattern= r'(\n)\1+'
        repl = r'\1'   
        content = re.sub(pattern, repl, content)
        pattern= r'( )\1+'
        content = re.sub(pattern, repl, content)

        print( "Length of the content : ", len(content))
        print( "words of the content : ", len(content.split(' ')))

        prompt_template = '''
        QUESTION: {prompt}
        TEXT: {documents} \n
        '''
        
        prompt = prompt_template.format( documents = content, prompt=aprompt)
        myoptions = {}


        myoptions["temperature"] = 0
        myoptions["max_tokens"] = 4000
        
        if "mloptions" in st.session_state :
           mloptions = st.session_state['mloptions']
        else :
           mloptions = myoptions
        
        llm_response_result = query_llm_with_prompt(cursor, prompt, myllm, mloptions)
        response = {}
        response_json = json.loads(llm_response_result)
        response['text'] = response_json['text']

        # print(response)
        return response

st.set_page_config(layout="wide")

with st.form('my_form'):
    col1, col2, col3 = st.columns(3)
    with col1 :
      myschema = st.text_input('Vector Store Schema :', 'mydb')
    with col2 :
      mytable = st.text_input('Vector Store table :', 'mytable')
    with col3 :
      mymodel = st.selectbox('Choose embed model : ', getEmbModel())

    
    col1, col2,col3 = st.columns(3)
    with col1 :
      mybucket = st.text_input('Object Storage Bucket :', 'myhw')
    with col2 :
      myfolder = st.text_input('Folder:', 'mypdf')
    with col3 :
      myprofile = st.text_input('OCI config profile:', 'DEFAULT')

    col1, col2 = st.columns(2)
    with col1:
      uploaded_files = st.file_uploader(
        "Choose a (CSV,PDF,HTML,DOC,PPT) file, ONLY one type a time for 1 table :", accept_multiple_files=True
      )
    with col2 :
      myllm = st.selectbox('Choose LLM : ', getLLMModel())

    myprompt = st.text_area('Prompt:', 'Extract the table from the TEXT provided and convert it into JSON  (Article, Description, Unit, Quantity). Unit can be kg,m3 or empty. Quantity is number.  Include all rows.')
    myskip = st.checkbox("Skip Document Upload")
    submitted = st.form_submit_button('Submit')
    gext_html = gext_pdf = gext_doc = gext_ppt = gext_txt =  False


    if submitted:
        print("myskip")
        print(myskip)
        # Load the configuration from the default location (~/.oci/config)
        if not myskip :
          config = from_file(profile_name=myprofile)
          st.write("not skip")

          # Define namespace and bucket name
          mynamespace = config['namespace']  # Tenancy ID is used as the namespace
          ext_html = ext_pdf = ext_doc = ext_ppt = ext_txt =  False
          for uploaded_file in uploaded_files:
            fname,fext = os.path.splitext(uploaded_file.name)
            ext_pdf = (fext in {'.pdf'})
            ext_doc = (fext in {'.doc', '.docx', '.rtf'})
            ext_ppt = (fext in {'.ppt', '.pptx'} )
            ext_txt = (fext in {'.txt', '.csv'})
            ext_html = (fext in {'.html', '.htmlx'})

            gext_pdf = ext_pdf if ext_pdf else gext_pdf
            gext_doc = ext_doc if ext_doc else gext_doc
            gext_ppt = ext_ppt if ext_ppt else gext_ppt
            gext_txt = ext_txt if ext_txt else gext_txt
            gext_html = ext_html if ext_html else gext_html

            object_name = myfolder + '/' + uploaded_file.name + iff(ext_pdf, '.pdf', iff(ext_doc, '.doc', iff(ext_ppt, '.ppt', iff(ext_txt, '.txt', iff(ext_html, '.html', '')))))
            delete_oci_objects(myprofile, uploaded_file, mybucket, myfolder + '/') 

            if upload_to_oci_object_storage(myprofile, uploaded_file, mybucket, object_name) :
               print('uploaded successful')

          firstfile = uploaded_files[0].name
          with connectMySQL(myconfig)as db:
            cursor = db.cursor()
            if gext_pdf :
               myans = vector_store_load(cursor, mybucket, mynamespace, myfolder, '*.pdf', myschema, mytable, mymodel, "uploaded for testing")
            if gext_doc :
               myans = vector_store_load(cursor, mybucket, mynamespace, myfolder, '*.doc', myschema, mytable, mymodel, "uploaded for testing")
            if gext_txt :
               myans = vector_store_load(cursor, mybucket, mynamespace, myfolder, '*.txt', myschema, mytable, mymodel, "uploaded for testing")
            if gext_ppt :
               myans = vector_store_load(cursor, mybucket, mynamespace, myfolder, '*.ppt', myschema, mytable, mymodel, "uploaded for testing")
            if gext_html :
               myans = vector_store_load(cursor, mybucket, mynamespace, myfolder, '*.html', myschema, mytable, mymodel, "uploaded for testing")
        mysummary = summarize(myschema, mytable, myllm, myprompt)
        st.divider()
        st.write(mysummary['text'])
        st.divider()

myoptions = {}
myoptions["temperature"] = 0
myoptions["max_tokens"] = 4000


if "mloptions" in st.session_state :
   mloptions = st.session_state['mloptions']
else :
   mloptions = myoptions

container1 = st.container(border=True)
col0, col1,col2,col3,col4 = container1.columns(5)
col0.text("Options")
#col0.write(mloptions)
option = col1.selectbox("options ", tuple(ml_generate_options), label_visibility='collapsed')
option_value = col2.number_input("Value", 0, label_visibility='collapsed')
add_button = col3.button('add', use_container_width=True)
reset_button = col4.button('reset', use_container_width=True)

if add_button:
    myvalue = {option: option_value}
    mloptions.update(myvalue)
    st.session_state['mloptions'] = mloptions
    col0.write(mloptions)

if reset_button:
    mloptions = myoptions
    st.session_state['mloptions'] = mloptions
    col0.empty()
    col0.write(mloptions)

