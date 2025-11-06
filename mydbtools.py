import globalvar
import mysql.connector

# MySQL Connectoin Profile
myconfig = globalvar.myconfig

# Used to connect to MySQL
def connectMySQL(myconfig) :
    cnx = mysql.connector.connect(**myconfig)
    return cnx

def runSQL(theSQL, cnx) :
    cursor = cnx.cursor()
    try : 
        cursor.execute(theSQL)
        data = cursor.fetchall()
        return data

    except mysql.connector.Error as error:
        print("executing SQL failure : {}".format(error))
        st.info("executing SQL error : {}".format(error))
    finally:
            if cnx.is_connected():
                cursor.close()

def getEmbModel() :
    cnx = connectMySQL(myconfig)
    embModels=[]
    try:
        data = runSQL("""
          select model_id, capabilities->>'$[0]' from sys.ML_SUPPORTED_LLMS where capabilities->>'$[0]'='TEXT_EMBEDDINGS'
        """, cnx)
        for row in data:
           embModels.append(row[0])

    except Exception as error:
        embModels=[]
        print("Error while inserting in DB : ", error)

    return tuple(embModels)

def getLLMModel() :
    cnx = connectMySQL(myconfig)
    llmModels=[]
    try:
        data = runSQL("""
          select model_id, capabilities->>'$[0]' from sys.ML_SUPPORTED_LLMS where capabilities->>'$[0]'='GENERATION'
        """, cnx)
        for row in data:
           llmModels.append(row[0])

    except Exception as error:
        llmModels=[]
        print("Error while inserting in DB : ", error)

    return tuple(llmModels)

