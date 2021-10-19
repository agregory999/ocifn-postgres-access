import io
import json
import logging

# OCI Vault
import oci
import base64 

# Postgres
from psycopg2 import connect, Error
from psycopg2.extras import Json

from fdk import response
import psycopg2

# Main function code - takes JSON document and runs custom insert query
def handler(ctx, data: io.BytesIO = None):
    try:
        # inputjson is a dictionary with JSON
        inputjson = json.loads(data.getvalue())
        
        # Set up a response
        outputjson = json.loads("{}")

        # Grab postgres details from config
        cfg = dict(ctx.Config())
        cfg_POSTGRES_HOST = cfg["POSTGRES_HOST"]
        cfg_POSTGRES_PORT = cfg["POSTGRES_PORT"]
        cfg_POSTGRES_USERNAME = cfg["POSTGRES_USERNAME"]
        cfg_POSTGRES_DBNAME = cfg["POSTGRES_DBNAME"]
        cfg_POSTGRES_PWD_OCID = cfg["POSTGRES_PWD_OCID"]
        # password = cfg["POSTGRES_PWD"]

        # Grab password from OCI vault - not plain text
        password = get_text_secret(cfg_POSTGRES_PWD_OCID)

        outputjson["message"] = password

        # Prepare SQL from JSON
        sql_string = get_sql_string(inputjson)
        outputjson["sql"] = sql_string

        # Get Connection to postgres
        conn = connect_postgres(cfg_POSTGRES_HOST, cfg_POSTGRES_PORT, cfg_POSTGRES_USERNAME, password, cfg_POSTGRES_DBNAME)
        cur = conn.cursor()

        # Execute query
        cur.execute(sql_string)

        #records = cur.fetchall()
        outputjson["rowcount"] = str(cur.rowcount)

        # Close up
        cur.close()
        conn.commit()

        # Try a read
        dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        dict_cur.execute("SELECT * FROM biogen_data")
        results_dict = dict_cur.fetchall()
        outputjson["all_rows"] = results_dict

        conn.close()

        # Add output
        # outputjson["message"] = cfg_POSTGRES_PWD_OCID

    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))
        outputjson["error"] = str(ex)

    #logging.getLogger().info("Inside Python Hello World function")
    return response.Response(
        ctx, response_data=outputjson,
        headers={"Content-Type": "application/json"}
    )

##########
# Helper Functions
##########

# Get Secret from OCID
def get_text_secret(secret_ocid):
    signer = oci.auth.signers.get_resource_principals_signer()
    try:
        client = oci.secrets.SecretsClient({}, signer=signer)
        secret_content = client.get_secret_bundle(secret_ocid).data.secret_bundle_content.content.encode('utf-8')
        decrypted_secret_content = base64.b64decode(secret_content).decode("utf-8")
    except Exception as ex:
        print("ERROR: failed to retrieve the secret content", ex, flush=True)
        raise
    return decrypted_secret_content

# Postgres Access
def connect_postgres(hostname,port,username,password,dbname):
    try:
        print
        # Connect Postgres here - return connection
            # declare a new PostgreSQL connection object
        conn = connect(
            dbname = dbname,
            user = username,
            host = hostname,
            password = password,
            # attempt to connect for 3 seconds then raise exception
            connect_timeout = 3
        )
        return conn
    except Exception as ex:
        print("Error: Postgres connection failed", ex, flush=True)
        raise

# SQL String
def get_sql_string(record_list):
    # create a nested list of the records' values
    values = [list(x.values()) for x in record_list]

    # get the column names
    columns = [list(x.keys()) for x in record_list][0]

    # value string for the SQL string
    values_str = ""

    # enumerate over the records' values
    for i, record in enumerate(values):

        # declare empty list for values
        val_list = []
        
        # append each value to a new list of values
        for v, val in enumerate(record):
            if type(val) == str:
                val = str(Json(val)).replace('"', '')
            val_list += [ str(val) ]

        # put parenthesis around each record string
        values_str += "(" + ', '.join( val_list ) + "),\n"

    # remove the last comma and end SQL with a semicolon
    values_str = values_str[:-2] + ";"

    # concatenate the SQL string
    table_name = "biogen_data"
    sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
        table_name,
        ', '.join(columns),
        values_str
    )

    return sql_string