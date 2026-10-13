import os
import psycopg2

db_user = os.environ['engine']
db_password =os.environ['password']
db_host =os.environ['host']
db_port =os.environ['port']
db_database = os.environ['engine']

def db_result(query, values=None):
    connection = None
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,  
            database=db_database
        )                      
        cursor = connection.cursor()
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        connection.commit()
        return result
    except psycopg2.Error as e:
        print(f"Database error: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()

def delete_db(query, values):
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,  
        database=db_database
    )
    
    cursor = connection.cursor()

    cursor.execute(query, values) 
    connection.commit()
    cursor.close()
    connection.close()
 
def update_db(query, data):
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,  
        database=db_database
    )                                  
    cursor = connection.cursor()
    cursor.execute(query, data)
    connection.commit()
    cursor.close()
    connection.close()  

def insert_db(query,values):
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,  
        database=db_database
    )                                                                            
    cursor = connection.cursor()
    cursor.execute(query,values)
    connection.commit()
    cursor.close()
    connection.close()      