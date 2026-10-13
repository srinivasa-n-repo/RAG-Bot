import json
import boto3
import os
import psycopg2
import uuid
import mimetypes
from db_utils import db_result, update_db, insert_db
from s3_operations import generate_presigned_url, generate_presigned_url_from_s3_uri
from query_llm import get_knowledge_base_response

bucket_name = os.environ.get('bucket_name')

def lambda_handler(event, context):
    endpoint_url = os.environ.get('endpoint_url')
    gatewayapi = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
    connection_id = "" 
    is_user_querying = False

    print("event",event)
    # TODO implement 

    try:
        # Support API Gateway event with string body
        overall_event = event
        body = event.get('body', event)
        event = json.loads(body) if isinstance(body, str) else body 
        connection_id = overall_event['requestContext']['connectionId']
    except Exception:
        print('no body')

    print('connection_id:', connection_id)

    def get_chat_metadata(session_id, task):
        value = (session_id, )
        print('task:', task, " session_id:", session_id)
        query = """
            SELECT *
            FROM (
                SELECT DISTINCT session_id, question, answer, timestamp
                FROM conversation_history
                WHERE session_id = %s
                ORDER BY timestamp DESC
                LIMIT 5
            ) t
            ORDER BY timestamp ASC;
            """

        if task == "last_chat":
            query = (
                "SELECT DISTINCT session_id, question, answer, timestamp "
                "FROM conversation_history "
                "WHERE session_id = %s order by timestamp desc limit 1;"
            )
            value = (session_id,)
        
        if task == "get_session_info":
            query =  """SELECT session_id,
                    question,
                    answer,
                    timestamp
                FROM conversation_history
                WHERE session_id = %s 
                order by timestamp asc;"""
        
        rows = db_result(query, value)
        output = []
        if rows:
            for val in rows:
                temp_output = {
                    "question": val[1],
                    "answer": val[2],
                    "conversation_time_stamp": str(val[3])
                }
                try:
                    temp_output["view_url"] = generate_presigned_url_from_s3_uri(bucket_name, val[4])
                    print('val[4]', val[4])
                except Exception as e:
                    print('error:', e)
                    
                output.append(temp_output)
        conversations = {"session_id": session_id, "conversations": output}
        print('conversations:', conversations)

        return {
            "statusCode": 200,
            "body":conversations
        }
    

    event_type = event['event_type']

    print("even_type",event_type)


    if event_type == "list":
        page_no = int(event['page_no'])

        page_size = int(event['limit'])
        user_name = event['user_name']
        offset = (page_no - 1) * page_size

        query ="""
            SELECT serial_no, session_id, conversation_start_time_stamp, conversation_end_time, user_name, status, file_name 
            FROM conversation_management
            where user_name = %s
            ORDER BY conversation_start_time_stamp DESC LIMIT %s OFFSET %s;
            """
        
        query_list = (user_name, page_size, offset)
        rows = db_result(query, query_list)

        # total count (without pagination)
        count_query ="""
            SELECT COUNT(*)
            FROM conversation_management
            where user_name = %s;
            """
        count_rows = db_result(count_query, (user_name,))
        total_count = count_rows[0][0] if count_rows else 0
        print('total_count:', total_count)

        output = []
        for r in rows:
            chat_data = get_chat_metadata(r[1], task=None)['body']
            conversations = chat_data.get('conversations', [])

            last_chat = conversations[0].get('question', "") if conversations else ""

            output.append({
                "session_id": r[1],
                "conversation_start_time_stamp": str(r[2]),
                "status": r[5],
                "file_name": r[6],
                "last_chat": last_chat
            })

        return {
            "statusCode": 200,
            "body": {"total_count": total_count, "page_no": page_no, "page_size": page_size, "items": output}
        }


    if event_type == "list_user_sessions":
        session_id = str(event.get('session_id'))

        return get_chat_metadata(session_id, "get_session_info")
    
    if event_type == "query_document":
        session_id = event['session_id']
        question = event['question']
        user_name = event['user_name']

        try:
            chat_id = str(uuid.uuid4())
            is_user_querying = True
            object_key = ""
            file_name = ""
            chat_history = ""
            task_type = "chat_document"
            answer = ""
            input_token = 0
            output_token = 0

            chat_history_query = (
                """
                SELECT question, answer
                FROM conversation_history
                WHERE session_id = %s
                ORDER BY timestamp ASC;
                """
            )
            conversations = []
            chat_history_rows = db_result(chat_history_query, (session_id,))
            chat_history = ""
            if chat_history_rows:
                # for row in chat_history_rows:
                #     conversations.append({
                #         "role": "user",
                #         "content": row[0]
                #     })
                #     conversations.append(
                #     {
                #         "role": "assistant",
                #         "content": row[1]
                #     })
                for row in chat_history_rows:
                    conversations.append({
                        "role": "user",
                        "content": [
                            {"text": row[0]}
                        ]
                    })
                    conversations.append({
                        "role": "assistant",
                        "content": [
                            {"text": row[1]}
                        ]
                    })

                    chat_history += f"\n\nQuestion: {row[0]}\nAnswer: {row[1]}"
            else:
                query = """
                INSERT INTO
                conversation_management
                (session_id, conversation_start_time_stamp, user_name)
                values(%s, now(), %s);
                """
                values = (session_id, user_name)

                print('values:',[values])
                insert_db(query,values)

            chat_history += f"\n\nQuestion: {question}\nAnswer:"        
            update_conversation_query = """
                INSERT INTO conversation_history (session_id, chat_id, question, answer, timestamp, input_token, output_token, user_name)
                VALUES (%s, %s, %s, %s, now(), %s, %s, %s);
            """
            update_conversation_values = (session_id, chat_id, question, answer, input_token, output_token, user_name)
            insert_db(update_conversation_query, update_conversation_values)
            conversations.append(
            {
                "role": "user",
                "content": [
                {"text": f"Current Question: {question}"}
                ]            
            }
        )
            
            result = get_knowledge_base_response(conversations, session_id, connection_id, question)
            answer = result.get("answer", "")
            input_token = result.get("input_token", "")
            output_token = result.get("output_token", "")
            update_ocr_query = """
                    UPDATE conversation_history
                    SET answer = %s
                    WHERE session_id = %s and chat_id = %s;
                """
            
            update_ocr_values = (answer, str(session_id), chat_id)
            update_db(update_ocr_query, update_ocr_values)

        
            return {
                "statusCode": 200,
                "body": result,
            }

        except Exception as e:
            print(f"Error processing query: {e}")
            if is_user_querying:
                if connection_id:
                    try:
                        gatewayapi.post_to_connection(
                            ConnectionId=connection_id,
                            Data=json.dumps({"output": "The application ran into an unexpected issue."})
                        )
                    except Exception as e:
                        print(f"WebSocket stream send error: {e}")


            status = 'failed'
            update_ocr_query = """
                UPDATE conversation_management
                SET status = %s
                WHERE session_id = %s;
            """
        
            update_ocr_values = (status, str(session_id))
            update_db(update_ocr_query, update_ocr_values)
     
