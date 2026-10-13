import boto3
import os, json
from api_activity import send_message_to_connection
from db_utils import db_result, update_db
from s3_operations import generate_presigned_url_from_s3_uri

endpoint_url = os.environ.get('endpoint_url')
gatewayapi = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)
access_key = os.environ['access_key']
secret_key = os.environ['secret_key']    

def get_prompt(task):
    query = f"""
        SELECT system_prompt, knowledgebase_id, model_id, model_arn
        FROM prompt_manager
        WHERE task = '{task}';
    """
    query_info = db_result(query)[0]
    knowledge_base_id = query_info[1]
    model_arn = query_info[3]
    model_id = query_info[2]
    prompt_template = query_info[0]
    return prompt_template, knowledge_base_id, model_arn, model_id

def stream_bedrock_to_websocket(model_id, user_prompt, conversation, connection_id, streaming=True):
    print('stream conversationsL', conversation)
    print("user_prompt:", [user_prompt])
    bedrock = boto3.client('bedrock-runtime', region_name='ap-south-1',aws_access_key_id=access_key, aws_secret_access_key=secret_key)  
    try:
        params = {
            "modelId": model_id,
            "messages": conversation,
        }

        reasoning_text = ""
        final_text = ""
        reference = ""
        status_code = 200
        input_tokens = 0
        output_tokens = 0

        response = bedrock.converse_stream(
            modelId=model_id,
            system=[{"text": user_prompt}],
            messages=conversation,
            inferenceConfig={
                "maxTokens": 5000
            }
        )
        print('reasoning response: ', response)

        result = ""
        current_block = ""
        for chunk in response["stream"]:
            # print("chunk:", chunk)
            temp_text = ""
            if "messageStart" in chunk:
                temp_text = "<end><start>"
                current_block = ""
            
            elif "contentBlockDelta" in chunk:
                delta = chunk["contentBlockDelta"]["delta"]
                if "text" in delta:
                    temp_text = delta["text"]
                    current_block += delta["text"]
            
            elif "contentBlockStop" in chunk:
                temp_text = "<end>"
            
            elif "messageStop" in chunk:
                # message fully complete
                result += current_block
                current_block = ""

            if temp_text and streaming:
                send_message_to_connection(connection_id, {
                    "status_code": 200,
                    "output": temp_text
                })

        try:
            input_tokens = chunk['metadata']['usage']['inputTokens']
        except:
            print("No input tokens")
        try:
            output_tokens = chunk['metadata']['usage']['outputTokens']
        except:
            print("No output tokens")

        unanswered_chat = ""
        if "i'm sorry, i don't have that information" in result.lower():
            unanswered_chat = " ,unanswered_chat = COALESCE(unanswered_chat, 0) + 1 "

        # response = {
        #         "status_code": 200,
        #         "output": "<end>"
        #     }
        # send_message_to_connection(connection_id, response)
        
        return {
            "answer": result,
            "input_token": input_tokens,
            "output_token": output_tokens
        }

    except Exception as e:
        print("Error during streaming:", str(e))
        
        response = {
            "status_code": status_code,
            "answer": str(e),
            "reference": "No Reference found."
        }

        print('streaming response', response)
        send_message_to_connection(connection_id, response)
        return {
            "answer": f"Error: {str(e)}",
            "input_token": 0,
            "output_token": 0
        }


def format_conversation(conversation_list):
    return "\n".join([f"{msg['role'].capitalize()}: {msg['content']}" for msg in conversation_list])

def get_knowledge_base_response(conversation, session_id, connection_id, streamed_content):
    
    response = {
        "status_code": 200,
        "output": "<start>Accessing knowledge..."
    }
    send_message_to_connection(connection_id, response)

    total_input_tokens = 0
    total_output_tokens = 0
    status_code = 200
    try:
        
        task = "manekancor_kb_retrieve"
        prompt_template, knowledge_base_id, model_arn, model_id = get_prompt(task)
        answer = stream_bedrock_to_websocket(
            model_id=model_id,   
            user_prompt = prompt_template,  
            conversation=conversation,
            connection_id=connection_id, 
            streaming = False
        )

        # Step 1: Fetch KB + Model details
        task = "manekancor_kb"
        prompt_template, knowledge_base_id, model_arn, model_id = get_prompt(task)
        bedrock_client = boto3.client('bedrock-agent-runtime', region_name='ap-south-1')

        # Step 2: Get KB session ID (if exists)
        query = f"""
            SELECT kb_session_id
            FROM conversation_management 
            WHERE session_id = '{session_id}';
        """
        kb_session_query = db_result(query)
        kb_session_id = kb_session_query[0][0] if kb_session_query and kb_session_query[0] and kb_session_query[0][0] else None
        context_question = answer['answer']
        total_input_tokens += answer["input_token"]
        total_output_tokens += answer["output_token"]
        print("context_question:", context_question)
        if "Error" in context_question:
            raise RuntimeError(context_question)
        # Step 3: Retrieve context (not generate)
        retrieve_params = {
            "knowledgeBaseId": knowledge_base_id,
            "retrievalQuery": {
                "text": context_question 
            },
            "retrievalConfiguration": {
                "vectorSearchConfiguration": {
                    "numberOfResults": 10
                }
            }
        }    

        # Include existing KB session if any
        if kb_session_id:
            retrieve_params["sessionId"] = kb_session_id

        print("Retrieving from KB...")
        retrieve_response = bedrock_client.retrieve(**retrieve_params)
        print('retrieve_response:', retrieve_response)

        # Update new kb_session_id if returned
        if "sessionId" in retrieve_response:
            new_kb_session_id = retrieve_response["sessionId"]
            query = """
                UPDATE conversation_management
                SET kb_session_id = %s
                WHERE session_id = %s;
            """
            update_db(query, (new_kb_session_id, session_id))

        # Step 4: Extract retrieved knowledge
        references = []
        retrieved_text = ""
        try:
            for val in retrieve_response['retrievalResults']:

                retrieved_text += val['content']['text']
                # if "location" in ref and "s3Location" in ref["location"]:
                #     presigned = generate_presigned_url_from_s3_uri(ref["location"]["s3Location"]["uri"])
                # else:
                #     presigned = "N/A"
                # references.append({"content": content, "location": presigned})
        except Exception as e:
            print("No valid references found.", e)
            references = "No Reference found."

        user_prompt = f"{prompt_template}\n\nUse the following knowledge to answer:\n{retrieved_text}."
        prompt_template = prompt_template.replace("{{RETRIEVED_TEXT_FROM_KNOWLEDGEBASE}}", retrieved_text)

        print("Starting Bedrock stream...")
        answer = stream_bedrock_to_websocket(
            model_id=model_id,   
            user_prompt = prompt_template,  
            conversation=conversation,
            connection_id=connection_id
        )
        print('result:', answer['answer'])
        answer['input_token'] += total_input_tokens
        answer['output_token'] +=total_output_tokens

        return answer

    except Exception as e:
        print("Error:", str(e))
        error_response = {
            "status_code": 500,
            "answer": "Internal server error. Try after some time.",
            "reference": str(e)
        }
        send_message_to_connection(connection_id, error_response)
        return "Internal server error. Try after some time."
        
    
