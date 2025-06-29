import asyncio
import config
from openai import OpenAI
import time 
client = OpenAI(api_key=config.OPENAI_API_KEY)


# Simplified to avoid redundancy
def load_client_assistant(assistant_id):
    print("assistant_id inside load_client_assistant:", assistant_id)
    # Retrieve the assistant directly
    my_assistant = client.beta.assistants.retrieve(assistant_id=assistant_id)
    return my_assistant


# client, my_assistant, user_thread = load_client_assistant_thread(config.Assistants_Pages_Ids[config.PAGE_ID])

# **No longer being used**
# async def wait_on_run(client, run, user_thread_id):
#     while run.status == "queued" or run.status == "in_progress":
#         run = client.beta.threads.runs.retrieve(thread_id=user_thread_id,
#                                                 run_id=run.id)
#         await asyncio.sleep(1)

#     return run


async def get_chatgpt_response(user_message, user_state, senderPSID, page_id):
    try:
        # Get the correct assistant ID for this page
        assistant_id = config.get_assistant_id(page_id)
        print(f"Using assistant ID {assistant_id} for page {page_id}")
        
        # Load the assistant
        my_assistant = load_client_assistant(assistant_id)
        
        # Define a conversation key for this user+page combination
        conversation_key = f"{senderPSID}_{page_id}"
        
        # CRITICAL FIX: Check restored user state from database FIRST for thread continuity
        # This ensures conversations maintain context after system restarts
        existing_thread_id = user_state[senderPSID].get("thread_id")
        
        # Check if this conversation is already in our global user_threads dictionary (from main_simple.py)
        # We'll use the global dictionary if available
        try:
            # Import the user_threads dictionary from main_simple.py if possible
            from main_simple import user_threads
            has_global_threads = True
            
            # If we have a thread ID in the global dictionary, use it
            if conversation_key in user_threads and user_threads[conversation_key].get('thread_id'):
                user_thread_id = user_threads[conversation_key]['thread_id']
                user_state[senderPSID]["thread_id"] = user_thread_id
                print(f"Using thread ID {user_thread_id} from global dictionary for {senderPSID}")
                existing_thread_id = user_thread_id
            
        except ImportError:
            has_global_threads = False
            print("Global user_threads not available")

        # If we have a restored thread ID from database, use it
        if existing_thread_id:
            print(f"[THREAD_CONTINUITY] Using existing thread ID: {existing_thread_id} for {senderPSID}")
            user_state[senderPSID]["thread_id"] = existing_thread_id
            
            # Sync with global threads dictionary if available
            if has_global_threads:
                user_threads[conversation_key] = {
                    'thread_id': existing_thread_id,
                    'assistant_id': assistant_id,
                    'last_message': user_state[senderPSID].get('last_message')
                }

        # Check if user has a previous thread
        if user_state[senderPSID].get("thread_id") is None:
            print("chatgpt is running case A - New thread")
            # Create a new thread of conversation
            print("Creating new conversation thread")
            user_thread = client.beta.threads.create()
            user_thread_id = user_thread.id
            user_state[senderPSID]["thread_id"] = user_thread_id
            
            # If global threads are available, store this thread ID there too
            if has_global_threads:
                user_threads[conversation_key] = {
                    'thread_id': user_thread_id,
                    'assistant_id': assistant_id,
                    'last_message': None
                }
                
            # Add welcome message to user state (for context tracking)
            # Get the greeting message from config
            greeting_message = config.get_greeting_message(page_id)
            
            # Use a simple default welcome message if none is configured
            # This avoids hardcoded references to specific greeting messages
            if greeting_message:
                welcome_message = greeting_message
            else:
                # Generic welcome message without specific content for all pages
                welcome_message = '''أهلاً
مبسوطين بتواصلك معانا 😊
نحب جدًا نحكى معك أكتر'''
                
            user_state[senderPSID]["messages_context"].append({
                "role": "assistant",
                "content": welcome_message
            })
            
            # Create user message in the thread
            message = client.beta.threads.messages.create(
                thread_id=user_thread_id, role="user", content=user_message
            )
            
            # Save user message in user_state
            user_state[senderPSID]["messages_context"].append({
                "role": "user",
                "content": user_message
            })

            # Create and poll a run to get the assistant's response
            run = client.beta.threads.runs.create(
                thread_id=user_thread_id, assistant_id=my_assistant.id
            )
            
            # Wait for the run to complete
            while run.status in ["queued", "in_progress"]:
                # Add a small delay to avoid rate limits
                await asyncio.sleep(0.5)
                run = client.beta.threads.runs.retrieve(
                    thread_id=user_thread_id, run_id=run.id
                )
                
                # Add a timeout mechanism
                if time.time() - run.created_at >= 20:
                    print(f"Run timed out after 20 seconds, status: {run.status}")
                    break
            
            # If run completed successfully, get assistant's response
            if run.status == "completed":
                # List messages, most recent first
                messages = client.beta.threads.messages.list(
                    thread_id=user_thread_id, order="desc", limit=1
                )
                
                # Get the most recent message (the assistant's response)
                if len(messages.data) > 0:
                    latest_message = messages.data[0]
                    if latest_message.role == "assistant" and len(latest_message.content) > 0:
                        assistant_response = latest_message.content[0].text.value
                        
                        # Store the response in user state
                        user_state[senderPSID]["last_message"] = assistant_response
                        
                        # Also store in global threads if available
                        if has_global_threads:
                            user_threads[conversation_key]['last_message'] = assistant_response
                        
                        # Log the response
                        print(f"Assistant response: {assistant_response[:50]}...")
                        
                        # Save to user_state for context maintenance
                        user_state[senderPSID]["messages_context"].append({
                            "role": "assistant",
                            "content": assistant_response
                        })
                        
                        respond = {"text": assistant_response}
                        return respond
            
            # If we got here, something went wrong
            print(f"Run failed or timed out with status: {run.status}")
            error_message = "عذراً، حدث خطأ في معالجة رسالتك. يرجى المحاولة مرة أخرى."
            respond = {"text": error_message}
            return respond

        else:
            # Use existing thread
            print("chatgpt is running case B - Existing thread")
            user_thread_id = user_state[senderPSID]["thread_id"]
            print(f"Using existing thread ID: {user_thread_id}")
            
            # Add user's new message to state
            user_state[senderPSID]["messages_context"].append({
                "role": "user",
                "content": user_message
            })
            
            # Check for active runs before creating a new one
            runs = client.beta.threads.runs.list(thread_id=user_thread_id)
            active_run = next((run for run in runs.data if run.status in ["queued", "in_progress"]), None)
            
            # If there's an active run, wait for it to complete
            if active_run:
                print(f"Found active run {active_run.id} in state {active_run.status}, waiting for completion")
                start_time = time.time()
                while active_run.status in ["queued", "in_progress"]:
                    active_run = client.beta.threads.runs.retrieve(
                        thread_id=user_thread_id, run_id=active_run.id
                    )
                    # Add timeout mechanism
                    if time.time() - start_time > 20:
                        print(f"Run timed out after 20 seconds, aborting")
                        break
                    await asyncio.sleep(0.5)
            
            # Create message in thread
            message = client.beta.threads.messages.create(
                thread_id=user_thread_id, role="user", content=user_message
            )
            
            # Create a new run
            run = client.beta.threads.runs.create(
                thread_id=user_thread_id, assistant_id=my_assistant.id
            )
            
            # Wait for the run to complete
            start_time = time.time()
            while run.status in ["queued", "in_progress"]:
                # Add a small delay to avoid rate limits
                await asyncio.sleep(0.5)
                run = client.beta.threads.runs.retrieve(
                    thread_id=user_thread_id, run_id=run.id
                )
                
                # Add a timeout mechanism
                if time.time() - start_time > 20:
                    print(f"Run timed out after 20 seconds, status: {run.status}")
                    break
            
            # If run completed successfully, get assistant's response
            if run.status == "completed":
                # List messages, most recent first
                messages = client.beta.threads.messages.list(
                    thread_id=user_thread_id, order="desc", limit=1
                )
                
                # Get the most recent message (the assistant's response)
                if len(messages.data) > 0:
                    latest_message = messages.data[0]
                    if latest_message.role == "assistant" and len(latest_message.content) > 0:
                        assistant_response = latest_message.content[0].text.value
                        
                        # Store the response
                        user_state[senderPSID]["last_message"] = assistant_response
                        
                        # Also store in global threads if available
                        if has_global_threads:
                            user_threads[conversation_key]['last_message'] = assistant_response
                        
                        # Log the response
                        print(f"Assistant response: {assistant_response[:50]}...")
                        
                        # Save to user_state for context maintenance
                        user_state[senderPSID]["messages_context"].append({
                            "role": "assistant",
                            "content": assistant_response
                        })
                        
                        # Check if the response is the same as the user message (error case)
                        if user_message == assistant_response:
                            respond = {
                                "text": "عذراً، لم أفهم سؤالك جيدا ، اسأل بطريقة اخرى لأفهم ما تقصده"
                            }
                        else:
                            respond = {"text": assistant_response}
                        
                        return respond
            
            # If we got here, something went wrong
            print(f"Run failed or timed out with status: {run.status}")
            error_message = "عذراً، حدث خطأ في معالجة رسالتك. يرجى المحاولة مرة أخرى."
            respond = {"text": error_message}
            return respond
                
    except Exception as e:
        print(f"Error in get_chatgpt_response: {str(e)}")
        error_message = "عذراً، حدث خطأ في معالجة رسالتك. يرجى المحاولة مرة أخرى."
        return {"text": error_message}
