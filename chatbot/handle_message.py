import asyncio
import time

import aiohttp
import requests

import config
from assistant_handler import get_assistant_response


def get_page_messages(page_id):
  url = f"https://graph.facebook.com/v20.0/{page_id}/conversations?fields=message,created_time,from,to&access_token={config.get_access_token(page_id)}"

  params = {'fields': 'messages{message,created_time,from,to}', 'limit': 2}

  try:
    response = requests.get(url, params=params)

    if response.status_code == 200:
      data = response.json()
      conversations = data.get('data', [])
      messages = []

      for conversation in conversations:
        conversation_id = conversation.get('id')
        for message in conversation['messages']['data']:
          msg = {
              'message_id': message.get('id'),
              'text': message.get('message'),
              'created_time': message.get('created_time'),
              'from': message['from'].get('id'),
              'to': message['to']['data'][0].get('id'),
              'conversation_id': conversation_id
          }
          messages.append(msg)

      return messages
    else:
      print("Failed to retrieve messages:", response.status_code,
            response.text)
      return None

  except Exception as e:
    print(f"Error: {str(e)}")
    return None


#####
async def get_conversation_id_for_messenger_user(user_id, page_id):
  url = "https://graph.facebook.com/v20.0/me/conversations?platform=messenger&access_token={}".format(
      config.get_access_token(page_id))

  params = {'fields': 'participants', 'limit': 5}

  async with aiohttp.ClientSession() as session:
    async with session.get(url, params=params) as response:
      if response.status == 200:
        data = await response.json()
        conversations = data.get('data', [])

        for conversation in conversations:
          participants = conversation.get('participants', {}).get('data', [])
          # print("participants: ", participants)
          for participant in participants:
            if participant.get('id') == user_id:
              return conversation.get('id')
        return None
      else:
        print(f"Failed to retrieve conversations: {response.status}")
        return None


async def get_conversation_id_for_instagram_user(user_id, page_id):
  url = "https://graph.facebook.com/v20.0/me/conversations?platform=instagram&access_token={}".format(
      config.get_access_token(page_id))

  params = {'fields': 'participants', 'limit': 5}

  async with aiohttp.ClientSession() as session:
    async with session.get(url, params=params) as response:
      if response.status == 200:
        data = await response.json()
        conversations = data.get('data', [])

        for conversation in conversations:
          participants = conversation.get('participants', {}).get('data', [])
          # print("participants: ", participants)
          for participant in participants:
            if participant.get('id') == user_id:
              return conversation.get('id')
        return None
      else:
        print(f"Failed to retrieve conversations: {response.status}")
        return None


####


async def get_conversation_id_for_user(user_id, page_id):
  # Map Instagram page ID to Facebook page ID if needed
  original_page_id = page_id
  if page_id == '17841456783426236':  # Instagram page ID
    page_id = '420350114484751'  # Mapped Facebook page ID
    print(f"Instagram page ID {original_page_id} detected in get_conversation_id_for_user, using Facebook page ID {page_id} instead")
  
  # First try Messenger
  conversation_id = await get_conversation_id_for_messenger_user(
      user_id, page_id)
  if conversation_id is not None:
    return conversation_id
  else:
    # Then try Instagram
    conversation_id = await get_conversation_id_for_instagram_user(
        user_id, page_id)
    if conversation_id is not None:
      return conversation_id
  
  print("Failed to retrieve conversations from both platforms")
  return None


async def get_messages_for_conversation(conversation_id, page_id):
  # Map Instagram page ID to Facebook page ID if needed
  original_page_id = page_id
  if page_id == '17841456783426236':  # Instagram page ID
    page_id = '420350114484751'  # Mapped Facebook page ID
    print(f"Instagram page ID {original_page_id} detected in get_messages_for_conversation, using Facebook page ID {page_id} instead")
  
  url = f"https://graph.facebook.com/v20.0/{conversation_id}/messages?access_token={config.get_access_token(page_id)}"

  params = {'fields': 'message,from,created_time', 'limit': 4}
  async with aiohttp.ClientSession() as session:
    async with session.get(url, params=params) as response:
      if response.status == 200:
        messages = (await response.json()).get('data', [])
        # for message in messages:
        #   print(message)

        return messages

      else:
        print("Failed to retrieve messages:", response.status, response.text)
        return ""


def check_word(message, word):
  return word in message


#check if the last 2 messages repeated
async def check_repeated_message(senderPSID, page_id):
  #get Conversation_id of that chat
  conversation_id = await get_conversation_id_for_user(senderPSID, page_id)
  #check if conversation_id exist
  if conversation_id:
    #get_messages_for_conversation
    messages = await get_messages_for_conversation(conversation_id, page_id)

    # Get last 2 messages from the user
    user_messages = []
    for message in messages:
      if message['from'].get('id') == senderPSID:
        user_messages.append(message.get('message', ''))
        if len(user_messages) >= 2:
          break

    # Check if we have 2 messages and they are identical
    if len(user_messages) >= 2 and user_messages[0] == user_messages[1]:
      print(f"User {senderPSID} repeated message: {user_messages[0]}")
      return True

    return False


##########


#check the last message from page
async def get_last_message_from_page(senderPSID, page_id):
  #get Conversation_id of that chat
  conversation_id = await get_conversation_id_for_user(senderPSID, page_id)
  #check if conversation_id exist
  if conversation_id:
    #get_messages_for_conversation
    messages = await get_messages_for_conversation(conversation_id, page_id)

    # Get last message from conversation
    page_message = []
    for message in messages:
      if message['from'].get('id') == page_id:
        page_message.append(message.get('message', ''))
        # print("last page message is :", page_message)

    return page_message


#######
#check the last message for the conversation
async def get_last_message_from_conversation(senderPSID, page_id):
  #get Conversation_id of that chat
  conversation_id = await get_conversation_id_for_user(senderPSID, page_id)
  #check if conversation_id exist
  if conversation_id:
    #get_messages_for_conversation
    messages = await get_messages_for_conversation(conversation_id, page_id)
    # Get last message from conversation
    conversation_message = []
    for message in messages:
      if message['from'].get('id') == senderPSID or page_id:
        conversation_message.append(message.get('message', ''))
        # print("last conversation message is :", conversation_message)
    return conversation_message


#this function need Page Public Content Access
async def check_admin_stop_message(SenderPSID, page_id):
  last_page_messages = await get_last_message_from_page(SenderPSID, page_id)
  if last_page_messages:
    for last_message in last_page_messages:
      if last_message == config.get_stop_message(page_id):
        print(f"Admin sent stop message for this conversation: {last_message}")
        return True

  return False


####

messages_queue = {}


async def merge_user_messages(senderPSID,
                              received_message,
                              page_id,
                              max_time=30.0,
                              max_messages=2):
  """
  Adds a message to the user's queue and creates a task to process it after delay.
  
  Note: page_id is either a string for Instagram or a config object for Facebook.
  We need to handle both cases properly.
  """
  # Make sure we're working with a string page_id, not a config object
  if not isinstance(page_id, str):
    print(f"WARNING: page_id is not a string but {type(page_id)}, using default PAGE_ID")
    page_id = config.PAGE_ID
  
  # Handle Instagram page ID
  if page_id == '17841456783426236':
    print(f"Instagram page ID detected in merge_user_messages, mapping to Facebook page ID")
    page_id = '420350114484751'  # Hardcoded Facebook page ID for Rodood
    print(f"Using Facebook page ID {page_id} for Instagram messages")
  
  """
  Adds a message to the user's queue and creates a task to process it after delay.
  """
  print(f"Adding message to queue for {senderPSID}")
  current_unix_time = int(time.time())

  if 'text' in received_message:  # Make sure the incoming message is a text message
    try:
      if senderPSID not in messages_queue:
        messages_queue[senderPSID] = {
            'page_id': page_id,
            'user_messages_queue': [],
            'first_message_time': current_unix_time
        }

      messages_queue[senderPSID]['user_messages_queue'].append(
          received_message.get('text', ''))
      print(
          f"Queue for {senderPSID} now has {len(messages_queue[senderPSID]['user_messages_queue'])} messages"
      )
      if len(messages_queue[senderPSID]['user_messages_queue']
             ) >= max_messages or time.time() - current_unix_time >= max_time:
        print(
            f"Queue for {senderPSID} has reached max_messages or max_time, processing messages"
        )
        try:
          merged_message = ' '.join(
              messages_queue[senderPSID]["user_messages_queue"])
          print(
              f"Successfully merged message for {senderPSID}: {merged_message}"
          )
        except Exception as merge_error:
          print(f"Error merging messages: {merge_error}")
          return

        # Store the merged message before clearing
        merged_message_copy = merged_message

        # Clear The Merged messages and reset the user entry
        try:
          del messages_queue[senderPSID]
          print(f"Messages queue cleared for {senderPSID}")
        except Exception as clear_error:
          print(f"Error clearing queue: {clear_error}")

        # Send merged message to get_assistant_response if not empty
        if merged_message_copy:
          print(
              f"Preparing to send merged message to assistant: {merged_message_copy}"
          )
          message_structure = {'text': merged_message_copy}
          await get_assistant_response(senderPSID, message_structure, page_id)

      if len(messages_queue[senderPSID]['user_messages_queue']
             ) < max_messages and time.time() - current_unix_time >= max_time:
        #Case the user use one message No need to merge
        for senderPSID in messages_queue:
          if (len(messages_queue[senderPSID]['user_messages_queue']) == 1):
            print(
                f"Queue for {senderPSID} has reached max_time, processing messages"
            )
            try:
              user_msg = messages_queue[senderPSID]['user_messages_queue'][0]
              print(
                  f"Successfully merged message for {senderPSID}: {user_msg}")
            except Exception as merge_error:
              print(f"Error merging messages: {merge_error}")
              return

            # Store the merged message before clearing
            message_copy = user_msg

            # Clear The Merged messages and reset the user entry
            try:
              del messages_queue[senderPSID]
              print(f"Messages queue cleared for {senderPSID}")
            except Exception as clear_error:
              print(f"Error clearing queue: {clear_error}")
            # Send message to get_assistant_response if not empty

            if message_copy:
              print(
                  f"Preparing to send merged message to assistant: {message_copy}"
              )
              message_structure = {'text': message_copy}

              await get_assistant_response(senderPSID, message_structure,
                                           page_id)
      else:
        print("there is a case we will handle it on background loop!")
      return "EVENT_RECEIVED", 200

    except Exception as e:
      print(f"Error in merge_user_messages: {e}")
      return "EVENT_RECEIVED", 200
  else:
    print("Received message is not text")
    return "EVENT_RECEIVED", 200


async def process_message_queue_after_delay(senderPSID):

  while True:
    print("check_user_message_15_minutes")
    current_time = time.time()
    current_time = int(current_time)
    users_to_analyze = []

    # First collect all users that need analysis
    for senderPSID, user_info in messages_queue.items():
      if not user_info.get("user_messages_queue"):
        continue

      last_message_time = int(user_info["first_message_time"])

      # Check if the user has no activity in last 2 seconds
      if len(user_info["user_messages_queue"]
             ) == 1 and current_time - last_message_time >= 2:
        #Make A copy from users to analyze
        users_to_analyze.append((senderPSID, user_info))
        print(
            f"User {senderPSID}, User_info: {user_info} , with Page_id: {user_info['page_id']}"
        )

    # Then process them
    for senderPSID, user_info in users_to_analyze:
      try:
        print(f"Processing bot responding for user {senderPSID}")
        print("USR MSG: ", user_info["user_messages_queue"][0])
        # print("USR MSG: ", user_info["user_messages_queue"])
        message_structure = {'text': user_info["user_messages_queue"][0]}
        
        # Get page_id from user_info and handle Instagram mapping
        page_id = user_info['page_id']
        if page_id == '17841456783426236':
          print(f"Instagram page ID detected in process_message_queue, mapping to Facebook page ID")
          page_id = '420350114484751'  # Hardcoded Facebook page ID for Rodood
          print(f"Using Facebook page ID {page_id} for Instagram messages")

        try:
          await get_assistant_response(senderPSID, message_structure, page_id)

        except asyncio.CancelledError:
          print(f"Task cancelled for user {senderPSID}")
          continue
        except Exception as e:
          print(
              f"Error in get_assistant_response for user {senderPSID}: {str(e)}"
          )
          continue
        # Clear the user's queue
        try:
          del messages_queue[senderPSID]
          print(
              f"User {senderPSID} is Cleared Succesfully from Queue and responding under processing!!"
          )
        except Exception as clear_error:
          print(f"Error clearing queue: {clear_error}")

      except Exception as e:
        print(f"Error processing user {senderPSID}: {str(e)}")
        continue

    # Wait before next check
    print("Waiting 30 seconds before next check")
    try:
      await asyncio.sleep(30)
    except asyncio.CancelledError:
      print("Message queue processing cancelled")
      break
    except Exception as e:
      print(f"Error in message queue processing: {str(e)}")


