# utils.py

import requests
import json
import os
import re
import aiohttp
import validators
from datetime import datetime, timedelta
import trafilatura
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import io
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz  # Import pytz for timezone handling


TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"

def get_oauth_token(client_id, client_secret, refresh_token, name=None):

    if not refresh_token:
        raise Exception("OAuth refresh token missing. Re-authorize the application.")

    return refresh_oauth_token(client_id, client_secret, refresh_token)


def refresh_oauth_token(client_id, client_secret, refresh_token):
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(TWITCH_AUTH_URL, data=payload)

    if response.status_code == 200:
        token_response = response.json()
        new_access_token = token_response['access_token']
        new_refresh_token = token_response.get('refresh_token', refresh_token)


        # Update refresh token if provided by Twitch
        if new_refresh_token != refresh_token:
            update_refresh_token(new_refresh_token)

        return new_access_token
    else:
        raise Exception(f"Failed to refresh token: {response.text}")


def update_refresh_token(refresh_token):
    os.environ['TWITCH_REFRESH_TOKEN'] = refresh_token
    print("Updated refresh token in environment.")


def load_about_data():
    """Load 'about' data from a JSON file."""
    with open('data/abouts.json', 'r') as f:
        return json.load(f)


async def web_search(query):
    """Perform a web search using the Google Custom Search API."""
    search_api_key = os.getenv('GOOGLE_API_KEY')
    search_engine_id = os.getenv('SEARCH_ENGINE_ID')
    search_url = "https://www.googleapis.com/customsearch/v1"

    if validators.url(query):
        return await fetch_page_content(query)
    else:
        params = {
            "key": search_api_key,
            "cx": search_engine_id,
            "q": query,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(search_url, params=params) as search_response:
                if search_response.status == 200:
                    return await search_response.json()
                else:
                    error_content = await search_response.text()
                    print(f"Error fetching search results: {search_response.status}")
                    print(f"Error details: {error_content}")
                    return None


async def fetch_page_content(url):
    """Fetch and extract main content from a given URL."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                    html_bytes = await response.read()
                    downloaded = trafilatura.extract(
                        html_bytes, include_comments=False, include_tables=False, favor_precision=True)
                    if downloaded:
                        return downloaded.strip()
                    else:
                        print(f"No content extracted from {url}")
                        return None
                else:
                    print(f"Failed to retrieve {url} with status {response.status}")
                    return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


async def fetch_about_page(url):
    """Fetch 'About' page content using Selenium."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    chrome_bin_path = os.getenv('GOOGLE_CHROME_SHIM', '/app/.apt/usr/bin/google-chrome')
    chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/app/.chromedriver/bin/chromedriver')
    options.binary_location = chrome_bin_path
    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(url)
    try:
        wait = WebDriverWait(driver, 20)
        about_elements = wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'div[data-test-selector="description_test_selector"]')
        ))
        about_content = [element.text for element in about_elements]
    except Exception as e:
        print(f"Error fetching content: {e}")
        about_content = None
    finally:
        driver.quit()
    return about_content


async def analyze_user_intent(client, user_input, conversation_hist):
    """Analyze user intent using OpenAI."""
    analysis_prompt = [
        {
            'role': 'system', 
            'content': (
                'As an AI assistant, analyze the user input and output a JSON object with the following keys:\n'
                '- "include_about": (boolean)\n'
                '- "internet_search": (boolean)\n'
                '- "favorite_songs": (boolean)\n'
                '- "active_users": (boolean)\n'
                '- "code_intent": (boolean)\n'
                '- "rand_num": (list)\n\n'
                '- "ai_memory": (boolean)\n'
                'Respond with only the JSON object and no additional text.\n\n'
                'Guidelines:\n'
                '1. **include_about** should be True only when the response would benefit from details specific to the Twitch stream. ALWAYS set to True when \'Yagami\' is mentioned. Examples include: \'what time does Yagami stream?\', \'what is the discord?\', \'what games does Yagami play?\'\n'
                '2. **perform_search** should be used OFTEN, especially when the response can benefit from an internet search. Examples include: \'What are some recent headlines?\', \'who is the current president?\', \'what time is it?\'.'
                '3. **favorite_songs** should be True when the response is related to songs. Examples include: \'Add your favorite song to the queue.\', \'What songs do you like?\', \'Play a song.\'\n'
                '4. **active_users** should be True if there is a question about the most active users. Example: \'Who is the most active viewer?\'.\n'
                "5. **code_intent** should be True when the user is asking about the bot\'s codebase, functions, implementation details, or debugging. Any question about a command that starts with '!' should set this to true. Examples include: \'How does your command handler work?\', \'Can you explain the purpose of the `find_upcoming_holidays` function?\', \'@darksharkAI tell me about !dssl\', \'Why does your bot use the `holidays` library?\'\n\n"
                '6. **rand_num** is True only when asked for a random number. If true, the list shall contain [lowest_num, highest_num] based on the content of the request.'
                '7. **ai_memory** should be True when the user is asking a question that would benefit from a longer memory. It should only be true when absolutely essential. A short memory is included, but when asked about something that it seems like you should know, but you do not, this should be true. Examples include: \'What did you say earlier?\', \'Do you remember what I said yesterday?\', \'What is the tally?\'\n\n'
                'Respond in JSON format.\nIMPORTANT: Boolean values only: True or False.'
            )
        },
        {'role': 'user', 'content': f"User input: '{user_input}'\n\nDetermine the user's intent and required actions."}
    ]
    analysis_prompt.extend(conversation_hist[-5:])

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=analysis_prompt,
            max_tokens=100
        )
    except Exception as e:
        print(f'Error in analyzing user intent: {e}')

    return response.choices[0].message.content



def extract_json(text):
    """Extract JSON object from text."""
    match = re.search(r'\{.*?\}', text, re.DOTALL)
    if match:
        return match.group(0)
    return '{}'



async def save_data(data, filename, google_service, append=False, date_based_filename=False):
    """Save data to a JSON file and upload it to Google Drive."""
    try:
        # Use EST timezone
        est_timezone = pytz.timezone('America/New_York')

        # Handle date-based filenames
        if date_based_filename:
            current_datetime = datetime.now(est_timezone)
            current_date = current_datetime.strftime("%Y-%m-%d")
            if current_datetime.weekday() == 6:
                weekly_folder = current_date
            else:
                days_since_sunday = current_datetime.weekday() + 1
                previous_sunday = current_datetime - timedelta(days=days_since_sunday)
                weekly_folder = previous_sunday.strftime("%Y-%m-%d")
            filename = f'{weekly_folder} - {filename}'

        # Define the local file path
        filepath = filename

        if append:
            # Load existing data if the file exists and is not empty
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                with open(filepath, 'r') as f:
                    current_data = json.load(f)
            else:
                current_data = []
            # Append new data to existing data
            current_data.extend(data)
            data_to_save = current_data
        else:
            data_to_save = data

        # Save the data to file
        with open(filepath, 'w') as f:
            json.dump(data_to_save, f, indent=4)

        # Upload to Google Drive
        
        upload_file_to_drive(google_service, filename, filepath, folder_id='133DM0d_aQbaEUoDXkO3RmdkscgJFHoIM')

    except Exception as e:
        print(f"Error saving data to {filename}: {e}")

def load_data(filename, google_service, default_data=None, date_based_filename=False):
    """Load data from a JSON file, downloading from Google Drive if necessary."""
    try:
        # Use EST timezone
        est_timezone = pytz.timezone('America/New_York')

        if date_based_filename:
            
            current_datetime = datetime.now(est_timezone)
            current_date = current_datetime.strftime("%Y-%m-%d")
            if current_datetime.weekday() == 6:
                weekly_folder = current_date
            else:
                days_since_sunday = current_datetime.weekday() + 1
                previous_sunday = current_datetime - timedelta(days=days_since_sunday)
                weekly_folder = previous_sunday.strftime("%Y-%m-%d")
            filename = f'{weekly_folder} - {filename}'

            # filename = '2025-03-16 - chat_history.json'
            # matching_files = find_all_files(google_service, filename)
            # delete_file_from_drive(google_service, matching_files)

        filepath = filename

        # Check if the file exists locally
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
        else:
            # Try to download from Google Drive
            file_id = find_file_id(google_service, filename)
            if file_id:
                download_file_from_drive(google_service, file_id, filepath)
                with open(filepath, 'r') as f:
                    data = json.load(f)
            else:
                # No file found, use default data
                data = default_data if default_data is not None else {}
                print(f"No {filename} file found. Starting with default data: {default_data}.")

        # print('Data is:', data)
        return data

    except Exception as e:
        print(f"Error loading data from {filename}: {e}")
        return default_data if default_data is not None else {}


def delete_file_from_drive(service, files):
    """Delete multiple files from Google Drive."""
    for file in files:
        file_id = file['id']
        file_name = file['name']
        try:
            service.files().delete(fileId=file_id).execute()
            print(f"Deleted file '{file_name}' with ID: {file_id}")
        except Exception as e:
            print(f"An error occurred while deleting file '{file_name}': {e}")

            
def upload_file_to_drive(service, filename, filepath, folder_id=None, mime_type='application/json'):
    """Upload a file to Google Drive."""
    from googleapiclient.http import MediaFileUpload

    file_metadata = {'name': filename}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(filepath, mimetype=mime_type)

    # Check if the file already exists
    existing_file_id = find_file_id(service, filename, folder_id=folder_id)

    if existing_file_id:
        # Update the existing file
        file = service.files().update(
            fileId=existing_file_id,
            media_body=media,
            fields='id'
        ).execute()
    else:
        # Upload new file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

    # print(f"File '{filename}' uploaded to Google Drive with ID: {file.get('id')}")
    return file.get('id')


def find_file_id(service, filename, folder_id=None):
    """Find the file ID for a given filename on Google Drive."""
    try:
        if folder_id:
            query = f"name='{filename}' and '{folder_id}' in parents"
        else:
            query = f"name='{filename}'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        if items:
            # print(f"Found '{filename}' on Google Drive with ID: {items[0]['id']}")
            return items[0]['id']
        else:
            # print(f"'{filename}' not found on Google Drive.")
            return None
    except Exception as e:
        # print(f"Error finding file ID for '{filename}': {e}")
        return None


def find_all_files(service, filename, folder_id=None):
    """Find all files with a given filename on Google Drive and print them."""
    try:
        # Construct the query
        if folder_id:
            query = f"name='{filename}' and '{folder_id}' in parents"
        else:
            query = f"name='{filename}'"

        # Initialize an empty list to hold all matching files
        all_files = []

        # Prepare pagination
        page_token = None
        while True:
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token
            ).execute()
            
            items = response.get('files', [])
            if items:
                all_files.extend(items)
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        if all_files:
            # print(f"Found {len(all_files)} file(s) named '{filename}' on Google Drive:")
            for file in all_files:
                print(f"File ID: {file['id']}, Name: {file['name']}")
            return all_files
        else:
            print(f"No files named '{filename}' found on Google Drive.")
            return []
    except Exception as e:
        print(f"Error finding files named '{filename}': {e}")
        return []



def download_file_from_drive(service, file_id, destination):
    """Download a file from Google Drive."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(destination, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            # print(f"Download progress: {int(status.progress() * 100)}%")

        # print(f"File downloaded to {destination}.")
    except Exception as e:
        print(f"Error downloading file: {e}")



def authenticate_gdrive():
    """Authenticate with Google Drive using the service account and return a service object."""
    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    # Load credentials from the service account file specified in constants.py
    # Get the current working directory
    current_directory = os.getcwd()

    # List all files and directories in the current directory
    files = os.listdir(current_directory)

    creds = service_account.Credentials.from_service_account_file(
        os.getenv('SERVICE_ACCOUNT_FILE'), scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=creds)
    return service







# Order 2
    # async def handle_user_interaction(self, selected_personality_key, last_five_messages, final_user_prompt):
        
    #     # Step 2: Apply personality to the base response
    #     personality_content = await self.apply_personality(final_user_prompt, selected_personality_key)
        
    #     # Step 3: Refine the response with user examples and guidelines
    #     final_response = await self.refine_with_examples(personality_content)

    #     # Step 1: Generate base response
    #     base_content = await self.generate_base_response(last_five_messages, final_response)
        
    #     return base_content


    # async def generate_base_response(self, last_five_messages, final_user_prompt):
    #     # Construct the initial messages with MULTI_AGENT_PROMPT
    #     system_message = {"role": "system", "content": system_prompts.MULTI_AGENT_PROMPT}
    #     messages = [system_message]
        
    #     # Append the last five user messages
    #     messages.extend([
    #         {"role": "user", "content": f"{msg['name']}: {msg['content']}"}
    #         for msg in last_five_messages
    #     ])
        
    #     # Append the final user prompt
    #     messages.append({"role": "user", "content": f'You are not responding to this, but translating it to meet the system prompt specifications:\n\n{final_user_prompt}'})
        
    #     # LLM Call 1: Generate base response
    #     base_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=messages,
    #         max_tokens=150,
    #         temperature=0.5
    #     )
        
    #     # Extract the base response content
    #     base_content = base_response.choices[0].message.content.strip()
    #     print('Base response', base_content)
    #     return base_content


    # async def apply_personality(self, base_content, personality_prompt):
    #     # personalities = self.load_personalities()
    #     # personality = personalities[selected_personality_key]
        
    #     # # Prepare the prompt to apply personality
    #     # personality_prompt = personality['prompt']
        
    #     prompt = f"""
    #     You have the personality described. Respond to the original input. Keep the response short, under 200 tokens.

    #     Personality:
    #     {personality_prompt}

    #     Original Input:
    #     {base_content}


    #     Revised Response:
    #     """
        
    #     # LLM Call 2: Apply personality to the base response
    #     personality_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=[{"role": "user", "content": prompt}],
    #         max_tokens=200,
    #         temperature=0.2
    #     )
        
    #     # Extract the personality-aligned response
    #     personality_content = personality_response.choices[0].message.content.strip()
    #     print('personality_content', personality_content)
    #     return personality_content


    # async def refine_with_examples(self, personality_content):
    #     # Prepare the prompt to refine the response based on user examples and guidelines
    #     refine_prompt = f"""
    #     Refine the following response to very closely align with the style in the provided user examples.
    #     The input has already been modified to adhere to previous instructions, and to be in the tone of the response personality. 
    #     Simply ensure it also closely resembles the examples provided in terms of how a typial response is structured.
    #     Only provide the rewrite, and nothing additional.

    #     System Guidelines:
    #     {system_prompts.conversation_text}

    #     Refined Response:
    #     """
        
    #     user_prompt = f'''
    #     Content:\n{personality_content}

    #     Instructions: Translate the following to follow the provided examples. You are not responding to this input, but rewriting before final submission to the user. 
    #     Simply translate it to very closely mirror the examples provided.
    #     '''
    #     # LLM Call 3: Refine the personality-aligned response
    #     refined_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=[
    #             {"role": "system", "content": refine_prompt},
    #             {"role": "user", "content": user_prompt}
    #         ],
    #         max_tokens=120,
    #         temperature=0.3
    #     )
        
    #     # Extract the refined response
    #     refined_content = refined_response.choices[0].message.content.strip()
    #     print('refined_content', refined_content)
    #     return refined_content


# Order 1

    # async def handle_user_interaction(self, selected_personality_key, last_five_messages, final_user_prompt):
    #     # Step 1: Generate base response
    #     base_content = await self.generate_base_response(last_five_messages, final_user_prompt)
        
    #     # Step 2: Apply personality to the base response
    #     personality_content = await self.apply_personality(base_content, selected_personality_key)
        
    #     # Step 3: Refine the response with user examples and guidelines
    #     final_response = await self.refine_with_examples(personality_content)
        
    #     return final_response


    # async def generate_base_response(self, last_five_messages, final_user_prompt):
    #     # Construct the initial messages with MULTI_AGENT_PROMPT
    #     system_message = {"role": "system", "content": system_prompts.MULTI_AGENT_PROMPT}
    #     messages = [system_message]
        
    #     # Append the last five user messages
    #     messages.extend([
    #         {"role": "user", "content": f"{msg['name']}: {msg['content']}"}
    #         for msg in last_five_messages
    #     ])
        
    #     # Append the final user prompt
    #     messages.append({"role": "user", "content": final_user_prompt})
        
    #     # LLM Call 1: Generate base response
    #     base_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=messages,
    #         max_tokens=150,
    #         temperature=0.5
    #     )
        
    #     # Extract the base response content
    #     base_content = base_response.choices[0].message.content.strip()
    #     print('Base response', base_content)
    #     return base_content


    # async def apply_personality(self, base_content, personality_prompt):
    #     # personalities = self.load_personalities()
    #     # personality = personalities[selected_personality_key]
        
    #     # # Prepare the prompt to apply personality
    #     # personality_prompt = personality['prompt']
        
    #     prompt = f"""
    #     You have the following personality.

    #     Personality:
    #     {personality_prompt}

    #     The following response needs to be transalted to sound like your personality.

    #     Original Response:
    #     {base_content}

    #     Instructions: Rewrite the Original Response to match the specified Personality, but maintain as much of the user input as possible. You are not responding to this, but rewriting it.
    #     This has already been modified to adhere to system prompt requirements, it only needs a slight modification to match the assigned role. 
    #     Do not return anything other than the rewrite.

    #     Revised Response:
    #     """
        
    #     # LLM Call 2: Apply personality to the base response
    #     personality_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=[{"role": "user", "content": prompt}],
    #         max_tokens=120,
    #         temperature=0.2
    #     )
        
    #     # Extract the personality-aligned response
    #     personality_content = personality_response.choices[0].message.content.strip()
    #     print('personality_content', personality_content)
    #     return personality_content


    # async def refine_with_examples(self, personality_content):
    #     # Prepare the prompt to refine the response based on user examples and guidelines
    #     refine_prompt = f"""
    #     Refine the following response to very closely align with the style in the provided user examples.
    #     The input has already been modified to adhere to previous instructions, and to be in the tone of the response personality. 
    #     Simply ensure it also closely resembles the examples provided in terms of how a typial response is structured.
    #     Only provide the rewrite, and nothing additional.

    #     System Guidelines:
    #     {system_prompts.conversation_text}

    #     Refined Response:
    #     """
        
    #     user_prompt = f'''
    #     Content:\n{personality_content}

    #     Instructions: Translate the following to follow the provided examples. You are not responing to this input, but rewriting before final submission to the user. 
    #     Simply translate into the correct response style, closely following your system prompt.
    #     '''
    #     # LLM Call 3: Refine the personality-aligned response
    #     refined_response = await self.bot.openai_model_calls(
    #         model="gpt-4o-mini",
    #         messages=[
    #             {"role": "system", "content": refine_prompt},
    #             {"role": "user", "content": user_prompt}
    #         ],
    #         max_tokens=120,
    #         temperature=0.2
    #     )
        
    #     # Extract the refined response
    #     refined_content = refined_response.choices[0].message.content.strip()
    #     print('refined_content', refined_content)
    #     return refined_content
