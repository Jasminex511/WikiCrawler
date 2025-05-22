import openai
import json
import time
from dotenv import load_dotenv
import os
from openai.error import RateLimitError, Timeout, APIError

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')


def extract_profile_info(text):
    prompt = """
    Extract the following details from the text: name, spouse, birthdate, nationality, and keywords (separated by commas).
    For spouse, use 'None' if not mentioned.
    For keywords, select the most relevant ones and maximum 5 keywords in total.
    For birthdates:
    - For dates after year 0, use YYYY-MM-DD format
    - For BC dates, convert to negative years: e.g., 384 BC should be -0384-01-01
    Please format your response in JSON format like this:
    {
        "name": "Name of the person",
        "spouse": "Spouse's name",
        "birthdate": "YYYY-MM-DD or -YYYY-MM-DD for BC dates",
        "nationality": "Nationality of the person",
        "keywords": "keyword1, keyword2, etc."
    }
    """

    for attempt in range(5):
        try:
            print("Calling OpenAI")
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": text[:15000]}
                ],
                temperature=0.5
            )

            extracted_info = response["choices"][0]["message"]["content"]
            res = json.loads(extracted_info)
            print(res)
            return res

        except (RateLimitError, Timeout, APIError, json.JSONDecodeError) as e:
            wait_time = (2 ** attempt) + 0.5
            print(f"Retry {attempt+1}/5 due to: {e}. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

    return None