import openai
import json
from dotenv import load_dotenv
import os

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

    print("calling openai")
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text[:10000]}  # remove the +'"}'
        ],
        temperature=0.5
    )

    extracted_info = response["choices"][0]["message"]["content"]

    try:
        extracted_data = json.loads(extracted_info)
    except json.JSONDecodeError:
        extracted_data = {"error": "Failed to extract data in JSON format", "raw": extracted_info}

    return extracted_data