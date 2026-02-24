import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

API_KEY = os.getenv("API_KEY", "")
API_URL = os.getenv("API_URL")
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
LLM_MODEL = os.getenv("LLM_MODEL")

llm_client = OpenAI(api_key=API_KEY, base_url=API_URL)
