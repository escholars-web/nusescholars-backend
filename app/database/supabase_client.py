from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing SUPABASE_URL or SUPABASE_KEY environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# def test_supabase_connection():
#     try:
#         response = supabase.table("census").select("*").limit(5).execute()
#         if response.data:
#             print("Successfully fetched 5 rows:")
#             for row in response.data:
#                 print(row)
#         else:
#             print("No data returned or query failed.")

#     except Exception as e:
#         print("Exception when querying Supabase:", e)

# if __name__ == "__main__":
#     test_supabase_connection()