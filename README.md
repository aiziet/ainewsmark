# AINEWSMARK MVP Repo Files
Upload into your repo root.import supabase  # Ensure this is installed: pip install supabase
from datetime import datetime

# Configure Supabase client
SUPABASE_URL = "https://nkccjtyfdlpilaowlmtr.supabase.co"  # Replace with your exact project URL
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5rY2NqdHlmZGxwaWxhb3dsbXRyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY1ODU1NzgsImV4cCI6MjA3MjE2MTU3OH0.I_kbjIcZERwEgRIdiS0Lo7tpNxwBMx-inzh0R1KEVgw"  # The key you provided
supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

# Example function to insert test data (replace your existing ingest logic later)
def insert_test_data():
    data = {
        "title": "Test AI News",
        "headline": "First Article",
        "summary": "A test summary for AI news.",
        "full_content": "Full test content here.",
        "translated_content": "Translated test content.",
        "source": "Test Source",
        "region": "USA",
        "category": "AI",
        "url": "http://test.com",
        "published_at": datetime.utcnow(),
        "created_at": datetime.utcnow(),
        "is_featured": False
    }
    response = supabase_client.table("ai_news").insert(data).execute()
    print("Test data inserted:", response.data)

# Run the test
if __name__ == "__main__":
    insert_test_data()
