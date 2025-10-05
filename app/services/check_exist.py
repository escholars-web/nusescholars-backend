from app.database.supabase_client import supabase

PROFILES_TABLE = "profiles"
STAGING_TABLE = "staging"

def get_all_names(table):
    """Fetch all full_name values from a table."""
    response = supabase.table(table).select("full_name").execute()
    if hasattr(response, "data"):
        return set(row['full_name'].strip() for row in response.data if row['full_name'] and row['full_name'].strip())
    elif isinstance(response, dict) and 'data' in response:
        return set(row['full_name'].strip() for row in response['data'] if row['full_name'] and row['full_name'].strip())
    else:
        return set()

def compare_profiles_and_staging():
    profiles_names = get_all_names(PROFILES_TABLE)
    staging_names = get_all_names(STAGING_TABLE)

    # Names in staging that already exist in profiles
    existing_names = [name for name in staging_names if name in profiles_names]

    # Names in staging that are new (not in profiles)
    new_names = [name for name in staging_names if name not in profiles_names]

    return existing_names, new_names

if __name__ == "__main__":
    exist, new = compare_profiles_and_staging()
    print("Existing names in profiles:")
    print(exist)
    print("\nNew names (not in profiles):")
    print(new)