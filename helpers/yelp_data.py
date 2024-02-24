import requests


# Function to get restaurants by cuisine type
def get_restaurants_by_cuisine(api_key, cuisine, location="Manhattan", limit=50):
    """
    Fetch restaurants by cuisine type.
    """
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {"Authorization": f"Bearer {api_key}"}
    offset = 0
    restaurants = []

    while True:
        params = {
            "term": f"{cuisine} restaurants",
            "location": location,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break

        data = response.json()
        businesses = data['businesses']

        for business in businesses:
            if business['id'] not in [restaurant['id'] for restaurant in restaurants]:
                restaurants.append(business)

        # Since we only need 50 restaurants, no need to adjust offset or check total
        break

    return restaurants[:50]  # Ensure only up to 50 restaurants are returned


# Function to fetch restaurants for multiple cuisines
def fetch_restaurants_for_cuisines(api_key, cuisines, location="New York"):
    """
    Fetch 50 restaurants for each specified cuisine.
    """
    all_restaurants = {}

    for cuisine in cuisines:
        print(f"Fetching {cuisine} restaurants...")
        restaurants = get_restaurants_by_cuisine(api_key, cuisine, location)
        all_restaurants[cuisine] = restaurants
        print(f"Found {len(restaurants)} {cuisine} restaurants.")

    return all_restaurants


# Example usage
API_KEY = ""
cuisines = ["Italian", "Chinese", "Mexican"]  # List of cuisines to fetch
all_cuisine_restaurants = fetch_restaurants_for_cuisines(API_KEY, cuisines)

