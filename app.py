import requests

def fetch_url_content():
    # Prompt the user to enter a URL
    url = input("Enter the URL (include http:// or https://): ")
    
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        # Raise an exception for HTTP errors (e.g., 404, 500)
        response.raise_for_status()
        
        # Print the content of the response
        print("\n--- Source Content ---\n")
        print(response.text)
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except KeyboardInterrupt:
        print("\nOperation cancelled by the user.")

if __name__ == "__main__":
    fetch_url_content()
