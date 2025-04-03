current_url = "https://twmanga.com/comic/chapter/woduzishengji-duburedicestudio_yfelsj/0_0.html"

    try:
        response = requests.get(current_url, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
