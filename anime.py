import requests
import re
from bs4 import BeautifulSoup

def read_content_8comic(book_id):
    """
    Fetches comic details and chapter URLs, handling cookies to mimic authenticated access.
    """
    url = f"https://www.8comic.com/html/{book_id}.html"
    
    # Use a session to persist cookies
    session = requests.Session()
    
    try:
        # Fetch the book page to capture cookies (e.g., CKVP)
        response = session.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return {"name": None, "chapters": []}

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract book name
    meta_name = soup.find('meta', {'name': 'name'})
    book_name = meta_name['content'].strip() if meta_name else None
    
    # Extract chapters
    chapters = []
    chapters_div = soup.find('div', id='chapters')
    
    if chapters_div:
        for a_tag in chapters_div.find_all('a', onclick=True):
            onclick_js = a_tag['onclick']
            match = re.search(r"cview\('([^']*)'", onclick_js)
            
            if match:
                u_param = match.group(1).replace('.html', '')
                book_part, _, ch_part = u_param.partition('-')
                ch_part = ch_part or '1'  # Default to chapter 1
                
                # Check if the CKVP cookie exists in the session
                ckvp_cookie = session.cookies.get("CKVP")
                if ckvp_cookie:
                    # Use the authenticated URL path
                    chapter_url = f"https://www.8comic.com/view/{book_part}.html?ch={ch_part}"
                else:
                    # Fallback to external URL
                    chapter_url = f"https://articles.onemoreplace.tw/online/new-{book_part}.html?ch={ch_part}"
                
                chapter_name = a_tag.get_text(strip=True)
                chapters.append({"name": chapter_name, "url": chapter_url})
    
    return {"name": book_name, "chapters": chapters}

def read_first_chapter(book_id):
    """
    Retrieves the first chapter content of a comic book from 8comic.com with cookie handling.
    Returns:
        dict: Contains book name, first chapter name, and chapter content (HTML or text)
    """
    session = requests.Session()
    base_url = f"https://www.8comic.com/html/{book_id}.html"
    
    try:
        # First request to get cookies and book details
        response = session.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            "book_name": None,
            "chapter_name": None,
            "content": None,
            "error": f"Failed to fetch book page: {str(e)}"
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract book name
    meta_name = soup.find('meta', {'name': 'name'})
    book_name = meta_name['content'].strip() if meta_name else "Unknown Comic"
    
    # Extract first chapter URL
    first_chapter_url = None
    chapters_div = soup.find('div', id='chapters')
    
    if chapters_div:
        first_chapter_link = chapters_div.find('a', onclick=True)
        if first_chapter_link:
            # Extract parameters from onclick JavaScript
            match = re.search(r"cview\('([^']*)'", first_chapter_link['onclick'])
            if match:
                u_param = match.group(1).replace('.html', '')
                book_part, _, ch_part = u_param.partition('-')
                ch_part = ch_part or '1'
                
                # Determine URL format based on CKVP cookie
                if session.cookies.get("CKVP"):
                    chapter_url = f"https://www.8comic.com/view/{book_part}.html?ch={ch_part}"
                else:
                    chapter_url = f"https://articles.onemoreplace.tw/online/new-{book_part}.html?ch={ch_part}"
                
                first_chapter_url = chapter_url

    if not first_chapter_url:
        return {
            "book_name": book_name,
            "chapter_name": None,
            "content": None,
            "error": "No chapters found"
        }

    try:
        # Second request to get chapter content with maintained cookies
        chapter_response = session.get(first_chapter_url)
        chapter_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            "book_name": book_name,
            "chapter_name": first_chapter_link.get_text(strip=True),
            "content": None,
            "error": f"Failed to fetch chapter: {str(e)}"
        }

    # Return raw content along with parsed information
    return {
        "book_name": book_name,
        "chapter_name": first_chapter_link.get_text(strip=True),
        "content": chapter_response.text,
        "url": first_chapter_url,
        "cookies_present": bool(session.cookies.get("CKVP")),
        "error": None
    }

if __name__ == "__main__":
    book_id = "21163" #input("Enter the 8comic book ID: ")
    result = read_first_chapter(book_id)
    print(result)