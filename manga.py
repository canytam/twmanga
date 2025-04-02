import argparse
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin

# Configure headers to mimic a browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/91.0.4472.124 Safari/537.36'
}

def fetch_page(url, session):
    """Fetch a web page with error handling and retries."""
    try:
        response = session.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_chapters(book_id, content_url, session):
    """Parse the content page to get manga title and chapters."""
    html = fetch_page(content_url, session)
    if not html:
        return None, []
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract manga title
    title_tag = soup.find('h1', class_='comics-detail__title')
    manga_title = title_tag.text.strip() if title_tag else "Unknown Title"
    
    # Extract all chapter items
    chapters = []
    for item in soup.find_all('a', class_='comics-chapters__item'):
        href = item.get('href', '')
        params = parse_qs(urlparse(href).query)
        section = params.get('section_slot', [''])[0]
        chapter = params.get('chapter_slot', [''])[0]
        title = item.find('span').text.strip() if item.find('span') else "Untitled"
        
        chapters.append({
            'section': section,
            'chapter': chapter,
            'title': title,
            'url': f'https://twmanga.com/comic/chapter/{book_id}/{section}_{chapter}.html'
        })
    
    # Sort chapters numerically by chapter slot
    try:
        chapters.sort(key=lambda x: int(x['chapter']))
    except ValueError:
        chapters.sort(key=lambda x: x['chapter'])
    
    return manga_title, chapters

def collect_chapter_urls(initial_url, session):
    """Follow next chapter links to collect all chapter URLs."""
    urls = []
    current_url = initial_url
    visited = set()
    
    while current_url and current_url not in visited:
        visited.add(current_url)
        urls.append(current_url)
        
        html = fetch_page(current_url, session)
        if not html:
            break
            
        soup = BeautifulSoup(html, 'html.parser')
        next_link = soup.find('div', class_='next_chapter')
        if not next_link:
            break
            
        a_tag = next_link.find('a')
        if not a_tag or 'href' not in a_tag.attrs:
            break
            
        current_url = urljoin(current_url, a_tag['href'])
        time.sleep(1)  # Be polite with delays
        
    return urls

def main():
    parser = argparse.ArgumentParser(description='Scrape comic chapter information')
    parser.add_argument('book_id', help='Comic book ID from baozimh.com')
    args = parser.parse_args()
    
    with requests.Session() as session:
        # Step 1: Get manga metadata and chapters
        content_url = f'https://www.baozimh.com/comic/{args.book_id}'
        manga_title, chapters = parse_chapters(args.book_id, content_url, session)
        
        if not chapters:
            print("No chapters found")
            return
            
        # Step 2: Create output directory
        output_dir = f"{manga_title}_{args.book_id}".replace(' ', '_')
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 3: Write initial log file
        log_path = os.path.join(output_dir, 'chapters.log')
        with open(log_path, 'w', encoding='utf-8') as f:
            for idx, ch in enumerate(chapters, 1):
                f.write(f"Index: {idx:03d} | Title: {ch['title']} | URL: {ch['url']}\n")
        
        # Step 4: Collect actual chapter URLs
        print("Starting chapter traversal...")
        visited_urls = collect_chapter_urls(chapters[0]['url'], session)
        
        # Step 5: Update log with actual URLs
        for i in range(min(len(chapters), len(visited_urls))):
            chapters[i]['url'] = visited_urls[i]
        
        with open(log_path, 'w', encoding='utf-8') as f:
            for idx, ch in enumerate(chapters, 1):
                f.write(f"Index: {idx:03d} | Title: {ch['title']} | URL: {ch['url']}\n")
        
        print(f"Process completed. Results saved to {output_dir}/")

if __name__ == '__main__':
    main()