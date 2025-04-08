import argparse
import asyncio
import logging
import os
from urllib.parse import urlparse, parse_qs, urljoin

import aiohttp
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/91.0.4472.124 Safari/537.36'
}

CONCURRENCY_LIMIT = 5  # Adjust based on server tolerance
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)


async def get_content_info(session, book_id):
    """Fetch and parse comic content page information asynchronously."""
    url = f"https://www.baozimh.com/comic/{book_id}"
    logging.info(f"Fetching content page: {url}")
    
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            text = await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Failed to fetch content page: {e}")
        raise

    soup = BeautifulSoup(text, 'html.parser')

    title_tag = soup.find('h1', class_='comics-detail__title')
    if not title_tag:
        raise ValueError("Comic title not found")
    manga_title = title_tag.get_text(strip=True)

    chapters = []
    for item in soup.find_all('a', class_='comics-chapters__item'):
        href = item.get('href', '')
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        slot = params.get('chapter_slot', [None])[0]

        title = item.find('span').get_text(strip=True) if item.find('span') else ''

        if slot and title:
            chapters.append({
                'slot': slot,
                'title': title,
                'parts': []
            })

    return manga_title, chapters


def create_output_dir(manga_title, book_id):
    """Create output directory with sanitized name."""
    sanitized = ''.join(c if c.isalnum() or c in (' ', '_') else '_' for c in manga_title)
    dir_name = f"{sanitized}_{book_id}"
    os.makedirs(dir_name, exist_ok=True)
    return dir_name


def extract_url_slot(url):
    """Extract chapter slot from chapter URL."""
    try:
        path = urlparse(url).path
        filename = path.split('/')[-1]
        base = filename.split('.')[0]
        parts = base.split('_')
        return parts[1] if len(parts) > 1 else None
    except Exception as e:
        logging.error(f"Error parsing URL slot: {e}")
        return None


def extract_part_number(url):
    """Extract part number from URL."""
    try:
        path = urlparse(url).path
        filename = path.split('/')[-1]
        base = filename.split('.')[0]
        parts = base.split('_')
        return int(parts[2]) if len(parts) >= 3 else 1
    except (IndexError, ValueError, AttributeError) as e:
        logging.error(f"Error extracting part number from {url}: {e}")
        return None


async def get_next_part(session, current_url):
    """Asynchronously find next part URL."""
    logging.debug(f"Analyzing navigation at: {current_url}")
    
    try:
        async with session.get(current_url) as response:
            response.raise_for_status()
            text = await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Request failed: {e}")
        return None

    soup = BeautifulSoup(text, 'html.parser')
    nav_divs = soup.find_all('div', class_='next_chapter')

    candidates = []
    for nav_div in nav_divs:
        for a_tag in nav_div.find_all('a'):
            raw_url = a_tag.get('href', '')
            clean_url = urljoin(current_url, raw_url.split('#')[0])
            link_text = a_tag.get_text(strip=True).lower()
            
            candidates.append({
                'url': clean_url,
                'text': link_text
            })

    current_part = extract_part_number(current_url)
    for candidate in candidates:
        candidate_part = extract_part_number(candidate['url'])
        if candidate_part == current_part + 1:
            return candidate['url']
        if any(keyword in candidate['text'] for keyword in ['下一頁', '下一章', '下一页', 'next']):
            return candidate['url']

    return None


async def process_chapter(session, book_id, chapter_slot, chapter_title):
    """Process a chapter asynchronously with concurrency control."""
    async with semaphore:
        base_url = f"https://www.twmanga.com/comic/chapter/{book_id}/0_{chapter_slot}"
        parts = []
        current_url = f"{base_url}.html"
        expected_slot = chapter_slot

        logging.info(f"Starting chapter {chapter_slot} - {chapter_title}")

        while True:
            current_slot = extract_url_slot(current_url)
            if current_slot != expected_slot:
                break

            parts.append(current_url)
            logging.info(f"Chapter {chapter_slot} part {len(parts)} added")

            next_url = await get_next_part(session, current_url)
            if not next_url:
                break

            next_slot = extract_url_slot(next_url)
            next_part = extract_part_number(next_url)
            if next_slot != expected_slot or next_part != extract_part_number(current_url) + 1:
                break

            current_url = next_url

        logging.info(f"Completed chapter {chapter_slot} with {len(parts)} parts")
        return parts


async def main():
    """Main async entry point."""
    parser = argparse.ArgumentParser(description='Async Comic Chapter Scraper')
    parser.add_argument('book_id', help='Comic book identifier')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Debug logging enabled")

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            title, chapters = await get_content_info(session, args.book_id)
            output_dir = create_output_dir(title, args.book_id)
            log_path = os.path.join(output_dir, 'chapters.txt')

            # Process chapters concurrently
            tasks = [process_chapter(session, args.book_id, ch['slot'], ch['title']) for ch in chapters]
            all_parts = await asyncio.gather(*tasks)

            # Write results in original order
            with open(log_path, 'w', encoding='utf-8') as f:
                for idx, (chapter, parts) in enumerate(zip(chapters, all_parts), 1):
                    f.write(f"Chapter {idx}: {chapter['title']}\n")
                    for part in parts:
                        f.write(f"  {part}\n")
                    f.write("\n")

            logging.info(f"Processing complete. Results saved to {log_path}")

        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise


if __name__ == '__main__':
    asyncio.run(main())