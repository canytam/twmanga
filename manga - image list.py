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

CONCURRENCY_LIMIT = 5
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
        image_urls = []
        seen_urls = set()
        parts_info = []
        current_url = f"{base_url}.html"
        expected_slot = chapter_slot

        logging.info(f"Starting chapter {chapter_slot} - {chapter_title}")

        while True:
            current_slot = extract_url_slot(current_url)
            if current_slot != expected_slot:
                break

            logging.info(f"Processing part: {current_url}")
            try:
                async with session.get(current_url) as response:
                    response.raise_for_status()
                    text = await response.text()
            except aiohttp.ClientError as e:
                logging.error(f"Failed to fetch part {current_url}: {e}")
                break

            soup = BeautifulSoup(text, 'html.parser')
            comic_contain = soup.find('ul', class_='comic-contain')
            if not comic_contain:
                logging.error(f"No comic-contain found in {current_url}")
                break

            images = comic_contain.find_all('img')
            part_images_count = 0
            for img in images:
                src = img.get('data-src') or img.get('src')
                if not src:
                    logging.warning("Image tag without src or data-src")
                    continue
                
                img_url = urljoin(current_url, src)
                
                if img_url not in seen_urls:
                    seen_urls.add(img_url)
                    image_urls.append(img_url)
                    part_images_count += 1

            part_number = extract_part_number(current_url)
            parts_info.append({
                'part_number': part_number if part_number is not None else 'N/A',
                'image_count': part_images_count
            })

            next_url = await get_next_part(session, current_url)
            if not next_url:
                break

            next_slot = extract_url_slot(next_url)
            next_part = extract_part_number(next_url)
            current_part = extract_part_number(current_url)
            if next_slot != expected_slot or (current_part is not None and next_part != current_part + 1):
                break

            current_url = next_url

        logging.info(f"Completed chapter {chapter_slot} with {len(image_urls)} unique images")
        return image_urls, parts_info


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
            log_path = os.path.join(output_dir, 'images.txt')
            stat_path = os.path.join(output_dir, 'statistics.txt')

            tasks = [process_chapter(session, args.book_id, ch['slot'], ch['title']) for ch in chapters]
            results = await asyncio.gather(*tasks)
            image_urls_list = [result[0] for result in results]
            parts_info_list = [result[1] for result in results]

            # Write image URLs log
            with open(log_path, 'w', encoding='utf-8') as f:
                for idx, (chapter, images) in enumerate(zip(chapters, image_urls_list), 1):
                    f.write(f"Chapter {idx}: {chapter['title']}\n")
                    for img_url in images:
                        f.write(f"{img_url}\n")
                    f.write("\n")

            # Write statistics
            with open(stat_path, 'w', encoding='utf-8') as f:
                f.write("Statistics of Images per Chapter/Part\n")
                f.write("====================================\n\n")
                total_images = 0
                for idx, (chapter, parts_info) in enumerate(zip(chapters, parts_info_list), 1):
                    chapter_title = chapter['title']
                    f.write(f"Chapter {idx}: {chapter_title}\n")
                    chapter_total = 0
                    for part in parts_info:
                        part_num = part['part_number']
                        img_count = part['image_count']
                        chapter_total += img_count
                        f.write(f"  Part {part_num}: {img_count} images\n")
                    f.write(f"  Total: {chapter_total} images\n\n")
                    total_images += chapter_total
                f.write(f"Grand Total: {total_images} images across all chapters\n")

            logging.info(f"Processing complete. Results saved to {log_path} and {stat_path}")

        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise


if __name__ == '__main__':
    asyncio.run(main())
