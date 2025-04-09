import argparse
import asyncio
import datetime
import logging
import os
import shutil
import sys
import tempfile
from functools import partial
from urllib.parse import urlparse, parse_qs, urljoin

import aiohttp
import img2pdf
from bs4 import BeautifulSoup
from PIL import Image
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import tqdm.asyncio
import unicodedata
from tqdm.contrib.logging import logging_redirect_tqdm


# Initial console logging configuration
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

CONCURRENCY_LIMIT = 10
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
IMAGE_CONCURRENCY_LIMIT = 20
image_semaphore = asyncio.Semaphore(IMAGE_CONCURRENCY_LIMIT)
PDF_PROCESSOR_WORKERS = 4

async def get_content_info(session, book_id):
    """Fetch and parse comic content page information asynchronously."""
    url = f"https://www.twmanga.com/comic/{book_id}"
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
    """Process a chapter to extract image URLs asynchronously."""
    async with semaphore:
        base_url = f"https://www.twmanga.com/comic/chapter/{book_id}/0_{chapter_slot}"
        image_urls = []
        seen_urls = set()
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
            for img in images:
                src = img.get('data-src') or img.get('src')
                if not src:
                    logging.warning("Image tag without src or data-src")
                    continue
                
                img_url = urljoin(current_url, src)
                
                if img_url not in seen_urls:
                    seen_urls.add(img_url)
                    image_urls.append(img_url)
                    logging.debug(f"Found new image URL: {img_url}")
                else:
                    logging.debug(f"Skipped duplicate image: {img_url}")

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
        return image_urls

def validate_image_dimensions(filepath):
    """Check if image dimensions in PDF points are within valid range."""
    try:
        with Image.open(filepath) as img:
            dpi_x, dpi_y = img.info.get('dpi', (96.0, 96.0))
            dpi_x = float(dpi_x) if float(dpi_x) > 0 else 96.0
            dpi_y = float(dpi_y) if float(dpi_y) > 0 else 96.0

            width_pt = (img.width / dpi_x) * 72
            height_pt = (img.height / dpi_y) * 72
            logging.debug(
                f"Image DPI: ({dpi_x:.1f}, {dpi_y:.1f}), "
                f"Dimensions: {img.width}x{img.height}px, "
                f"PDF Points: {width_pt:.1f}x{height_pt:.1f}"
            )
            if 3 <= width_pt <= 14400 and 3 <= height_pt <= 14400:
                return True
            else:
                logging.warning(
                    f"Invalid PDF size {width_pt:.1f}x{height_pt:.1f} pts "
                    f"for {filepath}. Skipping."
                )
                return False
    except Exception as e:
        logging.error(f"Image validation failed for {filepath}: {e}")
        return False
            
async def async_download_image(session, url, chapter_dir, idx):
    """Download and validate images asynchronously."""
    async with image_semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                
                content_type = response.headers.get('Content-Type', '').lower()
                if 'image/jpeg' in content_type:
                    ext = '.jpg'
                elif 'image/png' in content_type:
                    ext = '.png'
                elif 'image/gif' in content_type:
                    ext = '.gif'
                else:
                    ext = '.bin'

                filename = f"image_{idx:03d}{ext}"
                filepath = os.path.join(chapter_dir, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(content)
                
                loop = asyncio.get_event_loop()
                is_valid = await loop.run_in_executor(
                    None, partial(validate_image_dimensions, filepath))
                
                if not is_valid:
                    os.remove(filepath)
                    return None
                
                return filepath
        except Exception as e:
            logging.error(f"Image download failed: {url} - {e}")
            return None

def sanitize_filename(name):
    """Safely sanitize filenames with Unicode support."""
    name = unicodedata.normalize('NFKC', name)
    return "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in name]).strip('_')

async def verify_image_integrity(image_path):
    """Ensure images are valid and readable."""
    try:
        with Image.open(image_path) as img:
            img.verify()
        return True
    except Exception as e:
        logging.error(f"Invalid image: {image_path} - {str(e)}")
        return False

async def create_pdf_sync(image_paths, output_path):
    """Synchronous PDF creation wrapper with detailed diagnostics."""
    valid_images = []
    for img_path in image_paths:
        try:
            with Image.open(img_path) as img:
                dpi_x, dpi_y = img.info.get('dpi', (96.0, 96.0))
                dpi_x = dpi_x if dpi_x > 0 else 96.0
                dpi_y = dpi_y if dpi_y > 0 else 96.0
                width_pt = (img.width / dpi_x) * 72
                height_pt = (img.height / dpi_y) * 72
                if not (3 <= width_pt <= 14400 and 3 <= height_pt <= 14400):
                    logging.warning(f"Skipping {img_path} due to invalid dimensions post-validation")
                    continue
            valid_images.append(img_path)
        except Exception as e:
            logging.error(f"Invalid image {img_path} detected pre-conversion: {e}")
    
    if not valid_images:
        logging.error("No valid images remaining for PDF creation")
        return False

    try:
        with open(output_path, "wb") as f:
            f.write(img2pdf.convert(valid_images))
        
        if os.path.getsize(output_path) < 1024:
            raise RuntimeError("PDF file too small, likely invalid")
            
        return True
    except img2pdf.ImageOpenError as e:
        logging.error(f"Image opening failed during PDF creation: {e}")
    except ValueError as e:
        if "Page size must be between 3" in str(e):
            logging.error("Invalid page size despite validation. Check image DPI metadata.")
        else:
            logging.error(f"PDF value error: {e}")
    except Exception as e:
        logging.error(f"Unexpected PDF creation error: {e}")
    
    if os.path.exists(output_path):
        os.remove(output_path)
    return False

async def download_and_create_pdf(session, output_dir, manga_title, chapter_slot, chapter_title, image_urls, keep_images):
    """Robust PDF creation with enhanced diagnostics."""
    sanitized_manga_title = sanitize_filename(manga_title)
    sanitized_chapter_title = sanitize_filename(chapter_title)
    pdf_filename = f"Chapter_{chapter_slot}_{sanitized_manga_title}_{sanitized_chapter_title}.pdf"
    pdf_path = os.path.abspath(os.path.join(output_dir, pdf_filename))
    
    if os.path.exists(pdf_path):
        os.remove(pdf_path)

    temp_dir = os.path.abspath(os.path.join(output_dir, f"temp_{chapter_slot}"))
    os.makedirs(temp_dir, exist_ok=True)

    try:
        valid_files = []
        with tqdm.tqdm(
            total=len(image_urls),
            desc=f"🖼️ Ch-{chapter_slot}",
            leave=False,
            unit="img",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
        ) as pbar:
            for idx, url in enumerate(image_urls, 1):
                file_path = await async_download_image(session, url, temp_dir, idx)
                if file_path and await verify_image_integrity(file_path):
                    valid_files.append(file_path)
                pbar.update(1)

        if not valid_files:
            logging.error("No valid images available for PDF creation")
            return

        success = await create_pdf_sync(valid_files, pdf_path)
        
        if success:
            logging.info(f"Successfully created PDF: {pdf_path}")
            logging.info(f"PDF contains {len(valid_files)} pages")
        else:
            logging.error("PDF creation failed after image verification")

    except Exception as e:
        logging.error(f"Critical error in PDF creation: {str(e)}")
    finally:
        if not keep_images:
            shutil.rmtree(temp_dir, ignore_errors=True)

def generate_html_index(manga_title, chapters, output_dir):
    """Generate an HTML index file with sorted PDF links."""
    html_path = os.path.join(output_dir, "index.html")
    chapters_sorted = sorted(chapters, key=lambda x: x['slot'])
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{manga_title} - PDF Index</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 20px auto;
            padding: 20px;
            line-height: 1.6;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }}
        .chapter-list {{
            list-style-type: none;
            padding: 0;
        }}
        .chapter-item {{
            margin: 10px 0;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 5px;
            transition: transform 0.2s;
        }}
        .chapter-item:hover {{
            transform: translateX(10px);
            background-color: #e9ecef;
        }}
        .chapter-link {{
            text-decoration: none;
            color: #2980b9;
            font-weight: bold;
        }}
        .chapter-link:hover {{
            color: #3498db;
        }}
        .help-text {{
            color: #7f8c8d;
            margin-top: 30px;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <h1>{manga_title} - Chapters</h1>
    <ol class="chapter-list">
"""

    for chapter in chapters_sorted:
        sanitized_manga_title = sanitize_filename(manga_title)
        sanitized_chapter_title = sanitize_filename(chapter['title'])
        pdf_filename = f"Chapter_{chapter['slot']}_{sanitized_manga_title}_{sanitized_chapter_title}.pdf"
        html_content += f"""
        <li class="chapter-item">
            <a href="{pdf_filename}" class="chapter-link">
                Chapter {chapter['slot']}: {chapter['title']}
            </a>
        </li>
        """

    html_content += f"""
    </ol>
    <div class="help-text">
        <p>Total chapters: {len(chapters_sorted)}</p>
        <p>Click any chapter to open the PDF. Right-click to download.</p>
        <p>Generated at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
</body>
</html>
"""

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info(f"Generated index page: {html_path}")

async def main():
    parser = argparse.ArgumentParser(description='Async Comic Chapter Scraper')
    parser.add_argument('book_id', help='Comic book identifier')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--keep-images', action='store_true', help='Keep downloaded images after creating PDF')
    parser.add_argument('--force', action='store_true', help='Force re-download of existing chapters')
    args = parser.parse_args()

    # Configure root logger early
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler (stderr)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(console_handler)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            title, chapters = await get_content_info(session, args.book_id)
            output_dir = create_output_dir(title, args.book_id)
            
            # Configure file handler (after output dir exists)
            log_file = os.path.join(output_dir, 'scraper.log')
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            root_logger.addHandler(file_handler)
            logging.info(f"Logging to file: {log_file}")

            # Main processing with tqdm integration
            with logging_redirect_tqdm():
                # Content URL fetching
                tasks = [process_chapter(session, args.book_id, ch['slot'], ch['title']) for ch in chapters]
                all_images = await tqdm.asyncio.tqdm.gather(
                    *tasks,
                    desc="📖 Fetching chapter URLs",
                    colour="green",
                    ascii=True  # Better for some terminals
                )

                # PDF creation phase
                pdf_tasks = []
                for chapter, image_urls in zip(chapters, all_images):
                    if not image_urls:
                        continue
                    pdf_tasks.append(
                        download_and_create_pdf(
                            session, output_dir, title,
                            chapter['slot'], chapter['title'],
                            image_urls, args.keep_images
                        )
                    )

                # Use standard tqdm for non-async progress
                with tqdm.tqdm(
                    total=len(pdf_tasks),
                    desc="📚 Creating PDFs",
                    colour="blue",
                    disable=args.debug or not pdf_tasks
                ) as pbar:
                    for coro in asyncio.as_completed(pdf_tasks):
                        await coro
                        pbar.update(1)

            # Generate index after completion
            generate_html_index(title, chapters, output_dir)
            logging.info(f"Processing complete. Open index.html in {output_dir}")

        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise
            

if __name__ == '__main__':
    asyncio.run(main())