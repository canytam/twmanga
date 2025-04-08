import argparse
import asyncio
import datetime
import logging
import os
import shutil
import sys
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import tempfile  # Required for temp directory info
from functools import partial
from urllib.parse import urlparse, parse_qs, urljoin

import aiohttp
import img2pdf
from bs4 import BeautifulSoup
from PIL import Image
from PyPDF2 import PdfMerger

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
IMAGE_CONCURRENCY_LIMIT = 10
image_semaphore = asyncio.Semaphore(IMAGE_CONCURRENCY_LIMIT)


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
            # Get DPI, default to 96 if missing/invalid (matches img2pdf behavior)
            dpi_x, dpi_y = img.info.get('dpi', (96.0, 96.0))
            dpi_x = dpi_x if dpi_x > 0 else 96.0
            dpi_y = dpi_y if dpi_y > 0 else 96.0

            # Calculate dimensions in PDF points
            width_pt = (img.width / dpi_x) * 72
            height_pt = (img.height / dpi_y) * 72
            logging.debug(
                f"Image DPI: ({dpi_x}, {dpi_y}), "
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
                
                # Detect extension from Content-Type
                content_type = response.headers.get('Content-Type', '').lower()
                if 'image/jpeg' in content_type:
                    ext = '.jpg'
                elif 'image/png' in content_type:
                    ext = '.png'
                elif 'image/gif' in content_type:
                    ext = '.gif'
                else:
                    ext = '.bin'  # Fallback extension

                filename = f"image_{idx:03d}{ext}"
                filepath = os.path.join(chapter_dir, filename)
                
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(content)
                
                # Validate dimensions in executor
                loop = asyncio.get_event_loop()
                is_valid = await loop.run_in_executor(
                    None, partial(validate_image_dimensions, filepath))
                
                if not is_valid:  # Now properly indented
                    os.remove(filepath)
                    return None
                
                return filepath
        except Exception as e:
            logging.error(f"Image download failed: {url} - {e}")
            return None


async def create_pdf(image_paths, output_pdf):
    """Create a PDF from a list of image paths."""
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(
            None,
            partial(lambda paths, pdf: img2pdf.convert(*paths, output=pdf), image_paths, output_pdf)
        )
        logging.info(f"PDF created: {output_pdf}")
    except Exception as e:
        logging.error(f"Failed to create PDF {output_pdf}: {e}")
        raise


import sys
import unicodedata

def sanitize_filename(name):
    """Safely sanitize filenames with Unicode support."""
    # Normalize Unicode characters
    name = unicodedata.normalize('NFKC', name)
    # Replace problematic characters
    return "".join([c if c.isalnum() or c in ('_', '-') else '_' for c in name]).strip('_')

import sys
import asyncio
import img2pdf
from PIL import Image

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
            # Re-validate each image right before conversion
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

async def download_and_create_pdf(session, output_dir, chapter_slot, chapter_title, image_urls, keep_images, next_chapter=None):
    """Create PDF with next chapter navigation using proper PDF merging"""
    pdf_filename = f"chapter_{chapter_slot}.pdf"
    pdf_path = os.path.join(output_dir, pdf_filename)
    temp_dir = os.path.join(output_dir, f"temp_{chapter_slot}")
    
    if os.path.exists(pdf_path) and not keep_images:
        return

    os.makedirs(temp_dir, exist_ok=True)
    merger = PdfMerger()

    try:
        # 1. Create main PDF from images
        image_pdf = os.path.join(temp_dir, "content.pdf")
        valid_files = []
        
        # Download and validate images
        download_tasks = [async_download_image(session, url, temp_dir, idx+1)
                        for idx, url in enumerate(image_urls)]
        image_paths = await asyncio.gather(*download_tasks)
        valid_files = [p for p in image_paths if p is not None]

        if valid_files:
            # Create PDF from images
            with open(image_pdf, "wb") as f:
                f.write(img2pdf.convert(valid_files))
            merger.append(image_pdf)
        else:
            logging.warning(f"No valid images for chapter {chapter_slot}")
            return

        # 2. Add navigation page if next chapter exists
        if next_chapter:
            try:
                # Sanitize filename and create proper URI
                next_pdf = f"chapter_{next_chapter['slot']}.pdf"
                safe_uri = urljoin('file://', os.path.abspath(
                    os.path.join(output_dir, next_pdf)
                ).replace(' ', '%20'))

                pdf_path = os.path.join(temp_dir, "nav_next.pdf")
                c = canvas.Canvas(pdf_path, pagesize=letter)
                width, height = letter
        
                # Create visible button
                c.setFillColorRGB(0.13, 0.59, 0.95)  # Blue
                c.roundRect(50, 50, width-100, 50, 10, fill=1)
        
                # Add text
                c.setFillColorRGB(1,1,1)  # White
                c.setFont("Helvetica-Bold", 14)
                text = f"Next Chapter: {next_chapter['title']}"
                c.drawCentredString(width/2, 70, text)
        
                # Add clickable area with encoded URI
                c.linkURL(
                    safe_uri,
                    (50, 50, width-50, 100),
                    relative=0  # Use absolute path for macOS compatibility
                )
                c.save()
        
                valid_files.append(pdf_path)
                logging.debug("Added macOS-compatible navigation page")
            except Exception as e:
                logging.error(f"Navigation page error: {e}")

        # 3. Save merged PDF
        with open(pdf_path, "wb") as f:
            merger.write(f)

    finally:
        merger.close()
        if not keep_images:
            shutil.rmtree(temp_dir, ignore_errors=True)

# Verification steps for environment
logging.info("--- Environment Verification ---")
logging.info(f"Python version: {sys.version}")
logging.info(f"img2pdf version: {img2pdf.__version__}")
logging.info(f"Pillow version: {Image.__version__}")
logging.info(f"Filesystem encoding: {sys.getfilesystemencoding()}")
logging.info(f"Temporary directory: {tempfile.gettempdir()}")


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
        pdf_filename = f"chapter_{chapter['slot']}.pdf"
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

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Debug logging enabled")

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            # Fetch comic information
            title, raw_chapters = await get_content_info(session, args.book_id)
            output_dir = create_output_dir(title, args.book_id)
            
            # Sort chapters numerically by slot
            sorted_chapters = sorted(raw_chapters, key=lambda x: x['slot'])
            
            # Phase 1: Collect image URLs for all chapters
            logging.info("Collecting image URLs for chapters...")
            chapter_tasks = [process_chapter(session, args.book_id, ch['slot'], ch['title'])
                           for ch in sorted_chapters]
            all_images = await asyncio.gather(*chapter_tasks)

            # Create processing queue with next chapter info
            chapter_queue = []
            for i, (chapter, images) in enumerate(zip(sorted_chapters, all_images)):
                next_ch = sorted_chapters[i+1] if i < len(sorted_chapters)-1 else None
                chapter_queue.append({
                    'current': chapter,
                    'images': images,
                    'next': next_ch
                })

            # Phase 2: Process chapters with navigation links
            pdf_tasks = []
            for item in chapter_queue:
                chapter = item['current']
                pdf_path = os.path.join(output_dir, f"chapter_{chapter['slot']}.pdf")
                
                # Skip existing chapters unless forced
                if os.path.exists(pdf_path) and not args.force:
                    logging.info(f"Skipping existing chapter: {chapter['title']}")
                    continue
                    
                if not item['images']:
                    logging.warning(f"No images found for chapter {chapter['slot']}")
                    continue

                task = download_and_create_pdf(
                    session=session,
                    output_dir=output_dir,
                    chapter_slot=chapter['slot'],
                    chapter_title=chapter['title'],
                    image_urls=item['images'],
                    keep_images=args.keep_images,
                    next_chapter=item['next']
                )
                pdf_tasks.append(task)

            # Execute all PDF generation tasks
            await asyncio.gather(*pdf_tasks)

            # Generate final index with all chapter info
            generate_html_index(title, sorted_chapters, output_dir)
            logging.info(f"Processing complete. Open {output_dir}/index.html for navigation")

        except Exception as e:
            logging.error(f"Fatal error: {e}")
            if args.debug:
                import traceback
                traceback.print_exc()
            sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
