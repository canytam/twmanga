import sys
import os
import tempfile
import asyncio
import aiohttp
import img2pdf
import logging
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin
from PIL import Image

# Configuration
MAX_CONCURRENT_BOOKS = 5
MAX_CONCURRENT_IMAGES = 10

# Logging setup
def setup_logging(folder_name):
    os.makedirs(folder_name, exist_ok=True)
    log_file = os.path.join(folder_name, f"process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        raise

async def download_image(session, url, img_path, semaphore, pbar, chapter_desc):
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                response.raise_for_status()
                content = await response.read()
                
                with open(img_path, 'wb') as f:
                    f.write(content)
                
                # Validate image
                try:
                    with Image.open(img_path) as img:
                        if img.size[0] < 10 or img.size[1] < 10:
                            raise ValueError("Invalid image dimensions")
                except Exception as e:
                    logging.warning(f"[{chapter_desc}] Invalid image {url}: {str(e)}")
                    os.remove(img_path)
                    return None
                
                pbar.update(1)
                return img_path
        except Exception as e:
            logging.warning(f"[{chapter_desc}] Failed to download {url}: {str(e)}")
            return None

def pdf_layout(img_width, img_height, pdf_page_size):
    """Proper layout function with all required parameters"""
    dpi = 96  # Standard screen DPI
    width_pt = (img_width / dpi) * 72  # Convert pixels to points (1 inch = 72 points)
    height_pt = (img_height / dpi) * 72
    
    # Minimum page size (3x3 inches)
    min_size = 3.0 * 72  # Convert inches to points
    
    # Calculate scaling if needed
    if width_pt < min_size or height_pt < min_size:
        scale = max(min_size/width_pt, min_size/height_pt)
        width_pt *= scale
        height_pt *= scale
    
    # Return format: (image width, image height, page width, page height)
    return (width_pt, height_pt, width_pt, height_pt)

async def process_chapter(session, chapter_data, folder_name, image_semaphore):
    """Process a single chapter and return PDF info with detailed logging"""
    logger = logging.getLogger()
    chapter_desc = chapter_data['description']
    
    # Sanitize filename
    safe_name = "".join(c if c.isalnum() else "_" for c in chapter_desc)
    pdf_name = f"{safe_name}.pdf"
    pdf_path = os.path.join(folder_name, pdf_name)

    try:
        # Skip existing PDFs
        if os.path.exists(pdf_path):
            logger.info(f"[{chapter_desc}] PDF already exists, skipping: {pdf_name}")
            return (chapter_desc, pdf_name)

        logger.info(f"[{chapter_desc}] Starting chapter processing")
        
        # Fetch chapter page
        try:
            html = await fetch_page(session, chapter_data['link'])
        except Exception as e:
            logger.error(f"[{chapter_desc}] Failed to fetch chapter page: {str(e)}")
            return None

        # Extract image URLs
        soup = BeautifulSoup(html, 'html.parser')
        image_urls = []
        for tag in soup.find_all(['a', 'img']):
            url = tag.get('href') or tag.get('src')
            if url:
                absolute_url = urljoin(chapter_data['link'], url)
                if urlparse(absolute_url).path.lower().endswith(('.jpg', '.jpeg')):
                    image_urls.append(absolute_url)
        
        if not image_urls:
            logger.warning(f"[{chapter_desc}] No images found in chapter")
            return None

        # Download images
        with tempfile.TemporaryDirectory() as tmpdir:
            img_files = []
            logger.info(f"[{chapter_desc}] Downloading {len(image_urls)} images")
            
            with tqdm_asyncio(total=len(image_urls), desc=f"Downloading {chapter_desc[:15]}...") as pbar:
                tasks = []
                for idx, url in enumerate(image_urls):
                    img_path = os.path.join(tmpdir, f"page_{idx:04}.jpg")
                    tasks.append(
                        download_image(
                            session, url, img_path,
                            image_semaphore, pbar, chapter_desc
                        )
                    )
                
                results = await asyncio.gather(*tasks)
                img_files = [res for res in results if res is not None]

            # Validate downloads
            failed_count = len(image_urls) - len(img_files)
            if failed_count > 0:
                logger.warning(f"[{chapter_desc}] Failed to download {failed_count}/{len(image_urls)} images")
            
            if not img_files:
                logger.error(f"[{chapter_desc}] No valid images downloaded")
                return None

            # Create PDF
            try:
                logger.info(f"[{chapter_desc}] Creating PDF: {pdf_name}")
                with open(pdf_path, "wb") as f:
                    f.write(img2pdf.convert(
                        [open(img, "rb") for img in sorted(img_files)],
                        layout_fun=pdf_layout  # Directly use the function reference
                    ))
                logger.info(f"[{chapter_desc}] Successfully created PDF")
                return (chapter_desc, pdf_name)
            
            except Exception as e:
                logger.error(f"[{chapter_desc}] PDF creation failed: {str(e)}")
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                return None

    except Exception as e:
        logger.error(f"[{chapter_desc}] Critical processing error: {str(e)}")
        return None
    
async def main_async(book_id):
    base_url = f"https://www.twmanga.com/comic/{book_id}"
    
    try:
        async with aiohttp.ClientSession() as session:
            # Fetch book info
            html = await fetch_page(session, base_url)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Get book title
            title_div = soup.find('div', class_='comics-detail__info')
            if not title_div:
                logging.error("Could not find book title")
                return
            
            title = title_div.find('h1').get_text().strip()
            folder_name = f"{title}_{book_id}"[:100].replace(" ", "_")
            folder_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in folder_name)
            
            # Setup logging
            log_file = setup_logging(folder_name)
            logging.info(f"Starting processing for: {title} (ID: {book_id})")
            logging.info(f"Output folder: {os.path.abspath(folder_name)}")
            logging.info(f"Log file: {log_file}")

            # Find chapters
            chapters = []
            for a_tag in soup.find_all('a'):
                href = a_tag.get('href')
                if href and href.startswith("/user/page_direct"):
                    params = parse_qs(urlparse(href).query)
                    try:
                        chapters.append({
                            'section': int(params['section_slot'][0]),
                            'chapter': int(params['chapter_slot'][0]),
                            'description': a_tag.get_text().strip(),
                            'link': f"https://twmanga.com/comic/chapter/{book_id}/{params['section_slot'][0]}_{params['chapter_slot'][0]}.html"
                        })
                    except (KeyError, ValueError):
                        continue

            # Process chapters in order
            sorted_chapters = sorted(chapters, key=lambda x: (x['section'], x['chapter']))
            image_semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAGES)
            pdf_entries = []
            
            # Process in batches
            total = len(sorted_chapters)
            success = 0
            failed = []
            
            for batch_start in range(0, total, MAX_CONCURRENT_BOOKS):
                batch = sorted_chapters[batch_start:batch_start+MAX_CONCURRENT_BOOKS]
                tasks = [process_chapter(session, ch, folder_name, image_semaphore) for ch in batch]
                results = await asyncio.gather(*tasks)
                
                for ch, result in zip(batch, results):
                    if result:
                        pdf_entries.append(result)
                        success += 1
                    else:
                        failed.append(ch['description'])

            # Generate index
            if pdf_entries:
                index_path = os.path.join(folder_name, "index.html")
                with open(index_path, 'w') as f:
                    f.write(f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title} - Index</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 2rem; }}
        h1 {{ color: #333; border-bottom: 2px solid #eee; }}
        ul {{ list-style: none; padding: 0; }}
        li {{ margin: 1rem 0; }}
        a {{ color: #0066cc; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <ul>
""")
                    for desc, pdf in pdf_entries:
                        f.write(f'        <li><a href="{pdf}">{desc}</a></li>\n')
                    f.write("""    </ul>
</body>
</html>""")
                logging.info(f"Created index with {len(pdf_entries)} entries")

            # Final report
            logging.info("\nPROCESSING SUMMARY:")
            logging.info(f"Total chapters: {total}")
            logging.info(f"Successfully processed: {success}")
            logging.info(f"Failed chapters: {len(failed)}")
            
            if failed:
                logging.warning("Failed chapters:")
                for desc in failed:
                    logging.warning(f" - {desc}")
            
            logging.info(f"Log file saved to: {log_file}")

    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        raise

def main():
    if len(sys.argv) != 2:
        print("Usage: python manga_downloader.py <book_id>")
        sys.exit(1)
    
    book_id = sys.argv[1]
    
    try:
        asyncio.run(main_async(book_id))
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()