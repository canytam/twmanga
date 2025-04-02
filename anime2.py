import sys
import os
import re
import tempfile
import asyncio
import aiohttp
import img2pdf
import logging
from datetime import datetime
from tqdm.asyncio import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin
from PIL import Image
from typing import List, Dict, Tuple

# Configuration
MAX_CONCURRENT_CHAPTERS = 3
MAX_CONCURRENT_IMAGES = 10
MAX_PARTS_PER_CHAPTER = 10

def sanitize_filename(name: str) -> str:
    """Sanitize filename with length limit"""
    safe = re.sub(r'[^\w\-_（）【】\u4e00-\u9fff]', '_', name)
    return safe.strip('_')[:45]

def setup_logging(folder_name: str) -> str:
    """Configure logging system"""
    os.makedirs(folder_name, exist_ok=True)
    log_file = os.path.join(folder_name, f"process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch webpage content with error handling"""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        raise

async def download_image(session: aiohttp.ClientSession, url: str, img_path: str, 
                       semaphore: asyncio.Semaphore, pbar: tqdm, chapter_desc: str) -> str:
    """Download and validate single image"""
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

def generate_next_part_url(url: str, current_part: int) -> str:
    """Generate next part URL with numeric suffix"""
    base, ext = os.path.splitext(url)
    match = re.search(r'_(\d+)$', base)
    if match:
        new_base = re.sub(r'_(\d+)$', f'_{current_part}', base)
    else:
        new_base = f"{base}_{current_part}"
    return f"{new_base}{ext}"

async def process_chapter(session: aiohttp.ClientSession, chapter: Dict, 
                        folder_name: str, image_semaphore: asyncio.Semaphore,
                        book_title: str) -> Tuple[List[str], List[str]]:
    """Process a chapter with multiple parts"""
    logger = logging.getLogger()
    safe_title = sanitize_filename(book_title)
    chapter_desc = chapter['description']
    
    generated_pdfs = []
    source_urls = []
    current_url = chapter['link']
    part_number = 1
    
    while current_url and part_number <= MAX_PARTS_PER_CHAPTER:
        source_urls.append(current_url)
        pdf_name = f"{safe_title}_{sanitize_filename(chapter_desc)}_{part_number:02d}.pdf"
        pdf_path = os.path.join(folder_name, pdf_name)
        
        if os.path.exists(pdf_path):
            logger.info(f"[{chapter_desc} Part {part_number}] PDF exists, skipping")
            generated_pdfs.append(pdf_name)
            part_number += 1
            continue
        
        try:
            logger.info(f"[{chapter_desc} Part {part_number}] Processing {current_url}")
            html = await fetch_page(session, current_url)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract image URLs
            image_urls = []
            for tag in soup.find_all(['a', 'img']):
                url = tag.get('href') or tag.get('src')
                if url:
                    absolute_url = urljoin(current_url, url)
                    if urlparse(absolute_url).path.lower().endswith(('.jpg', '.jpeg')):
                        image_urls.append(absolute_url)
            
            if not image_urls:
                logger.warning(f"[{chapter_desc} Part {part_number}] No images found")
                part_number += 1
                continue
            
            # Download images
            with tempfile.TemporaryDirectory() as tmpdir:
                images = []
                with tqdm(total=len(image_urls), 
                         desc=f"{chapter_desc[:15]} P{part_number}") as pbar:
                    tasks = []
                    for idx, url in enumerate(image_urls):
                        img_name = f"{idx:04d}.jpg"
                        img_path = os.path.join(tmpdir, img_name)
                        tasks.append(
                            download_image(session, url, img_path, 
                                          image_semaphore, pbar, chapter_desc)
                        )
                    
                    results = await asyncio.gather(*tasks)
                    images = [img for img in results if img is not None]
                    images.sort()
                
                if not images:
                    logger.error(f"[{chapter_desc} Part {part_number}] No valid images")
                    part_number += 1
                    continue
                
                # Create PDF
                try:
                    with open(pdf_path, "wb") as f:
                        f.write(img2pdf.convert(images))
                    logger.info(f"[{chapter_desc} Part {part_number}] Created {pdf_name}")
                    generated_pdfs.append(pdf_name)
                except Exception as e:
                    logger.error(f"[{chapter_desc} Part {part_number}] PDF failed: {str(e)}")
                    if os.path.exists(pdf_path):
                        os.remove(pdf_path)
        
        except Exception as e:
            logger.error(f"[{chapter_desc} Part {part_number}] Processing failed: {str(e)}")
        
        part_number += 1
        current_url = generate_next_part_url(chapter['link'], part_number)
    
    return generated_pdfs, source_urls

async def main_async(book_id: str):
    """Main processing workflow"""
    base_url = f"https://www.twmanga.com/comic/{book_id}"
    all_chapter_data = []
    
    try:
        async with aiohttp.ClientSession() as session:
            # Fetch book metadata
            html = await fetch_page(session, base_url)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_div = soup.find('div', class_='comics-detail__info')
            title = title_div.find('h1').get_text().strip() if title_div else "Unknown"
            safe_title = sanitize_filename(title)
            
            # Create output folder
            folder_name = f"{safe_title}_{book_id}_output"
            os.makedirs(folder_name, exist_ok=True)
            log_file = setup_logging(folder_name)
            
            logging.info(f"Starting processing: {title} (ID: {book_id})")
            logging.info(f"Output folder: {os.path.abspath(folder_name)}")

            # Extract chapters
            chapters = []
            for a_tag in soup.find_all('a'):
                href = a_tag.get('href')
                if href and href.startswith("/user/page_direct"):
                    params = parse_qs(urlparse(href).query)
                    try:
                        chapters.append({
                            'section': int(params.get('section_slot', [0])[0]),
                            'chapter': int(params.get('chapter_slot', [0])[0]),
                            'description': a_tag.get_text().strip(),
                            'link': urljoin(base_url, href)
                        })
                    except (KeyError, ValueError) as e:
                        logging.warning(f"Skipping invalid chapter: {str(e)}")
                        continue

            # Sort chapters
            sorted_chapters = sorted(chapters, key=lambda x: (x['section'], x['chapter']))
            if not sorted_chapters:
                logging.error("No chapters found")
                return

            logging.info(f"Found {len(sorted_chapters)} chapters")

            # Process chapters with concurrency control
            image_semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAGES)
            chapter_queue = asyncio.Queue()
            for chap in sorted_chapters:
                await chapter_queue.put(chap)

            async def worker():
                while True:
                    try:
                        chapter = await chapter_queue.get()
                        pdfs, urls = await process_chapter(session, chapter, folder_name, 
                                                         image_semaphore, title)
                        all_chapter_data.append({
                            'description': chapter['description'],
                            'pdfs': pdfs,
                            'source_urls': urls
                        })
                    except Exception as e:
                        logging.error(f"Worker error: {str(e)}")
                    finally:
                        chapter_queue.task_done()

            # Create worker tasks
            tasks = []
            for _ in range(MAX_CONCURRENT_CHAPTERS):
                task = asyncio.create_task(worker())
                tasks.append(task)

            await chapter_queue.join()
            
            # Cancel workers
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # Generate comprehensive log
            if all_chapter_data:
                log_path = os.path.join(folder_name, "chapter_source_log.txt")
                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write(f"Book: {title}\n")
                    f.write(f"ID: {book_id}\n")
                    f.write(f"Processed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    for chap_data in all_chapter_data:
                        f.write(f"=== Chapter: {chap_data['description']} ===\n")
                        f.write("Source URLs:\n")
                        for url in chap_data['source_urls']:
                            f.write(f"  - {url}\n")
                        
                        f.write("\nGenerated PDFs:\n")
                        for pdf in chap_data['pdfs']:
                            f.write(f"  - {pdf}\n")
                        
                        # Calculate missing parts
                        expected = len(chap_data['source_urls'])
                        actual = len(chap_data['pdfs'])
                        missing = expected - actual
                        status = "COMPLETE" if missing == 0 else f"MISSING {missing} PARTS"
                        f.write(f"\nStatus: {status}\n\n")
                
                logging.info(f"Created detailed source log: {log_path}")

            logging.info("\nProcessing Summary:")
            logging.info(f"Total chapters processed: {len(all_chapter_data)}")
            logging.info(f"Total PDFs generated: {sum(len(c['pdfs']) for c in all_chapter_data)}")

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