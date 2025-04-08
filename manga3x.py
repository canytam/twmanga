import argparse
import asyncio
import logging
import shutil
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuration constants
CONCURRENCY_LIMIT = 10
IMAGE_CONCURRENCY_LIMIT = 20
PDF_PROCESSOR_WORKERS = 4
MAX_MEMORY_PERCENT = 85
MEMORY_CHECK_INTERVAL = 15
TEMP_DIR = Path("temp_images")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate, br'
}

class MemoryAwareExecutor:
    def __init__(self):
        self.pause_event = asyncio.Event()
        self.pause_event.set()
        self.active_tasks = 0

    async def memory_monitor(self):
        """Monitor system memory and pause operations when thresholds are exceeded"""
        try:
            import psutil
            while True:
                mem = psutil.virtual_memory()
                if mem.percent >= MAX_MEMORY_PERCENT:
                    logging.warning(f"High memory usage ({mem.percent}%), pausing tasks")
                    self.pause_event.clear()
                    while mem.percent > MAX_MEMORY_PERCENT - 5:
                        await asyncio.sleep(1)
                        mem = psutil.virtual_memory()
                    self.pause_event.set()
                    logging.info("Memory normalized, resuming operations")
                await asyncio.sleep(MEMORY_CHECK_INTERVAL)
        except ImportError:
            logging.warning("psutil not installed, memory monitoring disabled")
            await asyncio.sleep(0)

async def main():
    parser = argparse.ArgumentParser(description='Memory-Aware Comic Downloader')
    parser.add_argument('book_id', help='Comic book identifier')
    parser.add_argument('--force', action='store_true', help='Force re-download existing content')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize memory-aware executor
    executor = MemoryAwareExecutor()
    
    try:
        # Create necessary directories
        output_dir = Path(create_output_dir("comic", args.book_id))
        output_dir.mkdir(parents=True, exist_ok=True)
        TEMP_DIR.mkdir(parents=True, exist_ok=True)

        async with aiohttp.ClientSession(
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
            trust_env=True,
            auto_decompress=True
        ) as session:
            # Start memory monitoring
            mem_task = asyncio.create_task(executor.memory_monitor())

            # Fetch comic metadata
            manga_title, chapters = await get_content_info(session, args.book_id)
            logging.info(f"Processing {len(chapters)} chapters for '{manga_title}'")

            # Create process pool for PDF operations
            with ProcessPoolExecutor(max_workers=PDF_PROCESSOR_WORKERS) as pool:
                tasks = []
                for chapter in chapters:
                    task = process_chapter_wrapper(
                        session, pool, output_dir, chapter,
                        args.book_id, args.force, executor
                    )
                    tasks.append(task)
                    await asyncio.sleep(0.1)  # Prevent immediate memory spikes

                # Process chapters with controlled concurrency
                for i in range(0, len(tasks), CONCURRENCY_LIMIT):
                    batch = tasks[i:i+CONCURRENCY_LIMIT]
                    await asyncio.gather(*batch)
                    await asyncio.sleep(5)  # Allow memory to stabilize between batches

            # Cancel monitoring task
            mem_task.cancel()
            await mem_task

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise
    finally:
        # Cleanup resources
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        logging.info("Temporary files cleaned up")

def create_output_dir(base_name, book_id):
    """Create sanitized output directory with proper translation"""
    # Original problem: 10 characters to replace but only 9 underscores
    # Corrected to use equal length replacement
    translator = str.maketrans(
        ' \\/*?:"<>|',  # 10 invalid characters
        '_'*10          # 10 underscores
    )
    safe_name = f"{base_name}_{book_id}".translate(translator)
    return safe_name
    
async def find_next_url(session, current_url):
    """Find next chapter page URL"""
    try:
        async with session.get(current_url) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')
            for nav in soup.find_all('div', class_='next_chapter'):
                if link := nav.find('a'):
                    return urljoin(current_url, link['href'].split('#')[0])
    except Exception as e:
        logging.debug(f"Next URL detection failed: {str(e)}")
    return None
    
async def process_chapter(session, book_id, chapter):
    """Fetch all image URLs for a chapter"""
    base_url = f"https://www.twmanga.com/comic/chapter/{book_id}/0_{chapter['slot']}"
    image_urls = []
    current_url = f"{base_url}.html"
    
    while True:
        try:
            async with session.get(current_url) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), 'html.parser')

                # Extract images from comic container
                if container := soup.find('ul', class_='comic-contain'):
                    images = container.find_all('img')
                    for img in images:
                        if src := img.get('data-src') or img.get('src'):
                            image_urls.append(urljoin(current_url, src))

                # Find next page
                next_url = await find_next_url(session, current_url)
                if not next_url or urlparse(next_url).path == urlparse(current_url).path:
                    break
                current_url = next_url

        except Exception as e:
            logging.error(f"Failed to process {current_url}: {str(e)}")
            break

    return chapter['slot'], image_urls
async def get_content_info(session, book_id):
    """Fetch comic metadata and chapter list"""
    url = f"https://www.baozimh.com/comic/{book_id}"
    async with session.get(url) as response:
        response.raise_for_status()
        soup = BeautifulSoup(await response.text(), 'html.parser')
        
        title = soup.find('h1', class_='comics-detail__title').text.strip()
        chapters = []
        for item in soup.find_all('a', class_='comics-chapters__item'):
            href = item.get('href')
            # Now parse_qs is properly imported
            params = parse_qs(urlparse(href).query)
            if slot := params.get('chapter_slot', [None])[0]:
                title = item.find('span').text.strip()
                chapters.append({'slot': slot, 'title': title})
        
        return title, chapters

async def process_chapter_wrapper(session, pool, output_dir, chapter, book_id, force, executor):
    """Process a single chapter with memory awareness"""
    await executor.pause_event.wait()
    pdf_path = output_dir / f"chapter_{chapter['slot']}.pdf"
    
    if not force and pdf_path.exists():
        logging.info(f"Skipping existing chapter {chapter['slot']}")
        return

    chapter_dir = TEMP_DIR / f"chap_{chapter['slot']}"
    chapter_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Get image URLs
        _, image_urls = await process_chapter(session, book_id, chapter)
        
        # Download images with memory control
        download_tasks = [
            download_image(session, url, chapter_dir, executor)
            for url in image_urls
        ]
        image_files = await asyncio.gather(*download_tasks)
        valid_files = [f for f in image_files if f]

        # Create PDF in memory-safe batches
        if valid_files:
            batch_size = 10
            for i in range(0, len(valid_files), batch_size):
                await executor.pause_event.wait()
                batch = valid_files[i:i+batch_size]
                part_path = pdf_path.with_suffix(f".part{i//batch_size}.pdf")
                if await create_pdf(pool, batch, part_path):
                    logging.info(f"Created PDF part {i//batch_size} for chapter {chapter['slot']}")

    except Exception as e:
        logging.error(f"Failed processing chapter {chapter['slot']}: {str(e)}")
    finally:
        shutil.rmtree(chapter_dir, ignore_errors=True)

async def download_image(session, url, chapter_dir, executor):
    """Download image with memory-aware streaming"""
    await executor.pause_event.wait()
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            filename = url.split('/')[-1].split('?')[0]
            filepath = chapter_dir / filename
            
            # Stream directly to disk
            async with aiofiles.open(filepath, 'wb') as f:
                async for chunk in response.content.iter_chunked(1024*1024):  # 1MB chunks
                    await f.write(chunk)
                    await executor.pause_event.wait()
            
            if await verify_image(filepath):
                return filepath
            filepath.unlink(missing_ok=True)
            return None
    except Exception as e:
        logging.debug(f"Image download failed: {url} - {str(e)}")
        return None

async def verify_image(filepath):
    """Validate image integrity"""
    try:
        async with aiofiles.open(filepath, 'rb') as f:
            content = await f.read()
            with Image.open(io.BytesIO(content)) as img:
                img.verify()
                return True
    except Exception as e:
        logging.debug(f"Invalid image {filepath}: {str(e)}")
        return False

async def create_pdf(pool, image_paths, output_path):
    """Create PDF using process pool"""
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            pool,
            partial(img2pdf.convert, *image_paths, output=output_path)
        )
        return True
    except Exception as e:
        logging.error(f"PDF creation failed: {str(e)}")
        return False

if __name__ == '__main__':
    asyncio.run(main())
