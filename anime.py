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

def sanitize_filename(name):
    """Sanitize string for filesystem use"""
    safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
    return safe[:50]  # Truncate to avoid long filenames

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
    dpi = 96
    width_pt = (img_width / dpi) * 72
    height_pt = (img_height / dpi) * 72
    
    min_size = 3.0 * 72
    if width_pt < min_size or height_pt < min_size:
        scale = max(min_size/width_pt, min_size/height_pt)
        width_pt *= scale
        height_pt *= scale
    
    return (width_pt, height_pt, width_pt, height_pt)

def split_into_groups(lst, n_groups):
    if n_groups <= 0:
        return []
    k, m = divmod(len(lst), n_groups)
    return [lst[i*k + min(i, m): (i+1)*k + min(i+1, m)] for i in range(n_groups)]

async def process_pdf_batch(session, group, batch_number, folder_name, image_semaphore, total_batches, title, is_batch_mode):
    logger = logging.getLogger()
    safe_title = sanitize_filename(title)
    
    padding = len(str(total_batches))
    seq_str = f"{batch_number:0{padding}d}"
    if is_batch_mode:
        batch_desc = f"Batch {batch_number}/{total_batches}"
        pdf_name = f"{safe_title}_{seq_str}.pdf"
    else:
        chapter = group[0]
        batch_desc = f"Chapter: {chapter['description']}"
        pdf_name = f"{safe_title}_{seq_str}_{sanitize_filename(chapter['description'])}.pdf"

    pdf_path = os.path.join(folder_name, pdf_name)
    
    # Skip existing PDFs
    if os.path.exists(pdf_path):
        logger.info(f"[{batch_desc}] PDF already exists, skipping: {pdf_name}")
        return (pdf_name, [chap['description'] for chap in group])

    logger.info(f"[{batch_desc}] Starting processing")

    # Collect all image URLs
    chapters_data = []
    total_images = 0
    for chapter in group:
        try:
            html = await fetch_page(session, chapter['link'])
            soup = BeautifulSoup(html, 'html.parser')
            image_urls = []
            for tag in soup.find_all(['a', 'img']):
                url = tag.get('href') or tag.get('src')
                if url:
                    absolute_url = urljoin(chapter['link'], url)
                    if urlparse(absolute_url).path.lower().endswith(('.jpg', '.jpeg')):
                        image_urls.append(absolute_url)
            if not image_urls:
                logger.warning(f"[{batch_desc}] No images found in chapter")
                continue
            chapters_data.append({
                'desc': chapter['description'],
                'urls': image_urls
            })
            total_images += len(image_urls)
        except Exception as e:
            logger.error(f"[{batch_desc}] Failed to process chapter: {str(e)}")
            continue

    if total_images == 0:
        logger.warning(f"[{batch_desc}] No images found")
        return None

    # Download images
    with tempfile.TemporaryDirectory() as tmpdir:
        all_images = []
        with tqdm_asyncio(total=total_images, desc=batch_desc[:20]) as pbar:
            tasks = []
            for chap_data in chapters_data:
                chapter_dir = os.path.join(tmpdir, chap_data['desc'])
                os.makedirs(chapter_dir, exist_ok=True)
                for idx, url in enumerate(chap_data['urls']):
                    img_path = os.path.join(chapter_dir, f"page_{idx:04}.jpg")
                    tasks.append(
                        download_image(session, url, img_path, image_semaphore, pbar, chap_data['desc'])
                    )
            
            results = await asyncio.gather(*tasks)
            successful_images = [res for res in results if res is not None]
            successful_images.sort(key=lambda x: (os.path.basename(os.path.dirname(x)), x))
            all_images = successful_images

        if not all_images:
            logger.error(f"[{batch_desc}] No images downloaded")
            return None

        # Create PDF
        try:
            logger.info(f"[{batch_desc}] Creating PDF with {len(all_images)} images")
            with open(pdf_path, "wb") as f:
                f.write(img2pdf.convert(all_images, layout_fun=pdf_layout))
            logger.info(f"[{batch_desc}] Successfully created PDF: {pdf_name}")
            return (pdf_name, [chap['description'] for chap in group])
        except Exception as e:
            logger.error(f"[{batch_desc}] PDF creation failed: {str(e)}")
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            return None

async def main_async(book_id, num_pdfs=None):
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
            safe_title = sanitize_filename(title)
            
            # Determine operation mode
            is_batch_mode = num_pdfs is not None
            if is_batch_mode:
                folder_name = f"{safe_title}_{book_id}_{num_pdfs}PDFs"
            else:
                folder_name = f"{safe_title}_{book_id}_chapters"
            folder_name = folder_name[:100]

            # Setup logging
            log_file = setup_logging(folder_name)
            logging.info(f"Starting processing for: {title} (ID: {book_id})")
            logging.info(f"Mode: {'Batch' if is_batch_mode else 'Chapter-per-PDF'}")
            logging.info(f"Output folder: {os.path.abspath(folder_name)}")

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

            # Sort chapters
            sorted_chapters = sorted(chapters, key=lambda x: (x['section'], x['chapter']))
            if not sorted_chapters:
                logging.error("No chapters found")
                return

            # Determine grouping
            if is_batch_mode:
                groups = split_into_groups(sorted_chapters, num_pdfs)
            else:
                # One PDF per chapter
                groups = [[ch] for ch in sorted_chapters]
                num_pdfs = len(groups)

            total_batches = len(groups)
            logging.info(f"Total chapters: {len(sorted_chapters)}")
            logging.info(f"Total PDFs to create: {total_batches}")

            # Process groups
            image_semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAGES)
            pdf_entries = []
            failed_batches = []

            for batch_start in range(0, total_batches, MAX_CONCURRENT_BOOKS):
                current_groups = groups[batch_start:batch_start + MAX_CONCURRENT_BOOKS]
                tasks = [
                    process_pdf_batch(
                        session, group, idx+1,
                        folder_name, image_semaphore,
                        total_batches, title, is_batch_mode
                    )
                    for idx, group in enumerate(current_groups, start=batch_start)
                ]
                results = await asyncio.gather(*tasks)
                
                for result in results:
                    if result:
                        pdf_entries.append(result)
                    else:
                        failed_batches.append(len(pdf_entries) + 1)

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
        .chapters {{ font-size: 0.9em; color: #666; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <ul>
""")
                    for pdf_name, chapters_in_pdf in pdf_entries:
                        chapter_list = ", ".join(chapters_in_pdf)
                        f.write(f'        <li><a href="{pdf_name}" target="_blank">{pdf_name}</a><div class="chapters">{chapter_list}</div></li>\n')
                    f.write("""    </ul>
</body>
</html>""")
                logging.info(f"Created index with {len(pdf_entries)} entries")

            # Final report
            logging.info("\nPROCESSING SUMMARY:")
            logging.info(f"Total chapters: {len(sorted_chapters)}")
            logging.info(f"Successfully created PDFs: {len(pdf_entries)}")
            logging.info(f"Failed batches: {len(failed_batches)}")
            if failed_batches:
                logging.warning("Failed batch numbers: " + ", ".join(map(str, failed_batches)))

    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        raise

def main():
    if len(sys.argv) < 2:
        print("Usage: python manga_downloader.py <book_id> [num_pdfs]")
        print("Examples:")
        print("  Single PDF per chapter: python manga_downloader.py 123")
        print("  Combine into 5 PDFs:    python manga_downloader.py 123 5")
        sys.exit(1)
    
    book_id = sys.argv[1]
    num_pdfs = None
    
    if len(sys.argv) >= 3:
        try:
            num_pdfs = int(sys.argv[2])
            if num_pdfs < 1:
                raise ValueError
        except ValueError:
            print("Error: num_pdfs must be a positive integer")
            sys.exit(1)
    
    try:
        asyncio.run(main_async(book_id, num_pdfs))
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)

if __name__ == "__main__":
    main()
