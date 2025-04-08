import argparse
import asyncio
import json
import logging
import os
import re
import traceback
import shutil
from json import JSONDecodeError
from urllib.parse import urlparse, parse_qs, urljoin
from PIL import Image
import tempfile

import aiofiles
import aiohttp
import img2pdf
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

async def fetch(session, url, sem):
    async with sem:
        try:
            async with session.get(url, headers=HEADERS) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            return None

async def get_content_info(session, book_id):
    """Fetch comic metadata asynchronously"""
    url = f"https://www.baozimh.com/comic/{book_id}"
    logging.info(f"Fetching content page: {url}")
    
    html = await fetch(session, url, sem=asyncio.Semaphore(5))
    if not html:
        raise ValueError("Failed to fetch content page")

    soup = BeautifulSoup(html, 'html.parser')
    title_tag = soup.find('h1', class_='comics-detail__title')
    if not title_tag:
        raise ValueError("Comic title not found")
    
    chapters = []
    for item in soup.find_all('a', class_='comics-chapters__item'):
        href = item.get('href', '')
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        if slot := params.get('chapter_slot', [None])[0]:
            title = item.find('span').get_text(strip=True) if item.find('span') else ''
            chapters.append({'slot': slot, 'title': title, 'parts': []})

    return title_tag.get_text(strip=True), chapters

async def process_part(session, part_url, expected_slot):
    """Process a single part with concurrency control"""
    current_slot = extract_url_slot(part_url)
    if current_slot != expected_slot:
        logging.warning(f"Slot mismatch: {current_slot} vs {expected_slot}")
        return None

    logging.info(f"Processing part: {part_url}")
    return part_url

async def process_chapter(session, book_id, chapter_slot, chapter_title):
    """Process a chapter with async part handling"""
    base_url = f"https://www.twmanga.com/comic/chapter/{book_id}/0_{chapter_slot}"
    parts = []
    current_url = f"{base_url}.html"
    
    while True:
        part_task = asyncio.create_task(process_part(session, current_url, chapter_slot))
        next_url = await get_next_part(session, current_url)
        
        if part_result := await part_task:
            parts.append(part_result)
        
        if not next_url or extract_url_slot(next_url) != chapter_slot:
            break
        current_url = next_url

    logging.info(f"Chapter {chapter_slot} completed with {len(parts)} parts")
    return parts

async def extract_image_urls(session, part_url):
    """Async image URL extraction with AMP handling"""
    html = await fetch(session, part_url, sem=asyncio.Semaphore(10))
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    image_container = soup.find('ul', class_='comic-contain')
    if not image_container:
        return []

    image_urls = []
    for amp_img in image_container.find_all('amp-img'):
        # JSON data extraction
        if script_tag := amp_img.find_next('amp-state'):
            try:
                json_data = json.loads(script_tag.script.string)
                if url := json_data.get('url'):
                    image_urls.append(urljoin(part_url, url))
                    continue
            except (JSONDecodeError, AttributeError):
                pass

        # Attribute fallback
        for attr in ['data-src', 'src']:
            if url := amp_img.get(attr):
                image_urls.append(urljoin(part_url, url))
                break

    # Deduplicate while preserving order
    seen = set()
    return [url for url in image_urls if url.endswith('.jpg') and not (url in seen or seen.add(url))]

def write_image_links_log(output_dir, chapter_info, parts, image_urls):
    """Write image links to a text file with chapter context"""
    log_path = os.path.join(output_dir, 'image_links.txt')
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"\n\nChapter {chapter_info['slot']}: {chapter_info['title']}\n")
            f.write(f"Total Parts: {len(parts)}\n")
            
            for part_idx, part_url in enumerate(parts, 1):
                f.write(f"\nPart {part_idx}: {part_url}\n")
                part_images = image_urls.get(part_url, [])
                for img_idx, url in enumerate(part_images, 1):
                    f.write(f"  Image {img_idx}: {url}\n")
                if not part_images:
                    f.write("  No images found for this part\n")
                    
    except Exception as e:
        logging.error(f"Failed to write image links log: {e}")

async def download_image(session, url, save_path, referer, sem):
    """Enhanced image download with detailed error reporting"""
    async with sem:
        try:
            headers = HEADERS.copy()
            headers['Referer'] = referer
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    async with aiofiles.open(save_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    return True
                else:
                    logging.error(
                        f"Download failed: {url} | "
                        f"Status: {response.status} | "
                        f"Referer: {referer}"
                    )
                    return False
        except Exception as e:
            logging.error(
                f"Download error: {url} | "
                f"Error Type: {type(e).__name__} | "
                f"Message: {str(e)} | "
                f"Referer: {referer}"
            )
            return False
        
async def process_chapter_images(session, output_dir, book_id, chapter):
    """Enhanced image processing with PDF generation safeguards"""
    chapter_dir = os.path.join(output_dir, f"chapter_{chapter['slot']}")
    os.makedirs(chapter_dir, exist_ok=True)
    
    try:
        # 1. Get chapter parts
        parts = await process_chapter(session, book_id, chapter['slot'], chapter['title'])
        if not parts:
            logging.error(f"🛑 No parts found for chapter {chapter['slot']}")
            return None

        # 2. Track download statistics
        stats = {
            'total_images': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_images': 0,
            'part_details': []
        }

        # 3. Process parts with concurrency control
        sem = asyncio.Semaphore(10)  # Limit concurrent downloads
        image_tasks = []
        
        for part_idx, part_url in enumerate(parts, 1):
            try:
                image_urls = await extract_image_urls(session, part_url)
                stats['total_images'] += len(image_urls)
                
                part_stats = {
                    'url': part_url,
                    'images': len(image_urls),
                    'downloaded': 0
                }
                
                for img_idx, url in enumerate(image_urls, 1):
                    save_path = os.path.join(chapter_dir, f"part{part_idx}_img{img_idx}.jpg")
                    task = download_image(session, url, save_path, part_url, sem)
                    image_tasks.append(task)
                
                stats['part_details'].append(part_stats)
            except Exception as e:
                logging.error(f"Part processing failed: {part_url} | Error: {str(e)}")

        # 4. Process all downloads
        results = await asyncio.gather(*image_tasks, return_exceptions=True)
        
        # 5. Analyze results
        success_count = 0
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Download failed: {image_tasks[idx]} | Error: {str(result)}")
                stats['failed_downloads'] += 1
            elif result:
                success_count += 1
                stats['successful_downloads'] += 1
            else:
                stats['failed_downloads'] += 1

        # 6. Validate downloaded images
        valid_images = []
        for root, _, files in os.walk(chapter_dir):
            for file in files:
                if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                    img_path = os.path.join(root, file)
                    try:
                        with Image.open(img_path) as img:
                            if img.width >= 100 and img.height >= 100:
                                valid_images.append(img_path)
                            else:
                                stats['skipped_images'] += 1
                                os.remove(img_path)
                    except Exception as e:
                        logging.error(f"Invalid image: {img_path} | Error: {str(e)}")
                        os.remove(img_path)

        # 7. Generate PDF if valid images exist
        if valid_images:
            sanitized_title = sanitize_filename(chapter['title'])
            pdf_path = os.path.join(output_dir, f"chapter_{chapter['slot']}_{sanitized_title}.pdf")
            
            # Convert in thread pool
            loop = asyncio.get_running_loop()
            pdf_result = await loop.run_in_executor(
                None, 
                lambda: convert_images_to_pdf(valid_images, pdf_path)
            )
            
            if pdf_result:
                logging.info(f"✅ Successfully generated PDF: {pdf_path}")
                return pdf_path
            else:
                logging.error(f"❌ PDF generation failed for chapter {chapter['slot']}")

        # 8. Generate detailed report
        report = (
            f"\n📊 Chapter {chapter['slot']} Report:\n"
            f"Total Parts: {len(parts)}\n"
            f"Total Images Attempted: {stats['total_images']}\n"
            f"Successfully Downloaded: {stats['successful_downloads']}\n"
            f"Failed Downloads: {stats['failed_downloads']}\n"
            f"Skipped Images: {stats['skipped_images']}\n"
            f"Valid Images for PDF: {len(valid_images)}\n"
        )
        logging.info(report)
        
        return None

    except Exception as e:
        logging.error(f"🔥 Critical error processing chapter {chapter['slot']}: {str(e)}")
        return None
    finally:
        # Cleanup temporary files
        if os.path.exists(chapter_dir):
            shutil.rmtree(chapter_dir, ignore_errors=True)
                
def create_output_dir(manga_title, book_id):
    """Create sanitized output directory"""
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', manga_title)
    dir_name = f"{sanitized}_{book_id}"
    os.makedirs(dir_name, exist_ok=True)
    return dir_name

def sanitize_filename(name):
    """Sanitize filenames for safe storage"""
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

def convert_images_to_pdf(image_paths, output_path):
    """PDF conversion with enhanced error info"""
    valid_images = []
    size_errors = []
    
    for img_path in sorted(image_paths):
        try:
            with Image.open(img_path) as img:
                if img.width < 100 or img.height < 100:
                    size_errors.append(f"{img_path} - Size: {img.size}")
                    continue
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp:
                    img.convert('RGB').save(tmp.name, quality=95)
                    valid_images.append(tmp.name)
        except Exception as e:
            logging.error(
                f"Invalid image: {img_path} | "
                f"Error Type: {type(e).__name__} | "
                f"Message: {str(e)}"
            )
            continue

    if size_errors:
        logging.warning(f"Skipped {len(size_errors)} small images:")
        for err in size_errors:
            logging.warning(f"  {err}")

    if not valid_images:
        logging.error("PDF conversion aborted: No valid images")
        return False

    try:
        layout = img2pdf.get_layout_fun(
            pagesize=(img2pdf.mm_to_pt(210), img2pdf.mm_to_pt(297)),
            fit=img2pdf.FitMode.into
        )
        
        with open(output_path, "wb") as f:
            f.write(img2pdf.convert(valid_images, layout_fun=layout))
            
        logging.info(f"PDF created successfully: {output_path}")
        return True
        
    except Exception as e:
        logging.error(
            f"PDF conversion failed: {output_path} | "
            f"Error Type: {type(e).__name__} | "
            f"Message: {str(e)} | "
            f"Images: {len(valid_images)}"
        )
        return False
        
    finally:
        for tmp in valid_images:
            try: os.remove(tmp)
            except Exception as e:
                logging.warning(f"Temp file cleanup failed: {tmp} | Error: {e}")

def generate_index_html(output_dir, pdf_files, manga_title):
    """Generate ordered HTML index file"""
    html_content = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{manga_title}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 2rem; }}
            h1 {{ color: #333; border-bottom: 2px solid #eee; }}
            ol {{ list-style-type: none; padding: 0; }}
            li {{ margin: 0.8rem 0; }}
            a {{ text-decoration: none; color: #0066cc; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <h1>{manga_title}</h1>
        <ol>
    """
    
    # Ensure files are listed in chapter order
    for pdf in pdf_files:
        html_content += f'<li><a href="{pdf["filename"]}">{pdf["title"]}</a></li>\n'
    
    html_content += """</ol>
    </body>
    </html>"""
    
    index_path = os.path.join(output_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info(f"Generated index with {len(pdf_files)} chapters at {index_path}")

async def get_next_part(session, current_url):
    """Improved next part detection with enhanced parsing"""
    logging.debug(f"Analyzing navigation at: {current_url}")
    
    try:
        html = await fetch(session, current_url, sem=asyncio.Semaphore(5))
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check both current and common navigation patterns
        next_link = None

        # Find all navigation links
        nav_div_all = soup.find_all('div', class_='next_chapter')
        if not nav_div_all:
            logging.debug("❌ No navigation div found")
            return None

        candidates = []
        for nav_div in nav_div_all:
            for a_tag in nav_div.find_all('a'):
                raw_url = a_tag.get('href', '')
                clean_url = urljoin(current_url, raw_url.split('#')[0])
                link_text = a_tag.get_text(strip=True)
                
                logging.debug(f"🔗 Found link: {clean_url}")
                logging.debug(f"   Text: {link_text}")
                
                if clean_url != current_url:
                    candidates.append({
                        'url': clean_url,
                        'text': link_text.lower()
                    })

        # Multi-criteria validation
        current_part = extract_part_number(current_url)
        logging.debug(f"Current part: {current_part}")

        for candidate in candidates:
            url = candidate['url']
            candidate_part = extract_part_number(url)
            
            # 1. Check part number sequence
            if candidate_part == current_part + 1:
                logging.info(f"✅ Found next part via sequence: {url}")
                next_link = url
                break
                
            # 2. Check Chinese keywords
            if any(keyword in candidate['text'] for keyword in ['下一頁', '下一章', '下一页']):
                logging.info(f"✅ Found next part via Chinese text: {url}")
                next_link = url
                break
                
            # 3. Check English keywords
            if any(keyword in candidate['text'] for keyword in ['next', 'continue']):
                logging.info(f"✅ Found next part via English text: {url}")
                next_link = url
                break
        
        # # Pattern 1: Direct sibling chapter links
        # nav_container = soup.find('div', class_='navi-change-chapter')
        # if nav_container:
        #     for link in nav_container.find_all('a', class_='navi-change-chapter-next'):
        #         next_href = link.get('href')
        #         if next_href:
        #             next_link = urljoin(current_url, next_href)
        #             break
        
        # # Pattern 2: Pagination controls
        # if not next_link:
        #     pagination = soup.find('ul', class_='pagination')
        #     if pagination:
        #         current_page = pagination.find('li', class_='active')
        #         if current_page:
        #             next_page = current_page.find_next_sibling('li')
        #             if next_page:
        #                 next_link_tag = next_page.find('a')
        #                 if next_link_tag:
        #                     next_link = urljoin(current_url, next_link_tag.get('href'))
        
        # # Pattern 3: Increment part number in URL (fallback)
        # if not next_link:
        #     current_part = extract_part_number(current_url)
        #     if current_part is not None:
        #         next_part = current_part + 1
        #         next_link = re.sub(r'_(\d+)\.html$', f'_{next_part}.html', current_url)
                
        #         # Verify the new URL actually exists
        #         async with session.head(next_link, allow_redirects=False) as resp:
        #             if resp.status == 200:
        #                 return next_link
        #             else:
        #                 next_link = None

        # # New Pattern 4: Check for 點擊進入下一頁 text
        # if not next_link:
        #     next_link_tag = soup.find('a', text=lambda t: t and '點擊進入下一頁' in t.strip())
        #     if next_link_tag:
        #         next_href = next_link_tag.get('href')
        #         if next_href:
        #             next_link = urljoin(current_url, next_href)
        print("*****Links :", current_url, next_link)
        # Final validation
        if next_link and next_link != current_url:
            parsed_next = urlparse(next_link)
            parsed_current = urlparse(current_url)
            
            # Ensure same chapter slot
            if extract_url_slot(next_link) == extract_url_slot(current_url):
                logging.debug(f"Found valid next part: {next_link}")
                return next_link
        
        return None

    except Exception as e:
        logging.error(f"Error finding next part: {e}")
        return None
    
def extract_part_number(url):
    """Improved part number extraction"""
    try:
        # Match URLs ending with _X.html where X is the part number
        match = re.search(r'_(\d+)\.html$', url)
        return int(match.group(1)) if match else 1
    except:
        return 1
    
# Add these helper functions if missing
def extract_url_slot(url):
    """Extract chapter slot from URL"""
    try:
        path = urlparse(url).path
        return path.split('_')[-1].split('.')[0]
    except:
        return None
           
async def main():
    """Async main entry point"""
    parser = argparse.ArgumentParser(description='Async Comic Scraper')
    parser.add_argument('book_id', help='Comic book identifier')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    async with aiohttp.ClientSession() as session:
        try:
            title, chapters = await get_content_info(session, args.book_id)
            output_dir = create_output_dir(title, args.book_id)
            
            # Create tasks while preserving order
            tasks = [
                process_chapter_images(session, output_dir, args.book_id, ch)
                for ch in chapters
            ]
            
            # Process chapters in order with limited concurrency
            pdf_results = []
            for i in range(0, len(tasks), 5):  # Process 5 chapters at a time
                batch = tasks[i:i+5]
                batch_results = await asyncio.gather(*batch)
                pdf_results.extend(batch_results)

            # Generate index with proper ordering
            valid_pdfs = []
            for chapter, result in zip(chapters, pdf_results):
                if result:
                    sanitized_title = sanitize_filename(chapter['title'])
                    display_title = f"Chapter {chapter['slot']} - {sanitized_title}"
                    valid_pdfs.append({
                        'title': display_title,
                        'filename': os.path.basename(result)
                    })

            generate_index_html(output_dir, valid_pdfs, title)

        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise

if __name__ == '__main__':
    asyncio.run(main())