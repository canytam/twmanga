import argparse
import logging
import os
import re
import shutil
import time
import json
from json import JSONDecodeError
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs, urljoin

import img2pdf
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Set headers to mimic browser request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/91.0.4472.124 Safari/537.36'
}


def get_content_info(book_id):
    """Fetch and parse comic content page information."""
    url = f"https://www.baozimh.com/comic/{book_id}"
    logging.info(f"Fetching content page: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch content page: {e}")
        raise

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract manga title
    title_tag = soup.find('h1', class_='comics-detail__title')
    if not title_tag:
        raise ValueError("Comic title not found")
    manga_title = title_tag.get_text(strip=True)

    # Extract chapters list
    chapters = []
    for item in soup.find_all('a', class_='comics-chapters__item'):
        # Extract chapter slot from URL parameters
        href = item.get('href', '')
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        slot = params.get('chapter_slot', [None])[0]

        # Extract chapter title
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
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', manga_title)
    dir_name = f"{sanitized}_{book_id}"
    os.makedirs(dir_name, exist_ok=True)
    return dir_name


def extract_url_slot(url):
    """Extract chapter slot from chapter URL with debug logging."""
    try:
        path = urlparse(url).path
        filename = path.split('/')[-1]
        base = filename.split('.')[0]
        parts = base.split('_')
        slot = parts[1] if len(parts) > 1 else None
        
        logging.debug(f"URL Parsing Debug: {url}")
        logging.debug(f"  - Path: {path}")
        logging.debug(f"  - Filename: {filename}")
        logging.debug(f"  - Base: {base}")
        logging.debug(f"  - Parts: {parts}")
        logging.debug(f"  - Extracted slot: {slot}")
        
        return slot
    except Exception as e:
        logging.error(f"Error parsing URL slot: {e}")
        return None


def extract_part_number(url):
    """Extract part number from URL with default to 1 if not present."""
    try:
        path = urlparse(url).path
        filename = path.split('/')[-1]
        base = filename.split('.')[0]
        parts = base.split('_')
        
        # Default to part 1 if no part number in URL
        return int(parts[2]) if len(parts) >= 3 else 1
    except (IndexError, ValueError, AttributeError) as e:
        logging.error(f"Error extracting part number from {url}: {e}")
        return None


def get_next_part(current_url):
    """Improved next part detection with multi-link handling"""
    logging.debug(f"🔍 Analyzing navigation at: {current_url}")
    
    try:
        response = requests.get(current_url, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Capture raw HTML for debugging
    nav_html = str(soup.find('div', class_='next_chapter'))
    logging.debug(f"Navigation HTML:\n{nav_html[:500]}...")

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
            return url
            
        # 2. Check Chinese keywords
        if any(keyword in candidate['text'] for keyword in ['下一頁', '下一章', '下一页']):
            logging.info(f"✅ Found next part via Chinese text: {url}")
            return url
            
        # 3. Check English keywords
        if any(keyword in candidate['text'] for keyword in ['next', 'continue']):
            logging.info(f"✅ Found next part via English text: {url}")
            return url

    logging.debug("🚫 No valid next part found")
    return None


def process_chapter(book_id, chapter_slot, chapter_title):
    """Process chapter with enhanced part number validation."""
    base_url = f"https://www.twmanga.com/comic/chapter/{book_id}/0_{chapter_slot}"
    parts = []
    current_url = f"{base_url}.html"
    expected_slot = chapter_slot

    time.sleep(1)

    logging.info(f"📂 Starting chapter {chapter_slot} at {current_url}")
    current_part = extract_part_number(current_url)

    while True:
        # Verify current URL belongs to this chapter
        current_slot = extract_url_slot(current_url)
        if current_slot != expected_slot:
            logging.warning(f"Slot mismatch! Current: {current_slot}, Expected: {expected_slot}")
            break

        parts.append(current_url)
        logging.info(f"📑 Added part {len(parts)}: {current_url}")

        # Get next part URL
        next_url = get_next_part(current_url)
        if not next_url:
            logging.debug("No more parts found")
            break

        # Verify next URL properties
        next_slot = extract_url_slot(next_url)
        next_part = extract_part_number(next_url)
        
        if next_slot != expected_slot:
            logging.debug(f"Next URL slot mismatch: {next_slot} vs {expected_slot}")
            break
            
        if next_part != extract_part_number(current_url) + 1:
            logging.warning(f"Invalid part sequence: {next_part} after {extract_part_number(current_url)}")
            break

        current_url = next_url

    logging.info(f"📦 Chapter {chapter_slot} completed with {len(parts)} parts")
    return parts


def extract_image_urls(part_url):
    """Extract image URLs from AMP-powered comic-contain list"""
    logging.info(f"Extracting images from {part_url}")
    try:
        response = requests.get(part_url, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch part page {part_url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    
    image_container = soup.find('ul', class_='comic-contain')
    if not image_container:
        logging.error("comic-contain container not found")
        return []

    image_urls = []
    
    for amp_img in image_container.find_all('amp-img'):
        # Attempt to get JSON data first
        script_tag = amp_img.find_next('amp-state')
        if script_tag and script_tag.script:
            try:
                json_data = json.loads(script_tag.script.string)
                if 'url' in json_data:
                    image_url = urljoin(part_url, json_data['url'])
                    if image_url.lower().endswith('.jpg'):
                        image_urls.append(image_url)
                        continue
            except (JSONDecodeError, AttributeError) as e:
                logging.warning(f"JSON parsing failed: {e}")

        # Fallback to data-src/src attributes
        for attr in ['data-src', 'src']:
            if amp_img.has_attr(attr):
                image_url = urljoin(part_url, amp_img[attr])
                if image_url.lower().endswith('.jpg'):
                    image_urls.append(image_url)
                    break

    # Remove duplicates while preserving order
    seen = set()
    return [x for x in image_urls if not (x in seen or seen.add(x))]

def download_image(image_url, save_path, referer=None):
    """Download an image with retry logic."""
    headers = HEADERS.copy()
    if referer:
        headers['Referer'] = referer
    
    for attempt in range(3):
        try:
            response = requests.get(image_url, headers=headers, stream=True, timeout=10)
            response.raise_for_status()
            
            # Validate content type
            if 'image' not in response.headers.get('Content-Type', ''):
                raise ValueError("Invalid content type")
                
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
            
        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    
    logging.error(f"Failed to download after 3 attempts: {image_url}")
    return False


def sanitize_filename(name):
    """Sanitize filename by removing invalid characters."""
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    sanitized = sanitized.strip()
    return sanitized


def convert_images_to_pdf(image_paths, output_pdf_path):
    """Convert a list of image paths to a PDF file."""
    if not image_paths:
        logging.error("No images to convert")
        return False
    
    try:
        with open(output_pdf_path, 'wb') as f:
            f.write(img2pdf.convert(image_paths))
        logging.info(f"PDF generated at {output_pdf_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to generate PDF: {e}")
        return False


def generate_index_html(output_dir, pdf_files, manga_title):
    """Generate an HTML index page listing all PDFs."""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{manga_title} - Generated PDFs</title>
    </head>
    <body>
        <h1>{manga_title}</h1>
        <h2>Chapters</h2>
        <ol>
    """
    
    for pdf in pdf_files:
        html_content += f"""
            <li>
                <a href="{pdf['filename']}">{pdf['title']}</a>
            </li>
        """
    
    html_content += """
        </ol>
    </body>
    </html>
    """
    
    index_path = os.path.join(output_dir, 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info(f"Generated index.html at {index_path}")


def main():
    """Main program execution."""
    parser = argparse.ArgumentParser(description='Comic Chapter Scraper')
    parser.add_argument('book_id', help='Comic book identifier')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Debug logging enabled")

    try:
        # Get basic comic information
        title, chapters = get_content_info(args.book_id)
        logging.info(f"Found comic: {title} with {len(chapters)} chapters")
        
        # Create output directory
        output_dir = create_output_dir(title, args.book_id)
        log_path = os.path.join(output_dir, 'chapters.txt')
        pdf_files = []
        
        # Process all chapters
        with open(log_path, 'w', encoding='utf-8') as log_file:
            total_chapters = len(chapters)
            for idx, chapter in enumerate(chapters, 1):
                logging.info(f"\n📖 PROCESSING CHAPTER {idx}/{total_chapters}")
                logging.info(f"  Slot: {chapter['slot']}")
                logging.info(f"  Title: {chapter['title']}")
                
                # Get part URLs for this chapter
                parts = process_chapter(args.book_id, chapter['slot'], chapter['title'])
                
                # Create temporary directory for chapter images
                chapter_image_dir = os.path.join(output_dir, f"chapter_{chapter['slot']}")
                os.makedirs(chapter_image_dir, exist_ok=True)
                image_paths = []
                image_counter = 0
                
                # Download images from each part
                for part_url in parts:
                    image_urls = extract_image_urls(part_url)
                    for img_url in image_urls:
                        image_counter += 1
                        img_name = f"image_{image_counter:04d}.jpg"
                        img_path = os.path.join(chapter_image_dir, img_name)
                        if download_image(img_url, img_path, referer=part_url):
                            image_paths.append(img_path)
                
                # Generate PDF if images were downloaded
                if image_paths:
                    sanitized_title = sanitize_filename(chapter['title'])
                    pdf_filename = f"chapter_{chapter['slot']}_{sanitized_title}.pdf"
                    pdf_path = os.path.join(output_dir, pdf_filename)
                    if convert_images_to_pdf(image_paths, pdf_path):
                        pdf_files.append({
                            'title': f"Chapter {chapter['slot']}: {chapter['title']}",
                            'filename': pdf_filename
                        })
                else:
                    logging.error(f"No images downloaded for chapter {chapter['slot']}")
                
                # Clean up temporary images
                shutil.rmtree(chapter_image_dir, ignore_errors=True)
                
                # Write to log file
                log_file.write(f"Chapter {idx}: {chapter['title']}\n")
                for part in parts:
                    log_file.write(f"  {part}\n")
                log_file.write("\n")
                
                logging.info(f"✔️ Chapter {idx} completed ({len(image_paths)} images)")

        # Generate index.html
        generate_index_html(output_dir, pdf_files, title)
        logging.info(f"Processing completed. Results saved to: {output_dir}")

    except Exception as e:
        logging.error(f"Program failed: {e}")
        raise


if __name__ == '__main__':
    main()