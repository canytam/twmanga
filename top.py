import sys
import os
import tempfile
import requests
import img2pdf
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin
from PIL import Image

def make_books(description, link, book_id, pdf_entries):
    """Download images and create PDF with valid dimensions"""

    try:
        response = requests.get(link)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"\nError fetching {link}: {e}", file=sys.stderr)
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    image_urls = []
    
    # Generate PDF path first to check existence
    safe_desc = "".join(c if c.isalnum() else "_" for c in description)
    pdf_name = f"{safe_desc}.pdf"
    pdf_path = os.path.join(book_id, pdf_name)
    
    if os.path.exists(pdf_path):
        pdf_entries.append((description, pdf_name))
        print(f"⏩ PDF exists, skipping: {pdf_path}")
        return

    for tag in soup.find_all(['a', 'img']):
        url = tag.get('href') or tag.get('src')
        if url:
            absolute_url = urljoin(link, url)
            if urlparse(absolute_url).path.lower().endswith(('.jpg', '.jpeg')):
                image_urls.append(absolute_url)
    
    if not image_urls:
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        img_files = []
        total_images = len(image_urls)
        
        print(f"\nProcessing {description}:")
        print(f"Found {total_images} images in {link}")
        
        with tqdm(total=total_images, desc="Downloading images", unit="img") as pbar:
            for idx, img_url in enumerate(image_urls):
                try:
                    img_data = requests.get(img_url, stream=True, timeout=10)
                    img_data.raise_for_status()
                    img_path = os.path.join(tmpdir, f"page_{idx:04d}.jpg")
                    
                    with open(img_path, 'wb') as f:
                        for chunk in img_data.iter_content(chunk_size=1024):
                            if chunk:
                                f.write(chunk)
                    
                    # Validate image dimensions
                    with Image.open(img_path) as img:
                        width, height = img.size
                        if width < 10 or height < 10:  # Minimum 10 pixels
                            print(f"\n⚠️ Skipping invalid image: {img_url} ({width}x{height})")
                            continue
                    
                    img_files.append(img_path)
                    pbar.update(1)
                except Exception as e:
                    print(f"\nError downloading {img_url}: {e}", file=sys.stderr)
                    continue

        if img_files:
            print("Creating PDF with validated dimensions...")
            
            def layout_function(img_width, img_height, pdf_page_size):
                # Convert pixels to points (1 inch = 72 points, using 96 DPI)
                dpi = 96
                width_pt = (img_width / dpi) * 72
                height_pt = (img_height / dpi) * 72
                
                # Ensure minimum size of 3.0 PDF units
                min_size = 3.0
                if width_pt < min_size or height_pt < min_size:
                    # Calculate scale factor to meet minimum size
                    scale = max(min_size/width_pt, min_size/height_pt)
                    width_pt *= scale
                    height_pt *= scale
                
                return (width_pt, height_pt, width_pt, height_pt)
            
            try:
                with open(pdf_path, "wb") as f:
                    f.write(img2pdf.convert(
                        [open(img, "rb") for img in sorted(img_files)],
                        layout_fun=layout_function
                    ))
                
                pdf_entries.append((description, pdf_name))
                print(f"\n✅ PDF created: {pdf_path}")
                print(f"📄 Total pages: {len(img_files)}")
                print("──────────────────────────────────")
            except Exception as e:
                print(f"\n❌ Failed to create PDF: {str(e)}")
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)

def main():
    if len(sys.argv) < 2:
        print("Error: Please provide a book ID as an argument.", file=sys.stderr)
        sys.exit(1)
    
    book_id = sys.argv[1]
    base_url = f"https://www.twmanga.com/comic/{book_id}"
    
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}", file=sys.stderr)
        sys.exit(1)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract book title from comics-detail__info div
    detail_div = soup.find('div', class_='comics-detail__info')
    if not detail_div:
        print("Error: Could not find comic details div", file=sys.stderr)
        sys.exit(1)

    title_tag = detail_div.find('h1')
    if not title_tag:
        print("Error: Could not find title in comic details", file=sys.stderr)
        sys.exit(1)
    
    book_title = title_tag.get_text().strip()
    # Sanitize folder name
    folder_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in book_title)
    folder_name = folder_name.replace(" ", "_")[:50]  # Limit length    soup = BeautifulSoup(response.text, 'html.parser')
    os.makedirs(folder_name, exist_ok=True)

    entries = []
    pdf_entries = []

    for a_tag in soup.find_all('a'):
        href = a_tag.get('href')
        if href and href.startswith("/user/page_direct"):
            parsed_url = urlparse(href)
            query_params = parse_qs(parsed_url.query)
            
            section = query_params.get('section_slot', [None])[0]
            chapter = query_params.get('chapter_slot', [None])[0]
            
            if section and chapter:
                try:
                    section_num = int(section)
                    chapter_num = int(chapter)
                except ValueError:
                    continue  # Skip invalid numeric values
                
                # Generate new URL format
                new_link = f"https://twmanga.com/comic/chapter/{book_id}/{section_num}_{chapter_num}.html"
                description = a_tag.text.strip()
                
                entries.append({
                    'section': section_num,
                    'chapter': chapter_num,
                    'description': description,
                    'link': new_link
                })
    
    # Sort by section then chapter
    sorted_entries = sorted(entries, key=lambda x: (x['section'], x['chapter']))
    
    # Print and process entries
    for entry in sorted_entries:
        # Print description and link only
        print(f"{entry['description']}: {entry['link']}")
    for entry in sorted_entries:
        # Call dummy function with parameters
        make_books(entry['description'], entry['link'], folder_name, pdf_entries)
#    make_books(sorted_entries[0]['description'], sorted_entries[0]['link'], book_id, pdf_entries)

    # Generate index.html
    if pdf_entries:
        index_path = os.path.join(folder_name, "index.html")
        with open(index_path, 'w') as f:
            f.write(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{folder_name} - PDF Index</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 2rem; }}
        h1 {{ color: #333; }}
        ul {{ list-style: none; padding: 0; }}
        li {{ margin: 0.5rem 0; }}
        a {{ color: #0066cc; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <h1>{folder_name} - Chapters</h1>
    <ul>
""")
            for desc, pdf in pdf_entries:
                f.write(f'        <li><a href="{pdf}">{desc}</a></li>\n')
            f.write("""    </ul>
</body>
</html>""")
        print(f"\n📚 Index page created: {index_path}")

if __name__ == "__main__":
    main()
