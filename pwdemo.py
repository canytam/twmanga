from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
from urllib.parse import unquote

with sync_playwright() as p:
    book_id = 21163
    browser = p.firefox.launch(headless=False, slow_mo=300)
    page = browser.new_page()
    page.goto(f"https://www.8comic.com/html/{book_id}.html")
    header = page.inner_html('head')
    soup = BeautifulSoup(header, 'html.parser')
    meta_name = soup.find('meta', {'name': 'name'})
    book_name = meta_name['content'].strip() if meta_name else "Unknown Comic"
    book_dir = f"{book_name}_{book_id}"
    content_page = page.inner_html('div#chapters')
    soup = BeautifulSoup(content_page, 'html.parser')
    chapters = []
    for a_tag in soup.find_all('a'):
        if a_tag.has_attr('id'):
            chapters.append(a_tag['id'])
    
    os.makedirs(book_dir, exist_ok=True)
    os.makedirs(f'{book_dir}/{book_dir}-images', exist_ok=True)
    page.click(f'a#{chapters[0]}')
    page.is_visible('div.comics-end')
    page.click('a.view-back')
    for chapter in chapters:
        page.click(f'a#{chapter}')
        page.is_visible('div.comics-end')
        comic_page = page.inner_html('div#comics-pics')
        comic_soup = BeautifulSoup(comic_page, 'html.parser')
        print('soup ', comic_soup, flush=True)
        images = []
        for img_tag in comic_soup.find_all('img'):
            if img_tag.has_attr('src'):
                images.append('https:'+unquote(img_tag['src']))
            elif img_tag.has_attr('s'):
                images.append('https:'+unquote(img_tag['s']))
        print(images)
        with open(f'{book_dir}/{book_dir}-images/images-{chapter}.txt', 'w') as file:
            for item in images:
                file.write(f"{item}\n")
        page.click('a.view-back')
    

