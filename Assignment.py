import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

start_time=time.time()
logging.basicConfig(filename='scraping_errors.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s:%(message)s')

client = MongoClient('mongodb://localhost:27017/')

def get_text_or_none(cell):
    return cell.get_text(strip=True) if cell else None

def get_int_value(cell):
    try:
        text = get_text_or_none(cell)
        return int(text) if text else None
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to int: {e}")
        return None

def get_float_value(cell):
    try:
        text = get_text_or_none(cell)
        return float(text) if text else None
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to float: {e}")
        return None

def different_collections(url):
    try:
        htmltext = requests.get(url, timeout=10).text
        soup = BeautifulSoup(htmltext, "lxml")
        return [link.text for link in soup.find_all("a", class_="year-link")]
    except requests.RequestException as e:
        logging.error(f"Error fetching collections from {url}: {e}")
        return []

def ajax_data(url, year):
    params = {'ajax': 'true', 'year': year}
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch data for year {year} from {url}")
            return None
    except requests.RequestException as e:
        logging.error(f"Error fetching ajax data for year {year} from {url}: {e}")
        return None

def parse_team_data(url):
    try:
        htmltext = requests.get(url, timeout=10).text
        soup = BeautifulSoup(htmltext, "lxml")
        team_rows = soup.find_all("tr", class_="team")
        teams = []
        for row in team_rows:
            team = {
                "name": get_text_or_none(row.find("td", class_="name")),
                "year": get_int_value(row.find("td", class_="year")),
                "wins": get_int_value(row.find("td", class_="wins")),
                "losses": get_int_value(row.find("td", class_="losses")),
                "ot_losses": get_int_value(row.find("td", class_="ot-losses")),
                "win_percentage": get_float_value(row.find("td", class_="pct")),
                "goals_for": get_int_value(row.find("td", class_="gf")),
                "goals_against": get_int_value(row.find("td", class_="ga")),
                "plus_minus": get_int_value(row.find("td", class_=["diff text-success", "diff text-danger"]))
            }
            teams.append(team)
        return teams
    except requests.RequestException as e:
        logging.error(f"Error fetching team data from {url}: {e}")
        return []

def get_total_pages(url):
    try:
        htmltext = requests.get(url, timeout=10).text
        soup = BeautifulSoup(htmltext, "lxml")
        pagination = soup.find("ul", class_="pagination")
        return int(pagination.find_all("a")[-2].text.strip()) if pagination else 1
    except requests.RequestException as e:
        logging.error(f"Error fetching total pages from {url}: {e}")
        return 1

def scrape_all_pages(base_url):
    try:
        total_pages = get_total_pages(base_url)
        all_teams = []

        # Use ThreadPoolExecutor for parallel scraping
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(parse_team_data, f"{base_url}?page_num={page_num}&per_page=25") for page_num in range(1, total_pages + 1)]
            for future in as_completed(futures):
                try:
                    all_teams.extend(future.result())
                except Exception as e:
                    logging.error(f"Error in parallel scraping: {e}")
        return all_teams
    except Exception as e:
        logging.error(f"Error scraping all pages from {base_url}: {e}")
        return []

def advanced_topic(advanced_url):
    try:
        htmltext = requests.get(advanced_url, timeout=10).text
        soup = BeautifulSoup(htmltext, "lxml")
        data = []

        for heading in soup.find_all("h4"):
            topic = heading.text.strip()
            next_p = heading.find_next_sibling("p")
            paragraph = next_p.text.strip() if next_p else None
            link = heading.find("a")
            data.append({
                "topic": link.text.strip(),
                "link": f"{"https://www.scrapethissite.com"}{link["href"]}",
                "paragraph": paragraph
            })
        return data
    except requests.RequestException as e:
        logging.error(f"Error fetching advanced topics from {advanced_url}: {e}")
        return []

def save_to_mongo(collection, data):
    try:
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)
        print("Data saved to MongoDB.")
    except Exception as e:
        logging.error(f"Error saving data to MongoDB: {e}")

def scrape_and_save(url, db_name, collection_name, data_func, *args):
    db = client[db_name]
    collection = db[collection_name]
    data = data_func(url, *args)
    save_to_mongo(collection, data)

# Scraping Ajax JavaScript page
ajax_url = "https://www.scrapethissite.com/pages/ajax-javascript/#2015"
db_name = "Oscar_Winning_Films"
years = different_collections(ajax_url)
for year in years:
    scrape_and_save(ajax_url, db_name, year, ajax_data, year)

# Scraping Forms page
forms_url = "https://www.scrapethissite.com/pages/forms/"
db_name = "Hockey_Teams_Data"
scrape_and_save(forms_url, db_name, "teams", scrape_all_pages)

# Scraping Advanced Topics page
advanced_url = "https://www.scrapethissite.com/pages/advanced/"
db_name = "Advanced_topics"
scrape_and_save(advanced_url, db_name, "Topics",advanced_topic)

endtime=time.time()
print(endtime-start_time)