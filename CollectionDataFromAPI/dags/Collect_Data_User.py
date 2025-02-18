import pandas as pd
import requests
import os
import concurrent.futures
from dotenv import load_dotenv

def Get_API_User():
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    result = response.json()
    if 'results' in result and result['results']:
        return result['results'][0]
    else:
        raise Exception("No results found in API response")

def Get_API_News():
    load_dotenv()
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise Exception("NEWS_API_KEY is missing!")
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "country": "us",
        "category": "technology",
        "apiKey": api_key
    }
    res = requests.get(url, params=params)
    if res.status_code == 200:
        result = res.json()
        if 'articles' in result and result['articles']:
            return result['articles'][0]
        else:
            raise Exception("No articles found in News API response")
    else:
        raise Exception(f"Failed to fetch news. Status code: {res.status_code}")

def Fetch_User_And_News():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_user = executor.submit(Get_API_User)
        future_news = executor.submit(Get_API_News)
        user = future_user.result()
        news = future_news.result()
        return user, news

def Transform_Data_User():
    users = []
    for _ in range(10):
        try:
            res_user, res_news = Fetch_User_And_News()
            index_in_phone = res_user['phone'].find(')')
            phone = res_user['phone'][index_in_phone+1:].replace("-", "")
            date_registered = res_user['registered']['date'][0:10]
            location = res_user['location']
            date_published = res_news['publishedAt'][0:10]
            user = {
                'first_name': res_user['name']['first'],
                'last_name': res_user['name']['last'],
                'gender': res_user['gender'],
                'street': f"{location['street']['number']} {location['street']['name']}",
                'city': location['city'],
                'country': location['country'],
                'postcode': str(location['postcode']),
                'latitude': float(location['coordinates']['latitude']),
                'longitude': float(location['coordinates']['longitude']),
                'email': res_user['email'],
                'phone': str(phone),
                'username': res_user['login']['username'],
                'password_hash': res_user['login']['md5'],
                'date_registered': str(date_registered),
                'author': res_news['author'],
                'title': res_news['title'],
                'description': res_news['description'],
                'url': str(res_news['url']),
                'urlToImage': str(res_news['urlToImage']),
                'date_published': date_published,
                'content': res_news['content']
            }
            users.append(user)
        except Exception as e:
            print(f"Error fetching data: {e}")
    df = pd.DataFrame(users)
    return df

if __name__ == '__main__':
    Transform_Data_User()
