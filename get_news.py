import os
import eikon as ek
import pandas as pd
from resources.parameters import *

# Authenticate with Eikon API key
ek.set_app_key(appkey)

# Convert list to search query string
df = pd.read_csv(resources + 'instruments.csv')
instruments = df['instruments'].tolist()

for instrument in instruments:
    query = f"Topic:TTMACA AND Language:LEN AND R:{instrument}"

    # Query Refinitiv for M&A news
    try:
        df = ek.get_news_headlines(query, date_from=start_date, date_to=end_date, count=4)
    except:
        print(f"Error: {instrument} gives error")
        continue

    # Retrieve full text of articles
    story_ids = df['storyId'].tolist()
    articles = []
    for story_id in story_ids:
        article = ek.get_news_story(story_id)
        articles.append(article)
    df['full_text'] = articles

    # Save each headline and story to a separate HTML file
    for index, row in df.iterrows():

        # Create filename based on the headline
        news_date = str(row['versionCreated'])[:10].replace('-', '_')
        filename = f"{instrument}_{news_date}.html"

        # Write HTML file
        with open(results + filename, 'w', encoding='utf-8') as f:
            f.write(f"<h1>{row['text']}</h1>")
            f.write(f"<p>{row['versionCreated']}</p>")
            f.write(f"<p>{row['full_text']}</p>")
    print(f"{filename} Completed...")
