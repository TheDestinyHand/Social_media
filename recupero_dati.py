import praw
import pandas as pd
import json
from datetime import datetime
import time
import re
import spacy
from tqdm import tqdm
from typing import List, Dict

# Configurazione
SUBREDDITS = ["Conservative", "Liberal", "PoliticalDiscussion","Republican","democrats","politics"]
POST_LIMIT = 1000
COMMENTS_PER_POST = 30  

# Inizializzazione NLP
nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])

# Funzioni di supporto modificate
def is_valid_post(post: praw.models.Submission) -> bool:
    """Verifica se un post non è rimosso/cancellato"""
    return (post.selftext not in ['[removed]', '[deleted]'] 
            and not post.removed_by_category 
            and post.author is not None)

def clean_text(text: str) -> str:
    return " ".join([token.lemma_.lower() for token in nlp(text) if not token.is_stop and token.is_alpha])

def process_post(post: praw.models.Submission) -> Dict:
    return {
        "post_id": post.id,
        "subreddit": post.subreddit.display_name,
        "title": post.title.replace(';', ','),
        "text": post.selftext.replace('\n', ' ').replace(';', ','),
        "clean_text": clean_text(post.title + " " + post.selftext),
        "author": post.author.name if post.author else "NA",
        "score": post.score,
        "upvote_ratio": post.upvote_ratio,
        "num_comments": post.num_comments,
    }

def process_comment(comment: praw.models.Comment) -> Dict:
    return {
        "comment_id": comment.id,
        "post_id": comment.submission.id,
        "author":comment.author.name if comment.author else "NA",
        "parent_id": comment.parent_id.split("_")[1] if "_" in comment.parent_id else "NA",
        "body": comment.body.replace('\n', ' ').replace(';', ','),
        "clean_body": clean_text(comment.body),
        "score": comment.score,
    }

# Configurazione API Reddit
reddit = praw.Reddit(
    client_id='ajgNlDNPw4ffYuWwMKASKQ',
    client_secret='IlZ3tnKwHk-PyoqYSnbDimyFJ83ppg',
    user_agent='social_media'
)

def fetch_subreddit_data(subreddit_name: str) -> List[Dict]:
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []
    retries = 0
    after = None
    
    with tqdm(total=POST_LIMIT, desc=f"Raccolta da r/{subreddit_name}") as pbar:
        while len(posts_data) < POST_LIMIT:
            try:
                # Ottieni post in batch da 100
                posts = list(subreddit.new(limit=100, params={'after': after}))
                
                if not posts:
                    print("\nNessun nuovo post disponibile")
                    break
                
                for post in posts:
                    if is_valid_post(post):
                        post_data = process_post(post)
                        
                        # Processa commenti
                        try:
                            post.comments.replace_more(limit=1)  # Ridotto il limite
                            comments = []
                            for comment in post.comments.list()[:COMMENTS_PER_POST]:
                                if comment.body not in ("[deleted]", "[removed]"):
                                    comments.append(process_comment(comment))
                            post_data["comments"] = comments
                            
                            posts_data.append(post_data)
                            pbar.update(1)
                            
                            if len(posts_data) >= POST_LIMIT:
                                break
                                
                        except Exception as e:
                            print(f"\nErrore nei commenti: {str(e)}")
                            continue
                            
                        after = post.fullname
                
            except Exception as e:
                print(f"\nErrore grave: {str(e)} - Tentativo {retries+1}")
                retries += 1
    return posts_data[:POST_LIMIT]

def save_r_data(data: List[Dict]):
    posts_df = pd.DataFrame([{k:v for k,v in d.items() if k != 'comments'} for d in data])
    comments_df = pd.DataFrame([c for d in data for c in d["comments"]])
    
    # Salvataggio con controllo duplicati
    posts_df.drop_duplicates(subset='post_id').to_csv("posts_r.csv", index=False, sep=';', encoding='utf-8')
    comments_df.drop_duplicates(subset='comment_id').to_csv("comments_r.csv", index=False, sep=';', encoding='utf-8')
    
if __name__ == "__main__":
    all_data = []
    for sub in SUBREDDITS:
        sub_data = fetch_subreddit_data(sub)
        all_data.extend(sub_data)
        time.sleep(10)  # Pausa tra subreddit
        
    save_r_data(all_data)
    print("\nEsportazione completata con successo!")
    print(f"Post totali raccolti: {len(all_data)}")
    print(f"Subreddit processati: {len(SUBREDDITS)}")