import os
import praw
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from typing import Dict, List
import pyodbc
import time

class RedditCollector:
    def __init__(self):
        load_dotenv()
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT'),
            read_only=True  
        )
        
        self.conn = pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=.\SQLEXPRESS;'
            r'DATABASE=RedditAnalytics;'
            r'Trusted_Connection=yes;'
        )
        self.cursor = self.conn.cursor()

    def get_top_posts(self):
        """Collect top posts from r/all"""
        all_posts = []
        all_comments = []
        
        try:
            print(f"Starting collection at {datetime.now()}")
            posts = list(self.reddit.subreddit('all').top(time_filter='month', limit = 10))
            
            for i, post in enumerate(posts):
                try:
                    print(f"Processing post {i+1}/500: {post.id}")
                    post_data = self._extract_post_data(post)
                    all_posts.append(post_data)
                    
                    print(f"Collecting comments for post {post.id}")
                    comments_df = self.collect_comments(post.id)
                    all_comments.extend(comments_df.to_dict('records'))
                    print(f"Collected {len(comments_df)} comments")
                    
                    self.save_posts_to_db(pd.DataFrame([post_data]))
                    self.save_comments_to_db(comments_df, post.id)
                    print(f"Saved post and comments to database")
                    
                    time.sleep(1)
                except Exception as e:
                    print(f"Error on post {post.id}: {str(e)}")
                    continue
            
            print(f"Collection completed at {datetime.now()}")
            return pd.DataFrame(all_posts), pd.DataFrame(all_comments)
        except Exception as e:
            print(f"Major error: {str(e)}")
            return pd.DataFrame(all_posts), pd.DataFrame(all_comments)


    def _extract_post_data(self, post):
        return {
            'subreddit': str(post.subreddit),
            'title': post.title,
            'score': post.score,
            'id': post.id,
            'url': post.url,
            'num_comments': post.num_comments,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'upvote_ratio': post.upvote_ratio,
            'author': str(post.author),
            'is_self': post.is_self,
            'content_type': 'text' if post.is_self else 'link',
            'domain': post.domain,
            'distinguished': post.distinguished,
            'stickied': post.stickied,
            'over_18': post.over_18,
            'spoiler': post.spoiler,
            'word_count': len(post.selftext.split()) if post.is_self else 0,
            'flair': post.link_flair_text,
            'post_hint': getattr(post, 'post_hint', 'unknown'),
            'engagement_ratio': post.score / post.num_comments if post.num_comments > 0 else post.score
        }


    def save_posts_to_db(self, posts_df):
        try:
            for _, post in posts_df.iterrows():
                self.cursor.execute("""
                    MERGE posts AS target
                    USING (SELECT ? AS id, ? AS score, ? AS num_comments, ? AS upvote_ratio) AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN
                        UPDATE SET score = source.score,
                                num_comments = source.num_comments,
                                upvote_ratio = source.upvote_ratio
                    WHEN NOT MATCHED THEN
                        INSERT (subreddit, title, score, id, url, num_comments, 
                            created_utc, upvote_ratio, author, is_self, content_type, domain,
                            distinguished, stickied, over_18, spoiler, word_count, flair,
                            post_hint, engagement_ratio)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    post.id, post.score, post.num_comments, post.upvote_ratio, 
                    post.subreddit, post.title, post.score, post.id, post.url,
                    post.num_comments, post.created_utc, post.upvote_ratio, post.author,
                    post.is_self, post.content_type, post.domain, post.distinguished,
                    post.stickied, post.over_18, post.spoiler, post.word_count,
                    post.flair, post.post_hint, post.engagement_ratio
                ))
            self.conn.commit()
        except pyodbc.Error as e:
            print(f"Database error: {e}")
            self.conn.rollback()


    def save_comments_to_db(self, comments_df, post_id):
        try:
            for _, comment in comments_df.iterrows():
                self.cursor.execute("""
                    MERGE comments AS target
                    USING (SELECT ? AS comment_id) AS source
                    ON target.comment_id = source.comment_id
                    WHEN NOT MATCHED THEN
                        INSERT (comment_id, post_id, author, body, score, 
                            created_utc, is_submitter, parent_id, depth)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    comment.comment_id,  
                    comment.comment_id, post_id, comment.author, comment.body, 
                    comment.score, comment.created_utc, comment.is_submitter, 
                    comment.parent_id, comment.depth
                ))
            self.conn.commit()
        except pyodbc.Error as e:
            print(f"Database error: {e}")
            self.conn.rollback()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    
    def analyze_engagement_patterns(self, df: pd.DataFrame) -> Dict:
        return {
            'top_subreddits': df.groupby('subreddit')['score'].mean().sort_values(ascending=False).head(10).to_dict(),
            'avg_score_by_type': df.groupby('content_type')['score'].mean().to_dict(),
            'avg_comments_by_type': df.groupby('content_type')['num_comments'].mean().to_dict(),
            'top_domains': df[~df.is_self]['domain'].value_counts().head(5).to_dict(),
            'engagement_by_hour': df.groupby(df['created_utc'].dt.hour)['engagement_ratio'].mean().to_dict(),
            'flair_performance': df.groupby('flair')['score'].mean().to_dict()
        }
    
    def clean_text(self, text):
        """Clean and format text"""

        cleaned = ' '.join(text.split())
        cleaned = cleaned.replace('[', '').replace(']', '').replace('(', '').replace(')','')
        return cleaned
    
    def collect_comments(self, post_id, limit = None):
        """Collect Comments From A Post"""
        submission = self.reddit.submission(id = post_id)
        comments_data = []

        submission.comments.replace_more(limit = 0)
        comments = list(submission.comments)[:limit] if limit else list(submission.comments)

        for comment in comments:
            try:
                comment_data = {
                    'post_id' : post_id,
                    'comment_id' : comment.id,
                    'author' : str(comment.author),
                    'body' : self.clean_text(comment.body),
                    'score' : comment.score,
                    'created_utc' : datetime.fromtimestamp(comment.created_utc),
                    'is_submitter' : comment.is_submitter,
                    'parent_id' : comment.parent_id,
                    'depth' : 0
                }
                comments_data.append(comment_data)
            except Exception as e:
                print(f"Error processing comment {comment.id}: {str(e)}")

        return pd.DataFrame(comments_data)




if __name__ == "__main__":
    collector = RedditCollector()
    posts_df, comments_df = collector.get_top_posts()
    collector.save_posts_to_db(posts_df)

    for _, post in posts_df.iterrows():
        collector.save_comments_to_db(
            comments_df[comments_df['post_id'] == post.id],
            post.id
        )
    analysis = collector.analyze_engagement_patterns(posts_df)
    
