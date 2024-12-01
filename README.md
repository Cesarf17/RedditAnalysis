# Reddit Data Analysis Project

A data pipeline project that analyzes Reddit data through multiple stages:

1. **Data Collection**: Python script using PRAW (Python Reddit API Wrapper) to collect:
   - Top posts from r/all
   - Comments from these posts
   - Engagement metrics including scores, upvote ratios, and comment counts

2. **Data Storage**: Implemented SQL Server database to store:
   - Posts table containing post metadata and metrics
   - Comments table with relationship to parent posts
   - Used MERGE statements to handle data updates and insertions

3. **Data Visualization**: Created Power BI dashboard to visualize:
   - Engagement patterns
   - Content analysis
   - User interaction metrics

## Tech Stack

- Python (PRAW, pandas, pyodbc)
- SQL Server Express
- Power BI
