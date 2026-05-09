# NEWS_PULSE-Real-time-News-Analysis-Part-1-Kafka
<img width="1774" height="887" alt="ChatGPT Image May 3, 2026, 09_56_07 PM" src="https://github.com/user-attachments/assets/a1eed120-b3d8-478f-b2bc-bcbdd4ad1bb1" />

Built a real-time streaming ingestion pipeline using Python, Apache Kafka, AWS S3, and Snowflake. Live news articles are fetched from external APIs, streamed through Kafka, consumed using Python consumers, and stored in cloud storage for scalable downstream analytics processing.

Fetches live news articles from a news website every 15 minutes
Sends those articles into a message queue called Kafka
Reads from that queue and saves the articles to cloud storage on AWS
Automatically loads those saved files into a database called Snowflake
Cleans and organises the data using a tool called dbt
Displays the cleaned data in a live dashboard built with Streamlit
