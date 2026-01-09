
# Content Moderation Dashboard (PostGres + Streamlit + Plotly)

This project is an interactive data analytics dashboard built with **Streamlit**, **PostGres**, and **Plotly**.  
It allows moderators and analysts to visualize user engagement patterns, session statistics, and behavioral metrics from stored PostGres data.

---

## Overview

The dashboard connects to a PostGres database containing user session and video activity data.  
It provides insights into metrics such as:

- Video completion status  
- Session duration distribution  
- Idle time behavior  
- Acceptance vs. rejection decisions  
- Sound mute status  
- Unique videos watched per session  

The interface is fully interactive, with user-level filtering and dynamic charts.

---

## Features

### 1. Video Completion Status
Displays a pie chart showing how many videos were completed, partially watched, or not started for each user or across all users.

### 2. Session Duration Distribution
Analyzes and bins session durations into time ranges (e.g., 0–30s, 31–60s, 61–120s).  
Presented as a bar chart to visualize session engagement levels.

### 3. Idle Time Distribution
Shows a histogram of idle durations during sessions, categorized by idle type (e.g., inactivity or pause).  
Helps identify user drop-offs or disengagement patterns.

### 4. Total Video Count
Displays the total number of unique videos watched across all sessions.

### 5. Unique Videos per Session
Line chart illustrating how many unique videos were watched in each session.

### 6. Acceptance vs. Rejection
Classifies and visualizes user input decisions (e.g., Accepted, Rejected, No Decision) based on keypress data.

### 7. Sound Status Distribution
Shows how many videos were watched with sound muted vs. not muted using both pie and bar charts.  
Includes metrics for total, muted, and unmuted video counts, plus muted percentage.

---

## Setup Instructions

### Prerequisites
- Python 3.8 or above
- PostGres instance or cluster (with the appropriate connection URI)
- Streamlit
- Plotly
- Pandas
- PyMongo

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Ahmad-Luqman0/Content-Moderation-Dash.git
   
   ```

2. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure your PostGres connection URI is set correctly in the script:
   ```python
   MONGO_URI = "your_PostGres_connection_string"
   ```

4. Run the Streamlit app:
   ```bash
   streamlit run app.py
   ```

---

## Project Structure

```
.
├── app.py                # Main Streamlit dashboard script
├── requirements.txt      # Dependencies list
├── Procfile              # For deployment (e.g., Railway/Heroku)
└── README.md             # Project documentation
```

---

## Requirements File Example

```
flask
flask-cors
pymongo[srv]
gunicorn
streamlit
plotly
pandas
```

---

## PostGres Structure Overview

The dashboard expects documents with the following structure:

```json
{
  "username": "user1",
  "sessions": [
    {
      "_id": "session123",
      "starttime": "2025-10-17T10:00:00Z",
      "endtime": "2025-10-17T10:30:00Z",
      "duration": 1800,
      "videos": [
        {
          "videoId": "abc123",
          "status": "Completed",
          "watched": true,
          "loopTime": 2,
          "keys": ["a"],
          "soundMuted": "no"
        }
      ],
      "inactivity": [
        { "type": "pause", "duration": 45 }
      ]
    }
  ]
}
```

---


---

## Author

Developed by Ahmad Luqman  
