# --- Imports ---
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import streamlit as st

# --- MongoDB Connection ---
MONGO_URI = "mongodb+srv://admin:ahmad@cluster0.oyvzkiz.mongodb.net/test"
client = MongoClient(MONGO_URI)
db = client["test"]
users = db["users"]

# --- Extract Data (Videos) ---
pipeline = [
    {"$unwind": "$sessions"},
    {"$unwind": "$sessions.videos"},
    {
        "$project": {
            "username": 1,
            "session_id": {"$toString": "$sessions._id"},  # Convert ObjectId to string
            "session_start": "$sessions.starttime",
            "session_end": "$sessions.endtime",
            "session_duration": "$sessions.duration",
            "status": "$sessions.videos.status",
            "watched": "$sessions.videos.watched",
            "loopTime": "$sessions.videos.loopTime",
            "videoId": "$sessions.videos.videoId",
            "keys": "$sessions.videos.keys",
        }
    },
]

data = list(users.aggregate(pipeline))
df = pd.DataFrame(data)

if df.empty:
    st.error("No data found in MongoDB!")
    st.stop()

# --- Sidebar ---
st.sidebar.title("Filters")
usernames = ["ALL"] + sorted(df["username"].dropna().unique().tolist())
selected_user = st.sidebar.selectbox("Select User", usernames)

if selected_user != "ALL":
    df = df[df["username"] == selected_user]

st.title("Content Moderation Dashboard")

# --- 1. Video Completion Status Pie Chart ---
st.subheader("Video Completion Status Distribution")
fig1 = px.pie(df, names="status", title=f"Video Completion Status for {selected_user}")
fig1.update_traces(
    hovertemplate="<b>%{label}</b><br>Videos: %{value}<br>Percentage: %{percent}"
)
st.plotly_chart(fig1, use_container_width=True)

# --- 2. Session Duration (Binned Bar Chart) ---
st.subheader("Session Duration Distribution")

session_df = df.groupby("session_id", as_index=False).agg({"session_duration": "first"})
session_df = session_df.reset_index(drop=True)
session_df["session_number"] = session_df.index + 1  # sequential session numbers

# Bin session durations
bins = [0, 30, 60, 120, 300, 600, 1800, 3600, 7200]
labels = [
    "0-30s",
    "31-60s",
    "61-120s",
    "121-300s",
    "301-600s",
    "601-1800s",
    "1801-3600s",
    "3601-7200s",
]
session_df["duration_bin"] = pd.cut(
    session_df["session_duration"].fillna(0),
    bins=bins,
    labels=labels,
    right=True,
)

duration_counts = session_df.groupby("duration_bin").size().reset_index(name="count")

fig2 = px.bar(
    duration_counts,
    x="duration_bin",
    y="count",
    title=f"Session Duration Bins for {selected_user}",
    labels={"duration_bin": "Duration Range", "count": "Number of Sessions"},
)
st.plotly_chart(fig2, use_container_width=True)

# --- 3. Idle Time Distribution ---
st.subheader("Idle Time Distribution")
pipeline_idle = [
    {"$unwind": "$sessions"},
    {"$unwind": {"path": "$sessions.inactivity", "preserveNullAndEmptyArrays": True}},
    {
        "$project": {
            "username": 1,
            "session_id": {"$toString": "$sessions._id"},
            "idle_type": "$sessions.inactivity.type",
            "idle_duration": "$sessions.inactivity.duration",
        }
    },
]
idle_data = list(users.aggregate(pipeline_idle))
idle_df = pd.DataFrame(idle_data)

if not idle_df.empty:
    if selected_user != "ALL":
        idle_df = idle_df[idle_df["username"] == selected_user]

    idle_df = idle_df.dropna(subset=["idle_duration"])  # remove null durations

    if not idle_df.empty:
        fig3 = px.histogram(
            idle_df,
            x="idle_duration",
            color="idle_type",
            nbins=20,
            title="Idle Time Distribution (Seconds)",
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No idle time durations available.")
else:
    st.info("No idle time data available.")

# --- 4. Total Video Count ---
st.subheader("Total Video Count")
total_videos = df["videoId"].nunique()
st.metric("Total Unique Videos Watched", total_videos)

# --- 4b. Unique Videos Watched per Session (Line Chart) ---
st.subheader("Unique Videos Watched per Session")

videos_per_session = (
    df.groupby("session_id")["videoId"].nunique().reset_index(name="unique_videos")
)
videos_per_session = videos_per_session.reset_index(drop=True)
videos_per_session["session_number"] = videos_per_session.index + 1

fig4b = px.line(
    videos_per_session,
    x="session_number",
    y="unique_videos",
    markers=True,
    title="Unique Videos Watched per Session",
    labels={"session_number": "Session Number", "unique_videos": "Unique Videos"},
)
st.plotly_chart(fig4b, use_container_width=True)

# --- 5. Acceptance vs Rejection ---
st.subheader("Acceptance vs Rejection")


def classify_key(keys):
    if not keys or not isinstance(keys, list):
        return "No Decision"
    keys_lower = [str(k).lower() for k in keys]
    if "a" in keys_lower:
        return "Accepted"
    if "q" in keys_lower:
        return "Rejected"
    return "No Decision"


df["decision"] = df["keys"].apply(classify_key)

fig5 = px.histogram(
    df,
    x="decision",
    color="decision",
    title="Acceptance vs Rejection",
)
st.plotly_chart(fig5, use_container_width=True)
