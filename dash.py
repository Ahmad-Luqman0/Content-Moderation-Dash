from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import streamlit as st

# --- MongoDB Connection --- #
MONGO_URI = "mongodb+srv://admin:ahmad@cluster0.oyvzkiz.mongodb.net/test"
client = MongoClient(MONGO_URI)
db = client["test"]
users = db["users"]

# --- Extract Data (Videos) --- #
pipeline = [
    {"$unwind": "$sessions"},
    {"$unwind": "$sessions.videos"},
    {
        "$project": {
            "username": 1,
            "session_id": {"$toString": "$sessions._id"},
            "session_start": "$sessions.starttime",
            "session_end": "$sessions.endtime",
            "session_duration": "$sessions.duration",
            "status": "$sessions.videos.status",
            "watched": "$sessions.videos.watched",
            "loopTime": "$sessions.videos.loopTime",
            "videoId": "$sessions.videos.videoId",
            "keys": "$sessions.videos.keys",
            "soundMuted": "$sessions.videos.soundMuted",
        }
    },
]

data = list(users.aggregate(pipeline))
df = pd.DataFrame(data)

if df.empty:
    st.error("No data found in MongoDB!")
    st.stop()

# --- Sidebar --- #
st.sidebar.title("Filters")
usernames = ["ALL"] + sorted(df["username"].dropna().unique().tolist())
selected_user = st.sidebar.selectbox("Select User", usernames)

if selected_user != "ALL":
    df = df[df["username"] == selected_user]

st.title("Content Moderation Dashboard")

# ---  Video Completion Status Pie Chart --- #
st.subheader("Video Completion Status Distribution")
fig1 = px.pie(df, names="status", title=f"Video Completion Status for {selected_user}")
fig1.update_traces(
    hovertemplate="<b>%{label}</b><br>Videos: %{value}<br>Percentage: %{percent}"
)
st.plotly_chart(fig1, use_container_width=True)

# ---  Session Duration (Binned Bar Chart) --- #
st.subheader("Session Duration Distribution")

session_df = df.groupby("session_id", as_index=False).agg({"session_duration": "first"})
session_df = session_df.reset_index(drop=True)
session_df["session_number"] = session_df.index + 1

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

# ---  Idle Time Distribution --- #
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

    idle_df = idle_df.dropna(subset=["idle_duration"])

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

# --- Total Video Count ---  #
st.subheader("Total Video Count")
total_videos = df["videoId"].nunique()
st.metric("Total Unique Videos Watched", total_videos)

# --- Unique Videos Watched per Session (Line Chart) ---  #
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

# --- Acceptance vs Rejection ---  #
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

# --- Sound Muted vs Not Muted --- #
st.subheader("Sound Status Distribution")

# Clean and process sound muted data
df_sound = df.dropna(subset=["soundMuted"])  # Remove rows without sound data

if not df_sound.empty:
    # Count muted vs not muted videos
    sound_counts = df_sound["soundMuted"].value_counts().reset_index()
    sound_counts.columns = ["sound_status", "count"]
    
    # Create a more readable mapping
    sound_counts["sound_status"] = sound_counts["sound_status"].map({
        "yes": "Muted",
        "no": "Not Muted"
    })
    
    # Remove any unmapped values
    sound_counts = sound_counts.dropna(subset=["sound_status"])
    
    if not sound_counts.empty:
        # Create pie chart for sound status distribution
        fig6 = px.pie(
            sound_counts,
            names="sound_status",
            values="count",
            title=f"Sound Status Distribution for {selected_user}",
            color_discrete_map={"Muted": "#ff6b6b", "Not Muted": "#4ecdc4"}
        )
        fig6.update_traces(
            hovertemplate="<b>%{label}</b><br>Videos: %{value}<br>Percentage: %{percent}"
        )
        st.plotly_chart(fig6, use_container_width=True)
        
        # Create bar chart for better comparison
        fig7 = px.bar(
            sound_counts,
            x="sound_status",
            y="count",
            title=f"Sound Status Count for {selected_user}",
            color="sound_status",
            color_discrete_map={"Muted": "#ff6b6b", "Not Muted": "#4ecdc4"}
        )
        fig7.update_traces(
            hovertemplate="<b>%{x}</b><br>Count: %{y}"
        )
        st.plotly_chart(fig7, use_container_width=True)
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        total_sound_videos = sound_counts["count"].sum()
        muted_videos = sound_counts[sound_counts["sound_status"] == "Muted"]["count"].sum() if not sound_counts[sound_counts["sound_status"] == "Muted"].empty else 0
        not_muted_videos = sound_counts[sound_counts["sound_status"] == "Not Muted"]["count"].sum() if not sound_counts[sound_counts["sound_status"] == "Not Muted"].empty else 0
        
        with col1:
            st.metric("Total Videos with Sound Data", total_sound_videos)
        with col2:
            st.metric("Muted Videos", muted_videos)
        with col3:
            st.metric("Not Muted Videos", not_muted_videos)
            
        # Calculate percentage
        if total_sound_videos > 0:
            muted_percentage = (muted_videos / total_sound_videos) * 100
            st.info(f"**{muted_percentage:.1f}%** of videos were watched with sound muted")
    else:
        st.warning("No valid sound status data found after processing.")
else:
    st.info("No sound muted data available for the selected user.")
