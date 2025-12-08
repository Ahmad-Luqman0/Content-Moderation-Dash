from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import streamlit as st

# --- PostgreSQL Connection --- #
# Replace with your actual PostgreSQL connection string
# Format: postgresql://username:password@host:port/database_name
DB_URI = "postgresql://postgres.nhmrfxrpwjeufaxgukes:luqmanahmad1@aws-1-ap-southeast-2.pooler.supabase.com:6543/postgres"

@st.cache_resource
def get_connection():
    return create_engine(DB_URI)

try:
    engine = get_connection()
    # Test connection
    with engine.connect() as conn:
        pass
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.info("Please update the 'DB_URI' in the code with your PostgreSQL credentials.")
    st.stop()

# --- Extract Data (Videos) --- #
# Query to fetch video-level data with user and session info
# We aggregate keys for each video since one video can have multiple keys
videos_query = """
    SELECT 
        u.name as username,
        s.id as session_id,
        s.starttime as session_start,
        s.endtime as session_end,
        s.duration as session_duration,
        v.status,
        v.watched,
        v.loop_time as "loopTime",
        v.video_id as "videoId",
        v.sound_muted as "soundMuted",
        ARRAY_AGG(vk.key_value) FILTER (WHERE vk.key_value IS NOT NULL) as keys
    FROM videos v
    JOIN sessions s ON v.session_id = s.id
    JOIN users u ON s.user_id = u.id
    LEFT JOIN video_keys vk ON v.id = vk.video_id
    GROUP BY 
        u.name, s.id, s.starttime, s.endtime, s.duration, 
        v.id, v.status, v.watched, v.loop_time, v.video_id, v.sound_muted
"""

try:
    with engine.connect() as conn:
        df = pd.read_sql(text(videos_query), conn)
        
    # Ensure keys is a list, even if empty/None (pandas might handle array_agg as list or numpy array)
    # The existing code expects a list for 'classify_key' function
    df['keys'] = df['keys'].apply(lambda x: x if isinstance(x, list) else [])

except Exception as e:
    st.error(f"Error fetching data: {e}")
    st.stop()

if df.empty:
    st.error("No data found in PostgreSQL!")
    st.stop()

# --- Sidebar --- #
st.sidebar.title("Filters")

# Fetch all users for the dropdown, regardless of whether they have data
users_query = "SELECT name FROM users ORDER BY name"
try:
    with engine.connect() as conn:
        users_df = pd.read_sql(text(users_query), conn)
        all_usernames = ["ALL"] + users_df["name"].tolist()
except Exception as e:
    st.error(f"Error fetching users: {e}")
    # Fallback to existing logic if query fails
    all_usernames = ["ALL"] + sorted(df["username"].dropna().unique().tolist())

usernames = all_usernames
selected_user = st.sidebar.selectbox("Select User", usernames)

# Date Filter
if not df.empty:
    df["session_start"] = pd.to_datetime(df["session_start"])
    min_date = df["session_start"].min().date()
    max_date = df["session_start"].max().date()

    start_date = st.sidebar.date_input("Start Date", min_date)
    end_date = st.sidebar.date_input("End Date", max_date)

    if start_date > end_date:
        st.error("Error: End Date must fall after Start Date.")

# Filter by User
if selected_user != "ALL":
    df = df[df["username"] == selected_user]

# Filter by Date
if not df.empty and start_date and end_date:
    # Ensure both are date objects for comparison
    df = df[
        (df["session_start"].dt.date >= start_date) & 
        (df["session_start"].dt.date <= end_date)
    ]

# Download Button
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

st.sidebar.markdown("---")
if not df.empty:
    csv = convert_df(df)
    st.sidebar.download_button(
        label="Download Filtered Report",
        data=csv,
        file_name='content_moderation_report.csv',
        mime='text/csv',
    )

st.title("Content Moderation Dashboard")

if df.empty:
    st.info(f"No activity data found for user '{selected_user}' in the selected date range.")
    st.stop()

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

# Query to fetch inactivity data
idle_query = """
    SELECT 
        u.name as username,
        s.id as session_id,
        s.starttime as session_start,
        i.type as idle_type,
        i.duration as idle_duration
    FROM inactivity i
    JOIN sessions s ON i.session_id = s.id
    JOIN users u ON s.user_id = u.id
"""

try:
    with engine.connect() as conn:
        idle_df = pd.read_sql(text(idle_query), conn)
except Exception as e:
    st.error(f"Error fetching idle data: {e}")
    idle_df = pd.DataFrame()

if not idle_df.empty:
    if selected_user != "ALL":
        idle_df = idle_df[idle_df["username"] == selected_user]

    # Filter by Date
    if start_date and end_date:
        try:
            idle_df["session_start"] = pd.to_datetime(idle_df["session_start"])
            idle_df = idle_df[
                (idle_df["session_start"].dt.date >= start_date) & 
                (idle_df["session_start"].dt.date <= end_date)
            ]
        except Exception as e:
            st.warning(f"Could not filter idle time by date: {e}")

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
        st.info("No idle time durations available for the selected range.")
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
    keys_lower = [str(k).lower() for k in keys if k is not None]
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
    # Assuming standard "yes"/"no" or similar from DB. 
    # If DB has different values, this map might need adjustment or just show raw values if strict mapping fails.
    # The original code mapped "yes"->"Muted", "no"->"Not Muted".
    
    # We will try to handle case-insensitivity or potential differnet values if known, 
    # otherwise we stick to the original map logic.
    sound_counts["sound_status"] = sound_counts["sound_status"].map({
        "yes": "Muted",
        "no": "Not Muted",
        "true": "Muted",  # Handling potential boolean-like strings
        "false": "Not Muted"
    }).fillna(sound_counts["sound_status"]) # Fallback to original value if not mapped
    
    # Remove any unmapped values if strictly following original logic which did dropna(subset=["sound_status"]) after mapping?
    # Original: sound_counts = sound_counts.dropna(subset=["sound_status"]) 
    # This implies only "yes" and "no" were kept. Let's try to keep it robust.
    
    # If the map keys matched, we keep them. If they didn't, we might have raw values like "YES" or boolean.
    # Ideally standardizing the column content.
    
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
        # Be careful to match the mapped names "Muted" / "Not Muted"
        muted_videos = sound_counts[sound_counts["sound_status"] == "Muted"]["count"].sum()
        not_muted_videos = sound_counts[sound_counts["sound_status"] == "Not Muted"]["count"].sum()
        
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
