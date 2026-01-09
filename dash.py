from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import streamlit as st

# --- PostgreSQL Connection --- #
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
    df["keys"] = df["keys"].apply(lambda x: x if isinstance(x, list) else [])

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

# Session Filter (depends on selected user) - Query sessions directly from DB
if selected_user != "ALL":
    user_sessions_query = """
        SELECT s.id as session_id 
        FROM sessions s
        JOIN users u ON s.user_id = u.id
        WHERE u.name = :username
        ORDER BY s.starttime DESC
    """
    try:
        with engine.connect() as conn:
            user_sessions_df = pd.read_sql(text(user_sessions_query), conn, params={"username": selected_user})
            user_sessions = user_sessions_df["session_id"].tolist()
    except Exception as e:
        st.warning(f"Could not fetch sessions: {e}")
        user_sessions = []
    
    session_options = ["ALL"] + [
        f"Session {i+1} (ID: {sid})" for i, sid in enumerate(user_sessions)
    ]
    selected_session = st.sidebar.selectbox("Select Session", session_options)
else:
    selected_session = "ALL"

# Date Filter - Use sessions table for min date, today for end date
sessions_date_query = """
    SELECT MIN(starttime) as min_date FROM sessions
"""
try:
    with engine.connect() as conn:
        date_range_df = pd.read_sql(text(sessions_date_query), conn)
        if not date_range_df.empty and date_range_df["min_date"].notna().any():
            min_date = pd.to_datetime(date_range_df["min_date"].iloc[0]).date()
        else:
            min_date = pd.Timestamp.now().date()
except Exception as e:
    st.warning(f"Could not fetch date range from sessions: {e}")
    min_date = pd.Timestamp.now().date()

# End date always defaults to today
today = pd.Timestamp.now().date()

start_date = st.sidebar.date_input("Start Date", min_date)
end_date = st.sidebar.date_input("End Date", today)

if start_date > end_date:
    st.error("Error: End Date must fall after Start Date.")

# Convert session_start for filtering
if not df.empty:
    df["session_start"] = pd.to_datetime(df["session_start"])

# Filter by User
if selected_user != "ALL":
    df = df[df["username"] == selected_user]

# Filter by Session
if selected_user != "ALL" and selected_session != "ALL":
    # Extract session ID from the selected session string
    session_id = selected_session.split("ID: ")[1].rstrip(")")
    df = df[df["session_id"] == session_id]

# Filter by Date
if not df.empty and start_date and end_date:
    # Ensure both are date objects for comparison
    df = df[
        (df["session_start"].dt.date >= start_date)
        & (df["session_start"].dt.date <= end_date)
    ]


# Download Button
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode("utf-8")


st.sidebar.markdown("---")
if not df.empty:
    csv = convert_df(df)
    st.sidebar.download_button(
        label="Download Filtered Report",
        data=csv,
        file_name="content_moderation_report.csv",
        mime="text/csv",
    )

st.title("Content Moderation Dashboard")

has_video_data = not df.empty

if not has_video_data:
    st.info(
        f"No video activity data found for user '{selected_user}' in the selected date range."
    )

# ---  Video Completion Status Pie Chart --- #
if has_video_data:
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
                    (idle_df["session_start"].dt.date >= start_date)
                    & (idle_df["session_start"].dt.date <= end_date)
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
        sound_counts["sound_status"] = (
            sound_counts["sound_status"]
            .map(
                {
                    "yes": "Muted",
                    "no": "Not Muted",
                    "true": "Muted",
                    "false": "Not Muted",
                }
            )
            .fillna(sound_counts["sound_status"])
        )

        if not sound_counts.empty:
            # Create pie chart for sound status distribution
            fig6 = px.pie(
                sound_counts,
                names="sound_status",
                values="count",
                title=f"Sound Status Distribution for {selected_user}",
                color_discrete_map={"Muted": "#ff6b6b", "Not Muted": "#4ecdc4"},
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
                color_discrete_map={"Muted": "#ff6b6b", "Not Muted": "#4ecdc4"},
            )
            fig7.update_traces(hovertemplate="<b>%{x}</b><br>Count: %{y}")
            st.plotly_chart(fig7, use_container_width=True)

            # Display metrics
            col1, col2, col3 = st.columns(3)

            total_sound_videos = sound_counts["count"].sum()
            muted_videos = sound_counts[sound_counts["sound_status"] == "Muted"][
                "count"
            ].sum()
            not_muted_videos = sound_counts[sound_counts["sound_status"] == "Not Muted"][
                "count"
            ].sum()

            with col1:
                st.metric("Total Videos with Sound Data", total_sound_videos)
            with col2:
                st.metric("Muted Videos", muted_videos)
            with col3:
                st.metric("Not Muted Videos", not_muted_videos)

            # Calculate percentage
            if total_sound_videos > 0:
                muted_percentage = (muted_videos / total_sound_videos) * 100
                st.info(
                    f"**{muted_percentage:.1f}%** of videos were watched with sound muted"
                )
        else:
            st.warning("No valid sound status data found after processing.")
    else:
        st.info("No sound muted data available for the selected user.")

    # --- Video Playback Speeds Analysis --- #
    st.subheader("Video Playback Speeds Analysis")

    # Query to fetch video speeds data
    speeds_query = """
        SELECT 
            u.name as username,
            s.id as session_id,
            s.starttime as session_start,
            v.video_id,
            vs.speed_value,
            vs.created_at as speed_timestamp
        FROM video_speeds vs
        JOIN videos v ON vs.video_id = v.id
        JOIN sessions s ON v.session_id = s.id
        JOIN users u ON s.user_id = u.id
    """

    try:
        with engine.connect() as conn:
            speeds_df = pd.read_sql(text(speeds_query), conn)
    except Exception as e:
        st.error(f"Error fetching video speeds data: {e}")
        speeds_df = pd.DataFrame()

    if not speeds_df.empty:
        # Apply user filter
        if selected_user != "ALL":
            speeds_df = speeds_df[speeds_df["username"] == selected_user]

        # Apply session filter
        if selected_user != "ALL" and selected_session != "ALL":
            session_id = selected_session.split("ID: ")[1].rstrip(")")
            speeds_df = speeds_df[speeds_df["session_id"] == session_id]

        # Filter by Date
        if start_date and end_date:
            try:
                speeds_df["session_start"] = pd.to_datetime(speeds_df["session_start"])
                speeds_df = speeds_df[
                    (speeds_df["session_start"].dt.date >= start_date)
                    & (speeds_df["session_start"].dt.date <= end_date)
                ]
            except Exception as e:
                st.warning(f"Could not filter video speeds by date: {e}")

        speeds_df = speeds_df.dropna(subset=["speed_value"])

        if not speeds_df.empty:
            # Speed Distribution - Bar Chart
            speed_counts = speeds_df["speed_value"].value_counts().reset_index()
            speed_counts.columns = ["speed", "count"]
            speed_counts = speed_counts.sort_values("speed")

            fig_speed_bar = px.bar(
                speed_counts,
                x="speed",
                y="count",
                title=f"Video Playback Speed Usage Count for {selected_user}",
                labels={"speed": "Playback Speed (x)", "count": "Number of Videos"},
                color="speed",
                color_continuous_scale="Viridis",
            )
            fig_speed_bar.update_traces(hovertemplate="<b>Speed: %{x}x</b><br>Count: %{y}")
            st.plotly_chart(fig_speed_bar, use_container_width=True)

            # Average Speed per Session - Line Chart
            avg_speed_per_session = (
                speeds_df.groupby("session_id")["speed_value"].mean().reset_index()
            )
            avg_speed_per_session.columns = ["session_id", "avg_speed"]
            avg_speed_per_session = avg_speed_per_session.sort_values(
                "session_id"
            ).reset_index(drop=True)
            avg_speed_per_session["session_number"] = avg_speed_per_session.index + 1

            fig_avg_speed = px.line(
                avg_speed_per_session,
                x="session_number",
                y="avg_speed",
                markers=True,
                title="Average Playback Speed per Session",
                labels={
                    "session_number": "Session Number",
                    "avg_speed": "Average Speed (x)",
                },
            )
            fig_avg_speed.update_traces(
                hovertemplate="<b>Session %{x}</b><br>Avg Speed: %{y:.2f}x"
            )
            st.plotly_chart(fig_avg_speed, use_container_width=True)

        else:
            st.info("No video speed data available after applying filters.")
    else:
        st.info("No video speed data available in the database.")

# --- Queue Analysis --- #
st.subheader("Queue Analysis")

# Query to fetch queue data
queues_query = """
    SELECT 
        u.name as username,
        q.session_id,
        q.name as main_queue,
        q.main_queue_count,
        q.subqueues,
        q.subqueue_counts,
        s.starttime as session_start
    FROM queues q
    JOIN sessions s ON q.session_id = s.id
    JOIN users u ON s.user_id = u.id
    WHERE q.active = true
    ORDER BY q.created_at DESC
"""

try:
    with engine.connect() as conn:
        queues_df = pd.read_sql(text(queues_query), conn)
except Exception as e:
    st.error(f"Error fetching queue data: {e}")
    queues_df = pd.DataFrame()

if not queues_df.empty:
    # Apply user filter
    if selected_user != "ALL":
        queues_df = queues_df[queues_df["username"] == selected_user]

    # Apply session filter
    if selected_user != "ALL" and selected_session != "ALL":
        session_id = selected_session.split("ID: ")[1].rstrip(")")
        queues_df = queues_df[queues_df["session_id"] == session_id]

    # Filter by Date
    if start_date and end_date:
        try:
            queues_df["session_start"] = pd.to_datetime(queues_df["session_start"])
            queues_df = queues_df[
                (queues_df["session_start"].dt.date >= start_date)
                & (queues_df["session_start"].dt.date <= end_date)
            ]
        except Exception as e:
            st.warning(f"Could not filter queues by date: {e}")

    if not queues_df.empty:
        import json

        # Parse subqueues and subqueue_counts from JSON strings/lists
        def parse_json_field(val):
            if val is None:
                return []
            if isinstance(val, list):
                return val
            if isinstance(val, dict):
                return val
            try:
                return json.loads(val)
            except:
                return []

        queues_df["subqueues_parsed"] = queues_df["subqueues"].apply(parse_json_field)
        queues_df["subqueue_counts_parsed"] = queues_df["subqueue_counts"].apply(parse_json_field)

        # Queue Summary Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Queues", len(queues_df))
        with col2:
            total_main_count = queues_df["main_queue_count"].fillna(0).sum()
            st.metric("Total Main Queue Items", int(total_main_count))
        with col3:
            unique_queues = queues_df["main_queue"].nunique()
            st.metric("Unique Queue Names", unique_queues)

        # Main Queue Distribution Bar Chart
        queue_counts = queues_df.groupby("main_queue")["main_queue_count"].sum().reset_index()
        queue_counts.columns = ["Queue Name", "Item Count"]
        queue_counts = queue_counts.sort_values("Item Count", ascending=False)

        if not queue_counts.empty and queue_counts["Item Count"].sum() > 0:
            fig_queue = px.bar(
                queue_counts,
                x="Queue Name",
                y="Item Count",
                title=f"Main Queue Distribution for {selected_user}",
                color="Item Count",
                color_continuous_scale="Blues",
            )
            fig_queue.update_traces(hovertemplate="<b>%{x}</b><br>Items: %{y}")
            fig_queue.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_queue, use_container_width=True)

        # Detailed Queue Table with Subqueues - Only show when specific session is selected
        if selected_session != "ALL":
            st.markdown("### Queue Details with Subqueues")
            
            for idx, row in queues_df.iterrows():
                main_queue = row["main_queue"]
                main_count = row["main_queue_count"] if pd.notna(row["main_queue_count"]) else 0
                subqueues = row["subqueues_parsed"]
                subqueue_counts = row["subqueue_counts_parsed"]

                # Only show queues with data
                if main_count > 0 or (subqueues and len(subqueues) > 0):
                    with st.expander(f"**{main_queue}** (Main Count: {int(main_count)})"):
                        if subqueues and len(subqueues) > 0:
                            st.markdown("**Subqueues:**")
                            
                            # Create a dataframe for subqueues display
                            subqueue_data = []
                            for sq in subqueues:
                                # Get count from subqueue_counts dict
                                count = 0
                                if isinstance(subqueue_counts, dict):
                                    count = subqueue_counts.get(sq, 0)
                                # Truncate long subqueue names for display
                                display_name = sq[:80] + "..." if len(sq) > 80 else sq
                                subqueue_data.append({"Subqueue": display_name, "Count": count})
                            
                            if subqueue_data:
                                subqueue_df = pd.DataFrame(subqueue_data)
                                st.dataframe(subqueue_df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No subqueues available for this queue.")
    else:
        st.info("No queue data available after applying filters.")
else:
    st.info("No queue data available in the database.")
