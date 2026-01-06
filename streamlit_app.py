import os
import streamlit as st
from pymongo import MongoClient

# Title
st.set_page_config(page_title="Very Bad Script — Dashboard", layout="wide")

# CSS (cards + layout)
st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

      .header {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 16px;
        border-radius: 16px;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.10);
      }

      .header-title {
        font-size: 28px;
        font-weight: 800;
        margin: 0;
        line-height: 1.1;
      }

      .header-sub {
        font-size: 14px;
        opacity: 0.8;
      }

      .card {
        padding: 16px;
        border-radius: 16px;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.10);
        height: 110px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
      }

      .card-title {
        font-size: 13px;
        opacity: 0.8;
        margin-bottom: 8px;
      }

      .card-value {
        font-size: 28px;
        font-weight: 800;
      }

      .card-sub {
        font-size: 12px;
        opacity: 0.7;
        margin-top: 6px;
      }

      .section-title {
        font-size: 18px;
        font-weight: 750;
        margin-top: 12px;
      }

      .divider {
        height: 1px;
        background: rgba(255,255,255,0.12);
        margin: 14px 0;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# Card component
def card(title, value, subtitle=""):
    subtitle_html = f"<div class='card-sub'>{subtitle}</div>" if subtitle else ""
    st.markdown(
        f"""
        <div class="card">
            <div class="card-title">{title}</div>
            <div class="card-value">{value}</div>
            {subtitle_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# Mongo connection

@st.cache_resource
def get_movies_collection():
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "admin"),
        authSource=os.getenv("MONGO_AUTHSOURCE", "admin"),
    )
    return client["scriptsDB"]["movies"]

@st.cache_resource
def get_tropes_collection():
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "admin"),
        authSource=os.getenv("MONGO_AUTHSOURCE", "admin"),
    )
    return client["scriptsDB"]["tropes"]

@st.cache_data
def load_movies():
    col = get_movies_collection()
    cursor = col.find({}, {"_id": 0, "file_name": 1, "full_script": 1})

    movies = []
    total_words = 0
    max_words = 0
    max_movie = None

    for doc in cursor:
        name = doc.get("file_name")
        script = doc.get("full_script") or ""
        if not name:
            continue

        wc = len(script.split())
        movies.append((name, wc))
        total_words += wc

        if wc > max_words:
            max_words = wc
            max_movie = name

    movies.sort(key=lambda x: x[0].lower())
    return movies, total_words, max_words, max_movie


@st.cache_data
def load_total_tropes():
    col = get_tropes_collection()
    return col.count_documents({})


# Header (logo + title)
st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)

logo_path = os.path.join("images", "logo-insa_0.png")

c1, c2 = st.columns([1, 6])

with c1:
    if os.path.exists(logo_path):
        st.image(logo_path, width=120)

with c2:
    st.markdown(
        """
        <div class="header">
          <div>
            <div class="header-title">Very Bad Script</div>
            <div class="header-sub">INSA Lyon — Data Engineering Project by Thibault Vivier, Batiste Tron and Maia Jouenne</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# Load data from mongo
movies, total_words, max_words, max_movie = load_movies()
total_scripts = len(movies)

if total_scripts == 0:
    st.warning("No scripts found in MongoDB (scriptsDB.movies).")
    st.stop()

avg_words = int(total_words / total_scripts)

total_tropes = load_total_tropes()


# Stat cards

st.markdown("<div class='section-title'>Overview</div>", unsafe_allow_html=True)

st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)


k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    card("Total scripts", str(total_scripts), "Movies in database")

with k2:
    card("Average words / film", f"{avg_words:,}")

with k3:
    card("Total words", f"{total_words:,}", "All scripts combined")

with k4:
    card("Longest film", f"{max_words:,}", max_movie or "—")

with k5:
    card("Total tropes", f"{total_tropes:,}")
        
st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)


# Explore a movie 
st.markdown("<div class='section-title'>Explore a movie</div>", unsafe_allow_html=True)

movie_names = [m[0] for m in movies]
word_map = {m[0]: m[1] for m in movies}

# Selectbox is searchable by typing inside it (built-in)
selected_movie = st.selectbox("Select a movie", movie_names)

# Card for selected movie stats
card("Number of words", f"{word_map.get(selected_movie, 0):,}", selected_movie)

st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)

# Script preview
with st.expander("Show script preview (first 2000 characters)"):
    col = get_movies_collection()
    doc = col.find_one({"file_name": selected_movie}, {"_id": 0, "full_script": 1})
    text = (doc or {}).get("full_script", "")
    st.text(text[:2000] if text else "(empty script)")

