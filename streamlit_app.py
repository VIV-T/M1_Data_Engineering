import os
import streamlit as st
from pymongo import MongoClient
from collections import Counter
from Jaccard import find_most_similar_document


# Configuration
st.set_page_config(page_title="Very Bad Script — Dashboard", layout="wide")

### CSS (cards + layout)
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

      .header-title { font-size: 28px; font-weight: 800; margin: 0; line-height: 1.1; }
      .header-sub { font-size: 14px; opacity: 0.8; }

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

      .card-title { font-size: 13px; opacity: 0.8; margin-bottom: 8px; }
      .card-value { font-size: 28px; font-weight: 800; }
      .card-sub { font-size: 12px; opacity: 0.7; margin-top: 6px; }

      .section-title { font-size: 18px; font-weight: 750; margin-top: 12px; }
      .divider { height: 1px; background: rgba(255,255,255,0.12); margin: 14px 0; }
      .mini-note { font-size: 12px; opacity: 0.75; }

      /* Scroll area  */
      .scrolltext {
        max-height: 120px;
        overflow-y: auto;
        padding: 0;
        margin: 0;
      }

      /* Small spacing between items */
      .scrolltext .item {
        padding: 4px 0;
        margin: 0;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

### Helper: card component
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

### Mongo connection
@st.cache_resource
def get_db():
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "admin"),
        authSource=os.getenv("MONGO_AUTHSOURCE", "admin"),
    )
    return client["scriptsDB"]

@st.cache_resource
def get_movies_col():
    return get_db()["movies"]

@st.cache_resource
def get_tropes_col():
    return get_db()["tropes"]


# Parsing helper for movie tropes field
def parse_movie_tropes(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [p.strip() for p in value.split(",") if p.strip()]
    return []

# --------------------------------------------

@st.cache_data
def load_movies_and_stats():
    cursor = get_movies_col().find({}, {"_id": 0, "name": 1, "full_script": 1, "tropes": 1})

    movies = []
    total_words = 0
    max_words = 0
    max_movie = None
    counter = Counter()

    for doc in cursor:
        name = doc.get("name")
        script = doc.get("full_script") or ""
        if not name:
            continue

        wc = len(script.split())
        movies.append((name, wc))
        total_words += wc

        if wc > max_words:
            max_words = wc
            max_movie = name

        counter.update(parse_movie_tropes(doc.get("tropes")))

    movies.sort(key=lambda x: x[0].lower())
    return movies, total_words, max_words, max_movie, counter

@st.cache_data
def total_tropes_docs():
    return get_tropes_col().count_documents({})

# -------------------------------------------------------------------------- ADD by baptiste
@st.cache_data
def load_filtered_tropes(only_active=False):
    """Only get tropes present in the movies of the db OR get all tropes"""
    trope_col = get_tropes_col()
    movie_col = get_movies_col()

    if only_active:
        unique_names = movie_col.distinct("tropes")
    else:
       unique_names = trope_col.distinct("name")
   
    unique_names.sort()
    
    return [name for name in unique_names if name]
# --------------------------------------------------------------------------


### Header
st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

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
            <div class="header-sub">INSA Lyon — Data Engineering Project by Thibault Vivier, Baptiste Tron and Maia Jouenne</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# Load data
movies, total_words, max_words, max_movie, trope_counter = load_movies_and_stats()
total_scripts = len(movies)

if total_scripts == 0:
    st.warning("No movies found in MongoDB (scriptsDB.movies).")
    st.stop()

avg_words = int(total_words / total_scripts) if total_scripts else 0
tropes_total = total_tropes_docs()

### Overview cards

st.markdown("<div class='section-title'>Overview</div>", unsafe_allow_html=True)
st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    card("Total scripts", f"{total_scripts:,}", "Movies in database")
with k2:
    card("Average words / film", f"{avg_words:,}")
with k3:
    card("Total words", f"{total_words:,}", "All scripts combined")
with k4:
    card("Longest film", f"{max_words:,}", max_movie or "—")
with k5:
    card("Total tropes (definitions)", f"{tropes_total:,}")

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

### Top 10 tropes globally 
st.markdown("<div class='section-title'>Top 10 tropes (global)</div>", unsafe_allow_html=True)
st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

top10 = trope_counter.most_common(10)
if not top10:
    st.info("No tropes found in movies collection (movies.tropes is empty).")
else:
    st.markdown("<div class='scrolltext'>", unsafe_allow_html=True)
    for i, (t, c) in enumerate(top10, start=1):
        st.markdown(f"<div class='item'><b>{i}. {t}</b> — {c} movies</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

### Explore a movie

st.markdown("<div class='section-title'>Explore a movie</div>", unsafe_allow_html=True)

movie_names = [m[0] for m in movies]
word_map = {m[0]: m[1] for m in movies}

selected_movie = st.selectbox("Select a movie", movie_names)

card("Number of words", f"{word_map.get(selected_movie, 0):,}", selected_movie)
st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

movie_doc = get_movies_col().find_one(
    {"name": selected_movie},
    {"_id": 0, "full_script": 1, "tropes": 1},
)
movie_script = (movie_doc or {}).get("full_script") or ""
movie_tropes = parse_movie_tropes((movie_doc or {}).get("tropes"))

if st.button("Find most similar movie"):
    similar_name, score = find_most_similar_document(
        selected_movie,
        get_movies_col()
    )
    st.success(
        f"Most similar movie to **{selected_movie}** : "
        f"**{similar_name}** ({score:.2f})"
    )

left, right = st.columns([2.2, 1.0])

with left:
    with st.expander("Show script preview (first 2000 characters)"):
        st.text(movie_script[:2000] if movie_script else "(empty script)")

with right:
    with st.expander("Show tropes list"):
        if not movie_tropes:
            st.info("No tropes attached to this movie.")
        else:
            st.caption(f"{len(movie_tropes)} tropes")
            st.markdown("<div class='scrolltext'>", unsafe_allow_html=True)
            for t in sorted(movie_tropes, key=lambda x: x.lower()):
                st.markdown(f"<div class='item'>• {t}</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)



#---------------------------------------------------------------- ADD by baptiste

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
st.markdown("<div class='section-title'>Find Movies by Trope</div>", unsafe_allow_html=True)

# Checkbox
only_active = st.checkbox("Show only tropes present in movies", value=True)

# load tropes
available_tropes = load_filtered_tropes(only_active=only_active)

# selection
selected_trope = st.selectbox(
    f"Search among {len(available_tropes)} tropes", 
    [""] + available_tropes
)

if selected_trope:
    # get definiton
    trope_col = get_tropes_col()
    trope_info = trope_col.find_one({"name": selected_trope})
    
    if trope_info:
        st.info(f"**Definition:** {trope_info.get('definition', 'No definition available.')}")

    # find corresponding movies
    movie_col = get_movies_col()
    matching_movies = list(movie_col.find({"tropes": selected_trope}, {"name": 1, "_id": 0}))

    if matching_movies:
        st.write(f"### {len(matching_movies)} movies found:")
        cols = st.columns(3)
        for idx, movie in enumerate(matching_movies):
            cols[idx % 3].markdown(f"- **{movie['name']}**")
#----------------------------------------------------------------