import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ── App config ────────────────────────────────────────────────
st.set_page_config(
    page_title = "NewsPulse Analytics",
    page_icon  = "📰",
    layout     = "wide"
)

# ── Snowflake session (auto-injected in Streamlit in Snowflake)
session = get_active_session()

# ── Schema where Gold tables live ─────────────────────────────
GOLD   = "NEWS_PIPELINE.PROD_GOLD"   # adjust if your dbt target schema differs
SILVER = "NEWS_PIPELINE.PROD_SILVER"


# ══════════════════════════════════════════════════════════════
# HELPER: cached data loaders
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)   # cache for 5 minutes
def load_headlines():
    return session.sql(f"""
        SELECT
            TITLE, DESCRIPTION, SOURCE_NAME, CATEGORY,
            PUBLISHED_AT, URL, HAS_CONTENT,
            RANK_IN_CATEGORY, MINUTES_AGO
        FROM {GOLD}.GOLD_LATEST_HEADLINES
        ORDER BY PUBLISHED_AT DESC
    """).to_pandas()

@st.cache_data(ttl=300)
def load_trends():
    return session.sql(f"""
        SELECT
            CATEGORY, PUBLISHED_HOUR, PUBLISHED_DATE,
            ARTICLE_COUNT, CONTENT_COVERAGE_PCT
        FROM {GOLD}.GOLD_ARTICLES_BY_CATEGORY_HOUR
        WHERE PUBLISHED_HOUR >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY PUBLISHED_HOUR ASC
    """).to_pandas()

@st.cache_data(ttl=300)
def load_sources():
    return session.sql(f"""
        SELECT
            SOURCE_NAME, CATEGORY, ARTICLE_COUNT,
            CONTENT_COVERAGE_PCT, DESCRIPTION_COVERAGE_PCT,
            AVG_TITLE_WORDS, LATEST_ARTICLE_AT
        FROM {GOLD}.GOLD_TOP_SOURCES
        ORDER BY ARTICLE_COUNT DESC
    """).to_pandas()

@st.cache_data(ttl=300)
def load_patterns():
    return session.sql(f"""
        SELECT
            HOUR_OF_DAY, DAY_OF_WEEK, DAY_NUM,
            TOTAL_ARTICLES, TOP_CATEGORY
        FROM {GOLD}.GOLD_PUBLISHING_PATTERNS
        ORDER BY DAY_NUM, HOUR_OF_DAY
    """).to_pandas()

@st.cache_data(ttl=300)
def load_quality():
    return session.sql(f"""
        SELECT
            CATEGORY, TOTAL_ARTICLES, UNIQUE_SOURCES,
            CONTENT_COVERAGE_PCT, DESCRIPTION_COVERAGE_PCT,
            AUTHOR_COVERAGE_PCT, AVG_TITLE_WORDS, QUALITY_SCORE,
            LATEST_ARTICLE_AT
        FROM {GOLD}.GOLD_CONTENT_QUALITY
        ORDER BY TOTAL_ARTICLES DESC
    """).to_pandas()

@st.cache_data(ttl=300)
def load_summary_stats():
    return session.sql(f"""
        SELECT
            COUNT(*)                            AS TOTAL_ARTICLES,
            COUNT(DISTINCT SOURCE_NAME)         AS TOTAL_SOURCES,
            COUNT(DISTINCT CATEGORY)            AS TOTAL_CATEGORIES,
            MAX(PUBLISHED_AT)                   AS LATEST_ARTICLE,
            ROUND(AVG(TITLE_WORD_COUNT), 1)     AS AVG_TITLE_WORDS
        FROM {SILVER}.SILVER_CLEAN_NEWS
    """).to_pandas()


# ══════════════════════════════════════════════════════════════
# SIDEBAR — Navigation + Global Filters
# ══════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("📰 NewsPulse")
    st.caption("Real-time news analytics powered by Kafka + Snowflake + dbt")
    st.divider()

    page = st.radio(
        "Navigate",
        ["🏠 Home — Headlines",
         "📈 Trends",
         "🏆 Sources",
         "🕐 Publishing Patterns",
         "🎯 Quality Scorecard"],
        label_visibility="collapsed"
    )

    st.divider()

    # Global category filter (used across pages)
    all_categories = ["TECHNOLOGY","BUSINESS","SCIENCE",
                      "HEALTH","SPORTS","ENTERTAINMENT"]
    selected_cats = st.multiselect(
        "Filter Categories",
        options    = all_categories,
        default    = all_categories,
        help       = "Applies to all pages"
    )

    st.divider()

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.caption("Data refreshes every 5 min automatically")


# ══════════════════════════════════════════════════════════════
# GLOBAL HEADER METRICS (shown on all pages)
# ══════════════════════════════════════════════════════════════

try:
    stats = load_summary_stats()
    s = stats.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📄 Total Articles",  f"{int(s['TOTAL_ARTICLES']):,}")
    c2.metric("📡 News Sources",    f"{int(s['TOTAL_SOURCES'])}")
    c3.metric("🗂 Categories",      f"{int(s['TOTAL_CATEGORIES'])}")
    c4.metric("🕐 Last Article",
              pd.to_datetime(s['LATEST_ARTICLE']).strftime("%H:%M UTC")
              if s['LATEST_ARTICLE'] else "—")
except Exception as e:
    st.warning(f"Could not load summary stats: {e}")

st.divider()


# ══════════════════════════════════════════════════════════════
# PAGE 1 — HOME: LIVE HEADLINES FEED
# ══════════════════════════════════════════════════════════════

if page == "🏠 Home — Headlines":
    st.title("🏠 Live Headlines Feed")
    st.caption("Top 20 most recent articles per category · refreshes every 5 min")

    df = load_headlines()
    if df.empty:
        st.warning("No headlines loaded yet. Make sure your pipeline is running.")
        st.stop()

    # Filter by selected categories
    df = df[df["CATEGORY"].isin(selected_cats)]

    # ── Search bar ────────────────────────────────────────────
    search = st.text_input("🔍 Search headlines", placeholder="e.g. AI, FDA, Tesla...")
    if search:
        mask = (
            df["TITLE"].str.contains(search, case=False, na=False) |
            df["DESCRIPTION"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
        st.caption(f"Found **{len(df)}** articles matching '{search}'")

    # ── Category tabs ─────────────────────────────────────────
    tabs = st.tabs(selected_cats)

    category_colors = {
        "TECHNOLOGY":    "#3B82F6",
        "BUSINESS":      "#10B981",
        "SCIENCE":       "#8B5CF6",
        "HEALTH":        "#EF4444",
        "SPORTS":        "#F59E0B",
        "ENTERTAINMENT": "#EC4899"
    }

    for tab, cat in zip(tabs, selected_cats):
        with tab:
            cat_df = df[df["CATEGORY"] == cat].head(20)

            if cat_df.empty:
                st.info(f"No headlines for {cat} yet.")
                continue

            st.caption(f"**{len(cat_df)} articles** · Latest: "
                       f"{pd.to_datetime(cat_df['PUBLISHED_AT'].max()).strftime('%b %d, %H:%M UTC')}")

            for _, row in cat_df.iterrows():
                with st.container():
                    col1, col2 = st.columns([5, 1])

                    with col1:
                        # Minutes ago label
                        mins = int(row["MINUTES_AGO"]) if pd.notna(row["MINUTES_AGO"]) else 0
                        if mins < 60:
                            time_label = f"{mins}m ago"
                        elif mins < 1440:
                            time_label = f"{mins // 60}h ago"
                        else:
                            time_label = f"{mins // 1440}d ago"

                        st.markdown(
                            f"**[{row['TITLE']}]({row['URL']})**"
                        )
                        if pd.notna(row["DESCRIPTION"]) and row["DESCRIPTION"]:
                            st.caption(row["DESCRIPTION"][:200] + "...")
                        st.caption(
                            f"🗞 {row['SOURCE_NAME']} &nbsp;|&nbsp; 🕐 {time_label}"
                            + (" &nbsp;|&nbsp; 📄 Full article" if row["HAS_CONTENT"] else "")
                        )

                    with col2:
                        color = category_colors.get(cat, "#6B7280")
                        st.markdown(
                            f"<div style='background:{color};color:white;"
                            f"padding:4px 8px;border-radius:12px;"
                            f"font-size:11px;text-align:center;margin-top:8px'>"
                            f"{cat.title()}</div>",
                            unsafe_allow_html=True
                        )

                st.divider()


# ══════════════════════════════════════════════════════════════
# PAGE 2 — TRENDS
# ══════════════════════════════════════════════════════════════

elif page == "📈 Trends":
    st.title("📈 Article Volume Trends")
    st.caption("Hourly article counts per category over the last 7 days")

    df = load_trends()
    if df.empty:
        st.warning("No trend data available yet.")
        st.stop()

    df = df[df["CATEGORY"].isin(selected_cats)]
    df["PUBLISHED_HOUR"] = pd.to_datetime(df["PUBLISHED_HOUR"])

    # ── Controls ──────────────────────────────────────────────
    col1, col2 = st.columns([2, 1])
    with col1:
        granularity = st.radio(
            "Granularity",
            ["Hourly", "Daily"],
            horizontal=True
        )
    with col2:
        metric = st.radio(
            "Metric",
            ["Article Count", "Content Coverage %"],
            horizontal=True
        )

    # Aggregate to daily if selected
    if granularity == "Daily":
        df = (df.groupby(["CATEGORY", "PUBLISHED_DATE"])
                .agg(ARTICLE_COUNT=("ARTICLE_COUNT", "sum"),
                     CONTENT_COVERAGE_PCT=("CONTENT_COVERAGE_PCT", "mean"))
                .reset_index()
                .rename(columns={"PUBLISHED_DATE": "PUBLISHED_HOUR"}))
        df["PUBLISHED_HOUR"] = pd.to_datetime(df["PUBLISHED_HOUR"])

    y_col   = "ARTICLE_COUNT" if metric == "Article Count" else "CONTENT_COVERAGE_PCT"
    y_label = "Articles" if metric == "Article Count" else "Content Coverage %"

    # ── Line Chart ────────────────────────────────────────────
    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("PUBLISHED_HOUR:T",
                    title="Time",
                    axis=alt.Axis(format="%b %d %H:%M" if granularity == "Hourly" else "%b %d")),
            y=alt.Y(f"{y_col}:Q", title=y_label),
            color=alt.Color("CATEGORY:N",
                            scale=alt.Scale(
                                domain=["TECHNOLOGY","BUSINESS","SCIENCE",
                                        "HEALTH","SPORTS","ENTERTAINMENT"],
                                range =["#3B82F6","#10B981","#8B5CF6",
                                        "#EF4444","#F59E0B","#EC4899"]
                            )),
            tooltip=["CATEGORY", "PUBLISHED_HOUR:T", f"{y_col}:Q"]
        )
        .properties(height=400)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)

    # ── Summary table ─────────────────────────────────────────
    st.subheader("Category Summary (Last 7 Days)")
    summary = (df.groupby("CATEGORY")
                 .agg(
                     Total_Articles=("ARTICLE_COUNT", "sum"),
                     Avg_per_Hour=("ARTICLE_COUNT", "mean"),
                     Avg_Content_Pct=("CONTENT_COVERAGE_PCT", "mean")
                 )
                 .reset_index()
                 .sort_values("Total_Articles", ascending=False))
    summary["Avg_per_Hour"]    = summary["Avg_per_Hour"].round(1)
    summary["Avg_Content_Pct"] = summary["Avg_Content_Pct"].round(1)
    st.dataframe(summary, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE 3 — TOP SOURCES LEADERBOARD
# ══════════════════════════════════════════════════════════════

elif page == "🏆 Sources":
    st.title("🏆 Publisher Leaderboard")
    st.caption("Which sources produce the most — and best quality — news?")

    df = load_sources()
    if df.empty:
        st.warning("No source data available yet.")
        st.stop()

    df = df[df["CATEGORY"].isin(selected_cats)]

    # ── Controls ──────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        cat_filter = st.selectbox(
            "Filter by Category",
            ["All"] + selected_cats
        )
    with col2:
        top_n = st.slider("Show Top N sources", 5, 30, 15)

    if cat_filter != "All":
        df = df[df["CATEGORY"] == cat_filter]

    df_top = df.groupby("SOURCE_NAME").agg(
        ARTICLE_COUNT        =("ARTICLE_COUNT",         "sum"),
        CONTENT_COVERAGE_PCT =("CONTENT_COVERAGE_PCT",  "mean"),
        AVG_TITLE_WORDS      =("AVG_TITLE_WORDS",       "mean"),
        LATEST_ARTICLE_AT    =("LATEST_ARTICLE_AT",     "max")
    ).reset_index().sort_values("ARTICLE_COUNT", ascending=False).head(top_n)

    # ── Horizontal bar chart ──────────────────────────────────
    chart = (
        alt.Chart(df_top)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("ARTICLE_COUNT:Q", title="Total Articles"),
            y=alt.Y("SOURCE_NAME:N",
                    sort="-x",
                    title="Publisher"),
            color=alt.Color("CONTENT_COVERAGE_PCT:Q",
                            scale=alt.Scale(scheme="blues"),
                            title="Content Coverage %"),
            tooltip=["SOURCE_NAME",
                     alt.Tooltip("ARTICLE_COUNT:Q",        title="Articles"),
                     alt.Tooltip("CONTENT_COVERAGE_PCT:Q", title="Content %",  format=".1f"),
                     alt.Tooltip("AVG_TITLE_WORDS:Q",      title="Avg Title Words", format=".1f")]
        )
        .properties(height=max(300, top_n * 28))
    )
    st.altair_chart(chart, use_container_width=True)

    # ── Detail table ──────────────────────────────────────────
    st.subheader("Full Leaderboard")
    df_top["CONTENT_COVERAGE_PCT"] = df_top["CONTENT_COVERAGE_PCT"].round(1)
    df_top["AVG_TITLE_WORDS"]      = df_top["AVG_TITLE_WORDS"].round(1)
    df_top["LATEST_ARTICLE_AT"]    = pd.to_datetime(
        df_top["LATEST_ARTICLE_AT"]
    ).dt.strftime("%b %d, %H:%M")

    st.dataframe(
        df_top.rename(columns={
            "SOURCE_NAME":           "Publisher",
            "ARTICLE_COUNT":         "Articles",
            "CONTENT_COVERAGE_PCT":  "Content %",
            "AVG_TITLE_WORDS":       "Avg Title Words",
            "LATEST_ARTICLE_AT":     "Latest Article"
        }),
        use_container_width=True,
        hide_index=True
    )


# ══════════════════════════════════════════════════════════════
# PAGE 4 — PUBLISHING PATTERNS HEATMAP
# ══════════════════════════════════════════════════════════════

elif page == "🕐 Publishing Patterns":
    st.title("🕐 Publishing Patterns")
    st.caption("When does news break? Article volume by hour of day and day of week (UTC)")

    df = load_patterns()
    if df.empty:
        st.warning("No pattern data available yet.")
        st.stop()

    # Order days correctly
    day_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    df["DAY_OF_WEEK"] = pd.Categorical(
        df["DAY_OF_WEEK"], categories=day_order, ordered=True
    )
    df = df.sort_values(["DAY_OF_WEEK", "HOUR_OF_DAY"])

    # ── Heatmap ───────────────────────────────────────────────
    heatmap = (
        alt.Chart(df)
        .mark_rect(cornerRadius=3)
        .encode(
            x=alt.X("HOUR_OF_DAY:O",
                    title="Hour of Day (UTC)",
                    axis=alt.Axis(labelAngle=0)),
            y=alt.Y("DAY_OF_WEEK:O",
                    title="Day of Week",
                    sort=day_order),
            color=alt.Color("TOTAL_ARTICLES:Q",
                            scale=alt.Scale(scheme="orangered"),
                            title="Articles"),
            tooltip=[
                alt.Tooltip("DAY_OF_WEEK:O",    title="Day"),
                alt.Tooltip("HOUR_OF_DAY:O",    title="Hour (UTC)"),
                alt.Tooltip("TOTAL_ARTICLES:Q", title="Articles"),
                alt.Tooltip("TOP_CATEGORY:N",   title="Top Category")
            ]
        )
        .properties(height=300, title="Article Volume Heatmap")
    )

    text = (
        alt.Chart(df)
        .mark_text(fontSize=9, color="white")
        .encode(
            x=alt.X("HOUR_OF_DAY:O"),
            y=alt.Y("DAY_OF_WEEK:O", sort=day_order),
            text=alt.Text("TOTAL_ARTICLES:Q")
        )
    )

    st.altair_chart(heatmap + text, use_container_width=True)

    # ── Peak hours insight ────────────────────────────────────
    st.subheader("📊 Peak Publishing Hours")
    col1, col2 = st.columns(2)

    with col1:
        hourly = (df.groupby("HOUR_OF_DAY")["TOTAL_ARTICLES"]
                    .sum().reset_index()
                    .sort_values("TOTAL_ARTICLES", ascending=False))
        peak_hour = int(hourly.iloc[0]["HOUR_OF_DAY"])
        st.metric("🔥 Busiest Hour (UTC)", f"{peak_hour:02d}:00")

        bar = (
            alt.Chart(hourly)
            .mark_bar(color="#F59E0B", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("HOUR_OF_DAY:O", title="Hour (UTC)", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("TOTAL_ARTICLES:Q", title="Articles"),
                tooltip=["HOUR_OF_DAY", "TOTAL_ARTICLES"]
            )
            .properties(height=200, title="Articles by Hour")
        )
        st.altair_chart(bar, use_container_width=True)

    with col2:
        daily = (df.groupby("DAY_OF_WEEK")["TOTAL_ARTICLES"]
                   .sum().reset_index()
                   .sort_values("TOTAL_ARTICLES", ascending=False))
        peak_day = daily.iloc[0]["DAY_OF_WEEK"]
        st.metric("📅 Busiest Day", peak_day)

        bar2 = (
            alt.Chart(daily)
            .mark_bar(color="#8B5CF6", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("DAY_OF_WEEK:O",
                        sort=day_order,
                        title="Day",
                        axis=alt.Axis(labelAngle=0)),
                y=alt.Y("TOTAL_ARTICLES:Q", title="Articles"),
                tooltip=["DAY_OF_WEEK", "TOTAL_ARTICLES"]
            )
            .properties(height=200, title="Articles by Day")
        )
        st.altair_chart(bar2, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# PAGE 5 — QUALITY SCORECARD
# ══════════════════════════════════════════════════════════════

elif page == "🎯 Quality Scorecard":
    st.title("🎯 Content Quality Scorecard")
    st.caption("Which categories have the richest, most complete articles?")

    df = load_quality()
    if df.empty:
        st.warning("No quality data available yet.")
        st.stop()

    df = df[df["CATEGORY"].isin(selected_cats)]

    # ── Quality Score Cards ───────────────────────────────────
    st.subheader("Overall Quality Score by Category")
    st.caption("Score = weighted average of content (40%), description (35%), author (25%) coverage")

    cols = st.columns(len(df))
    category_colors = {
        "TECHNOLOGY":    "#3B82F6",
        "BUSINESS":      "#10B981",
        "SCIENCE":       "#8B5CF6",
        "HEALTH":        "#EF4444",
        "SPORTS":        "#F59E0B",
        "ENTERTAINMENT": "#EC4899"
    }

    for col, (_, row) in zip(cols, df.iterrows()):
        score = float(row["QUALITY_SCORE"]) if pd.notna(row["QUALITY_SCORE"]) else 0
        color = category_colors.get(row["CATEGORY"], "#6B7280")
        emoji = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"

        col.markdown(
            f"""
            <div style='background:{color}18;border:2px solid {color};
                        border-radius:12px;padding:16px;text-align:center'>
                <div style='font-size:28px;font-weight:bold;color:{color}'>{score:.0f}</div>
                <div style='font-size:11px;color:#6B7280'>Quality Score</div>
                <div style='font-size:13px;font-weight:600;margin-top:6px'>{row['CATEGORY'].title()}</div>
                <div style='font-size:11px'>{emoji} {int(row['TOTAL_ARTICLES']):,} articles</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.divider()

    # ── Coverage breakdown chart ──────────────────────────────
    st.subheader("Coverage Breakdown")

    coverage_df = df.melt(
        id_vars    = ["CATEGORY"],
        value_vars = ["CONTENT_COVERAGE_PCT",
                      "DESCRIPTION_COVERAGE_PCT",
                      "AUTHOR_COVERAGE_PCT"],
        var_name   = "Metric",
        value_name = "Coverage_Pct"
    )
    coverage_df["Metric"] = coverage_df["Metric"].map({
        "CONTENT_COVERAGE_PCT":     "Has Full Content",
        "DESCRIPTION_COVERAGE_PCT": "Has Description",
        "AUTHOR_COVERAGE_PCT":      "Has Author"
    })

    bar = (
        alt.Chart(coverage_df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("CATEGORY:N",       title="Category",  axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Coverage_Pct:Q",   title="Coverage %", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("Metric:N", scale=alt.Scale(
                domain=["Has Full Content","Has Description","Has Author"],
                range =["#3B82F6","#10B981","#F59E0B"]
            )),
            xOffset="Metric:N",
            tooltip=["CATEGORY","Metric",
                     alt.Tooltip("Coverage_Pct:Q", format=".1f", title="Coverage %")]
        )
        .properties(height=350)
    )
    st.altair_chart(bar, use_container_width=True)

    # ── Detail metrics table ──────────────────────────────────
    st.subheader("Full Quality Metrics")
    display_df = df[[
        "CATEGORY","TOTAL_ARTICLES","UNIQUE_SOURCES",
        "CONTENT_COVERAGE_PCT","DESCRIPTION_COVERAGE_PCT",
        "AUTHOR_COVERAGE_PCT","AVG_TITLE_WORDS","QUALITY_SCORE"
    ]].copy()

    display_df.columns = [
        "Category","Articles","Sources",
        "Content %","Description %","Author %",
        "Avg Title Words","Quality Score"
    ]
    for col in ["Content %","Description %","Author %","Quality Score"]:
        display_df[col] = display_df[col].round(1)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Quality Score": st.column_config.ProgressColumn(
                "Quality Score",
                min_value=0,
                max_value=100,
                format="%.1f"
            ),
            "Content %": st.column_config.ProgressColumn(
                "Content %",
                min_value=0,
                max_value=100,
                format="%.1f%%"
            )
        }
    )

    # ── Source diversity ──────────────────────────────────────
    st.subheader("📡 Source Diversity per Category")
    diversity = (
        alt.Chart(df)
        .mark_bar(color="#8B5CF6",
                  cornerRadiusTopLeft=3,
                  cornerRadiusTopRight=3)
        .encode(
            x=alt.X("CATEGORY:N",      title="Category", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("UNIQUE_SOURCES:Q", title="Unique Publishers"),
            tooltip=["CATEGORY","UNIQUE_SOURCES","TOTAL_ARTICLES"]
        )
        .properties(height=250)
    )
    st.altair_chart(diversity, use_container_width=True)