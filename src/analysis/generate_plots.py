"""
Generate comprehensive analytical visualizations from the SQLite database.
Saves plots to the 'output_plots' directory.

Plots generated:
  01 - YouTube: Top 15 universities by total views
  02 - TCAS: Top 15 universities by total applicants
  03 - MHESI: Top 15 universities by student enrollment
  04 - Wikipedia: Top 15 universities by pageviews
  05 - Cross-source: YouTube views vs TCAS applicants (scatter)
  06 - TCAS: Competition ratio (applicants / seats) top 15
  07 - TCAS: Applicant trends across rounds (line chart)
  08 - YouTube: Video count growth by year (bar)
  09 - YouTube: Engagement rate (likes per view) top 15
  10 - Wikipedia: Monthly pageview trends for top 5 universities (line)
  11 - Wikipedia vs MHESI: Pageviews vs enrollment (scatter)
  12 - Cross-source: Heatmap ranking across all sources
  13 - University type comparison across all metrics (grouped bar)
"""
import os
import logging
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from src.config import DB_PATH, OUTPUT_DIR

log = logging.getLogger(__name__)

# Configure Matplotlib for Thai Language
# NOTE: seaborn set_style resets font.family to sans-serif, so we must set fonts AFTER it.
sns.set_style("whitegrid")
plt.rcParams.update({
    'font.family': 'Tahoma',
    'axes.unicode_minus': False,
})


def _conn():
    return sqlite3.connect(DB_PATH)


def _short(name: str) -> str:
    """Shorten Thai university name for plot labels."""
    return (name
            .replace("มหาวิทยาลัยเทคโนโลยีราชมงคล", "ราชมงคล")
            .replace("มหาวิทยาลัยเทคโนโลยีพระจอมเกล้า", "พจ.")
            .replace("สถาบันเทคโนโลยีพระจอมเกล้าเจ้าคุณทหารลาดกระบัง", "สจล.")
            .replace("สถาบันบัณฑิตพัฒนบริหารศาสตร์", "นิด้า")
            .replace("มหาวิทยาลัยราชภัฏ", "ราชภัฏ")
            .replace("มหาวิทยาลัย", "ม.")
            )


def _save(fig, filename):
    fig.tight_layout()
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    log.info("  Saved %s", filename)


# ═══════════════════════════════════════════════════════════════
# PLOT 01: YouTube - Top 15 by total views
# ═══════════════════════════════════════════════════════════════
def plot_01_youtube_views():
    log.info("[01] YouTube: Top 15 by views")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th, SUM(y.view_count) AS total_views, COUNT(*) AS total_videos
        FROM fact_youtube_engagement y
        JOIN dim_university u ON y.university_id = u.university_id
        GROUP BY u.university_id ORDER BY total_views DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df['views_m'] = df['total_views'] / 1e6
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short'], df['views_m'], color=sns.color_palette('viridis', len(df)))
    ax.set_xlabel('ยอดวิวรวม (ล้านวิว)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — ยอดวิว YouTube สูงสุด', fontsize=16, pad=15)
    for bar, v in zip(bars, df['views_m']):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                f'{v:,.1f}M', va='center', fontsize=9)
    _save(fig, "01_youtube_views.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 02: TCAS - Top 15 by total applicants
# ═══════════════════════════════════════════════════════════════
def plot_02_tcas_applicants():
    log.info("[02] TCAS: Top 15 by applicants")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th, SUM(t.applicants) AS total_app
        FROM fact_tcas_admission t
        JOIN dim_university u ON t.university_id = u.university_id
        GROUP BY u.university_id ORDER BY total_app DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short'], df['total_app'], color=sns.color_palette('flare', len(df)))
    ax.set_xlabel('จำนวนผู้สมัครรวมทุกรอบ (คน)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — ผู้สมัคร TCAS มากที่สุด', fontsize=16, pad=15)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
    for bar, v in zip(bars, df['total_app']):
        ax.text(bar.get_width() + 5000, bar.get_y() + bar.get_height()/2,
                f'{v:,.0f}', va='center', fontsize=9)
    _save(fig, "02_tcas_applicants.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 03: MHESI - Top 15 by enrollment
# ═══════════════════════════════════════════════════════════════
def plot_03_mhesi_enrollment():
    log.info("[03] MHESI: Top 15 enrollment")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th, MAX(m.total_students) AS students
        FROM fact_mhesi_enrollment m
        JOIN dim_university u ON m.university_id = u.university_id
        WHERE m.total_students > 0
        GROUP BY u.university_id ORDER BY students DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short'], df['students'], color=sns.color_palette('crest', len(df)))
    ax.set_xlabel('จำนวนนักศึกษา (คน)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — จำนวนนักศึกษามากที่สุด (อว.)', fontsize=16, pad=15)
    for bar, v in zip(bars, df['students']):
        ax.text(bar.get_width() + 200, bar.get_y() + bar.get_height()/2,
                f'{v:,.0f}', va='center', fontsize=9)
    _save(fig, "03_mhesi_enrollment.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 04: Wikipedia - Top 15 by pageviews
# ═══════════════════════════════════════════════════════════════
def plot_04_wikipedia_pageviews():
    log.info("[04] Wikipedia: Top 15 pageviews")
    conn = _conn()
    if conn.execute("SELECT COUNT(*) FROM fact_wikipedia_pageviews").fetchone()[0] == 0:
        conn.close(); return

    df = pd.read_sql("""
        SELECT u.name_th, SUM(w.pageviews) AS total_pv
        FROM fact_wikipedia_pageviews w
        JOIN dim_university u ON w.university_id = u.university_id
        GROUP BY u.university_id ORDER BY total_pv DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df['pv_k'] = df['total_pv'] / 1e3
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short'], df['pv_k'], color=sns.color_palette('mako', len(df)))
    ax.set_xlabel('จำนวนการเข้าชม Wikipedia รวม (พัน)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — ยอดเข้าชม Wikipedia สูงสุด', fontsize=16, pad=15)
    for bar, v in zip(bars, df['pv_k']):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                f'{v:,.0f}K', va='center', fontsize=9)
    _save(fig, "04_wikipedia_pageviews.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 05: Cross-source scatter — YouTube vs TCAS
# ═══════════════════════════════════════════════════════════════
def plot_05_youtube_vs_tcas():
    log.info("[05] Cross: YouTube vs TCAS scatter")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th,
            yt.total_views, tcas.total_app
        FROM dim_university u
        JOIN (SELECT university_id, SUM(view_count) AS total_views FROM fact_youtube_engagement GROUP BY university_id) yt
            ON yt.university_id = u.university_id
        JOIN (SELECT university_id, SUM(applicants) AS total_app FROM fact_tcas_admission GROUP BY university_id) tcas
            ON tcas.university_id = u.university_id
    """, conn)
    conn.close()
    if len(df) < 2: return

    df['views_m'] = df['total_views'] / 1e6
    df['short'] = df['name_th'].apply(_short)

    fig, ax = plt.subplots(figsize=(14, 10))
    scatter = ax.scatter(df['total_app'], df['views_m'], s=150, alpha=0.7,
                         c=df['views_m'], cmap='coolwarm', edgecolors='gray', linewidth=0.5)

    med_app, med_view = df['total_app'].median(), df['views_m'].median()
    ax.axvline(x=med_app, color='gray', ls='--', alpha=0.4)
    ax.axhline(y=med_view, color='gray', ls='--', alpha=0.4)

    for _, row in df.iterrows():
        if row['views_m'] > med_view or row['total_app'] > med_app:
            ax.annotate(row['short'], (row['total_app'], row['views_m']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8)

    ax.set_xlabel('ผู้สมัคร TCAS รวม (คน)', fontsize=12)
    ax.set_ylabel('ยอดวิว YouTube รวม (ล้านวิว)', fontsize=12)
    ax.set_title('ความสนใจออนไลน์ (YouTube) vs ความนิยมสมัครเรียน (TCAS)', fontsize=16, pad=15)
    plt.colorbar(scatter, label='YouTube Views (M)')
    _save(fig, "05_youtube_vs_tcas.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 06: TCAS competition ratio (applicants / seats)
# ═══════════════════════════════════════════════════════════════
def plot_06_tcas_competition_ratio():
    log.info("[06] TCAS: Competition ratio")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th,
            SUM(t.applicants) AS total_app,
            SUM(t.seats_available) AS total_seats,
            CAST(SUM(t.applicants) AS REAL) / NULLIF(SUM(t.seats_available), 0) AS ratio
        FROM fact_tcas_admission t
        JOIN dim_university u ON t.university_id = u.university_id
        WHERE t.seats_available > 0
        GROUP BY u.university_id
        HAVING ratio IS NOT NULL
        ORDER BY ratio DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    colors = sns.color_palette('YlOrRd', len(df))
    bars = ax.barh(df['short'], df['ratio'], color=colors)
    ax.set_xlabel('อัตราการแข่งขัน (ผู้สมัคร / ที่นั่ง)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — อัตราการแข่งขัน TCAS สูงสุด', fontsize=16, pad=15)
    for bar, v in zip(bars, df['ratio']):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                f'{v:.1f}x', va='center', fontsize=9)
    _save(fig, "06_tcas_competition_ratio.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 07: TCAS applicant trends across rounds (line)
# ═══════════════════════════════════════════════════════════════
def plot_07_tcas_trends():
    log.info("[07] TCAS: Applicant trends")
    conn = _conn()
    df = pd.read_sql("""
        SELECT tcas_round_name, SUM(applicants) AS total_app, SUM(seats_available) AS total_seats
        FROM fact_tcas_admission
        GROUP BY tcas_round_name
        ORDER BY tcas_round_name
    """, conn)
    conn.close()
    if df.empty: return

    # Shorten round names
    df['round'] = df['tcas_round_name'].str.replace('รอบ 3 ครั้งที่', 'ร3-').str.strip()
    df['app_k'] = df['total_app'] / 1e3
    df['seats_k'] = df['total_seats'] / 1e3

    fig, ax = plt.subplots(figsize=(12, 6))
    x = range(len(df))
    ax.bar(x, df['app_k'], width=0.4, label='ผู้สมัคร', color='#e74c3c', align='center')
    ax.bar([i + 0.4 for i in x], df['seats_k'], width=0.4, label='ที่นั่ง', color='#3498db', align='center')
    ax.set_xticks([i + 0.2 for i in x])
    ax.set_xticklabels(df['round'], rotation=30, ha='right', fontsize=9)
    ax.set_ylabel('จำนวน (พัน)', fontsize=12)
    ax.set_title('แนวโน้มจำนวนผู้สมัคร vs ที่นั่งรับ ในแต่ละรอบ TCAS', fontsize=16, pad=15)
    ax.legend(fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}K'))
    _save(fig, "07_tcas_trends.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 08: YouTube video count growth by year
# ═══════════════════════════════════════════════════════════════
def plot_08_youtube_yearly_growth():
    log.info("[08] YouTube: Yearly growth")
    conn = _conn()
    df = pd.read_sql("""
        SELECT publish_year AS year, COUNT(*) AS videos, SUM(view_count) AS views
        FROM fact_youtube_engagement
        WHERE publish_year >= 2015 AND publish_year <= 2025
        GROUP BY publish_year ORDER BY publish_year
    """, conn)
    conn.close()
    if df.empty: return

    fig, ax1 = plt.subplots(figsize=(12, 6))
    color1, color2 = '#2ecc71', '#e74c3c'

    bars = ax1.bar(df['year'], df['videos'], color=color1, alpha=0.7, label='จำนวนวิดีโอ')
    ax1.set_xlabel('ปี', fontsize=12)
    ax1.set_ylabel('จำนวนวิดีโอ', fontsize=12, color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)

    ax2 = ax1.twinx()
    ax2.plot(df['year'], df['views'] / 1e6, color=color2, marker='o', linewidth=2, label='ยอดวิวรวม (ล้าน)')
    ax2.set_ylabel('ยอดวิวรวม (ล้านวิว)', fontsize=12, color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)

    ax1.set_title('การเติบโตของวิดีโอมหาวิทยาลัยบน YouTube (2015-2025)', fontsize=16, pad=15)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    _save(fig, "08_youtube_yearly_growth.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 09: YouTube engagement rate (likes/views)
# ═══════════════════════════════════════════════════════════════
def plot_09_youtube_engagement_rate():
    log.info("[09] YouTube: Engagement rate")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th,
            SUM(y.view_count) AS views,
            SUM(y.like_count) AS likes,
            SUM(y.comment_count) AS comments,
            CAST(SUM(y.like_count) AS REAL) / NULLIF(SUM(y.view_count), 0) * 100 AS like_rate
        FROM fact_youtube_engagement y
        JOIN dim_university u ON y.university_id = u.university_id
        GROUP BY u.university_id
        HAVING views > 1000
        ORDER BY like_rate DESC LIMIT 15
    """, conn)
    conn.close()
    if df.empty: return

    df['short'] = df['name_th'].apply(_short)
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['short'], df['like_rate'], color=sns.color_palette('rocket', len(df)))
    ax.set_xlabel('อัตราการกดไลค์ (% ของยอดวิว)', fontsize=12)
    ax.set_title('Top 15 มหาวิทยาลัย — อัตราการมีส่วนร่วมบน YouTube สูงสุด', fontsize=16, pad=15)
    for bar, v in zip(bars, df['like_rate']):
        ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                f'{v:.2f}%', va='center', fontsize=9)
    _save(fig, "09_youtube_engagement_rate.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 10: Wikipedia monthly trends for top 5 universities
# ═══════════════════════════════════════════════════════════════
def plot_10_wikipedia_monthly_trends():
    log.info("[10] Wikipedia: Monthly trends top 5")
    conn = _conn()
    if conn.execute("SELECT COUNT(*) FROM fact_wikipedia_pageviews").fetchone()[0] == 0:
        conn.close(); return

    # Get top 5
    top5 = pd.read_sql("""
        SELECT w.university_id, u.name_th
        FROM fact_wikipedia_pageviews w
        JOIN dim_university u ON w.university_id = u.university_id
        GROUP BY w.university_id ORDER BY SUM(w.pageviews) DESC LIMIT 5
    """, conn)

    ids = ",".join(str(i) for i in top5['university_id'])
    df = pd.read_sql(f"""
        SELECT w.university_id, u.name_th, w.year, w.month, SUM(w.pageviews) AS pv
        FROM fact_wikipedia_pageviews w
        JOIN dim_university u ON w.university_id = u.university_id
        WHERE w.university_id IN ({ids})
        GROUP BY w.university_id, w.year, w.month
        ORDER BY w.year, w.month
    """, conn)
    conn.close()
    if df.empty: return

    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2) + '-01')
    df['short'] = df['name_th'].apply(_short)

    fig, ax = plt.subplots(figsize=(14, 7))
    for name in df['short'].unique():
        sub = df[df['short'] == name]
        ax.plot(sub['date'], sub['pv'], marker='o', markersize=4, linewidth=2, label=name)

    ax.set_xlabel('เดือน', fontsize=12)
    ax.set_ylabel('จำนวนการเข้าชม Wikipedia', fontsize=12)
    ax.set_title('แนวโน้มการเข้าชม Wikipedia รายเดือน — Top 5 มหาวิทยาลัย', fontsize=16, pad=15)
    ax.legend(fontsize=10, loc='upper right')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1e3:.0f}K'))
    fig.autofmt_xdate()
    _save(fig, "10_wikipedia_monthly_trends.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 11: Wikipedia vs MHESI scatter
# ═══════════════════════════════════════════════════════════════
def plot_11_wikipedia_vs_mhesi():
    log.info("[11] Cross: Wikipedia vs MHESI")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th,
            wiki.total_pv, mhesi.students
        FROM dim_university u
        JOIN (SELECT university_id, SUM(pageviews) AS total_pv FROM fact_wikipedia_pageviews GROUP BY university_id) wiki
            ON wiki.university_id = u.university_id
        JOIN (SELECT university_id, MAX(total_students) AS students FROM fact_mhesi_enrollment WHERE total_students > 0 GROUP BY university_id) mhesi
            ON mhesi.university_id = u.university_id
    """, conn)
    conn.close()
    if len(df) < 3: return

    df['short'] = df['name_th'].apply(_short)
    df['pv_k'] = df['total_pv'] / 1e3

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.scatter(df['students'], df['pv_k'], s=120, alpha=0.7, c=df['pv_k'], cmap='viridis', edgecolors='gray')

    for _, row in df.iterrows():
        if row['pv_k'] > df['pv_k'].quantile(0.6) or row['students'] > df['students'].quantile(0.6):
            ax.annotate(row['short'], (row['students'], row['pv_k']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8)

    ax.set_xlabel('จำนวนนักศึกษา (คน)', fontsize=12)
    ax.set_ylabel('จำนวนเข้าชม Wikipedia (พัน)', fontsize=12)
    ax.set_title('ขนาดมหาวิทยาลัย (นักศึกษา) vs ความสนใจ (Wikipedia)', fontsize=16, pad=15)
    _save(fig, "11_wikipedia_vs_mhesi.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 12: Cross-source heatmap ranking
# ═══════════════════════════════════════════════════════════════
def plot_12_cross_source_heatmap():
    log.info("[12] Cross: Heatmap ranking")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.name_th,
            COALESCE(yt.total_views, 0) AS youtube,
            COALESCE(tcas.total_app, 0) AS tcas,
            COALESCE(mhesi.students, 0) AS mhesi,
            COALESCE(wiki.total_pv, 0) AS wikipedia
        FROM dim_university u
        LEFT JOIN (SELECT university_id, SUM(view_count) AS total_views FROM fact_youtube_engagement GROUP BY university_id) yt ON yt.university_id = u.university_id
        LEFT JOIN (SELECT university_id, SUM(applicants) AS total_app FROM fact_tcas_admission GROUP BY university_id) tcas ON tcas.university_id = u.university_id
        LEFT JOIN (SELECT university_id, MAX(total_students) AS students FROM fact_mhesi_enrollment GROUP BY university_id) mhesi ON mhesi.university_id = u.university_id
        LEFT JOIN (SELECT university_id, SUM(pageviews) AS total_pv FROM fact_wikipedia_pageviews GROUP BY university_id) wiki ON wiki.university_id = u.university_id
    """, conn)
    conn.close()

    # Keep only universities with at least 2 sources of data
    df['sources'] = (df[['youtube', 'tcas', 'mhesi', 'wikipedia']] > 0).sum(axis=1)
    df = df[df['sources'] >= 2].copy()
    if len(df) < 5: return

    # Compute rank for each column (rank 1 = highest)
    for col in ['youtube', 'tcas', 'mhesi', 'wikipedia']:
        df[f'{col}_rank'] = df[col].rank(ascending=False)

    # Average rank and take top 20
    df['avg_rank'] = df[['youtube_rank', 'tcas_rank', 'mhesi_rank', 'wikipedia_rank']].mean(axis=1)
    df = df.nsmallest(20, 'avg_rank')
    df['short'] = df['name_th'].apply(_short)

    rank_cols = ['youtube_rank', 'tcas_rank', 'mhesi_rank', 'wikipedia_rank']
    heatmap_data = df.set_index('short')[rank_cols]
    heatmap_data.columns = ['YouTube', 'TCAS', 'MHESI', 'Wikipedia']

    fig, ax = plt.subplots(figsize=(10, 12))
    sns.heatmap(heatmap_data, annot=True, fmt='.0f', cmap='YlOrRd_r',
                linewidths=0.5, ax=ax, cbar_kws={'label': 'อันดับ (1 = สูงสุด)'})
    ax.set_title('อันดับมหาวิทยาลัยจาก 4 แหล่งข้อมูล (Top 20)', fontsize=16, pad=15)
    ax.set_ylabel('')
    _save(fig, "12_cross_source_heatmap.png")


# ═══════════════════════════════════════════════════════════════
# PLOT 13: University type comparison
# ═══════════════════════════════════════════════════════════════
def plot_13_university_type_comparison():
    log.info("[13] University type comparison")
    conn = _conn()
    df = pd.read_sql("""
        SELECT u.university_id, u.name_th,
            COALESCE(yt.total_views, 0) AS youtube,
            COALESCE(tcas.total_app, 0) AS tcas,
            COALESCE(mhesi.students, 0) AS mhesi,
            COALESCE(wiki.total_pv, 0) AS wikipedia
        FROM dim_university u
        LEFT JOIN (SELECT university_id, SUM(view_count) AS total_views FROM fact_youtube_engagement GROUP BY university_id) yt ON yt.university_id = u.university_id
        LEFT JOIN (SELECT university_id, SUM(applicants) AS total_app FROM fact_tcas_admission GROUP BY university_id) tcas ON tcas.university_id = u.university_id
        LEFT JOIN (SELECT university_id, MAX(total_students) AS students FROM fact_mhesi_enrollment GROUP BY university_id) mhesi ON mhesi.university_id = u.university_id
        LEFT JOIN (SELECT university_id, SUM(pageviews) AS total_pv FROM fact_wikipedia_pageviews GROUP BY university_id) wiki ON wiki.university_id = u.university_id
    """, conn)
    conn.close()

    # Classify university type based on name
    def classify(name):
        if 'ราชภัฏ' in name: return 'ราชภัฏ'
        if 'ราชมงคล' in name: return 'ราชมงคล'
        if any(x in name for x in ['รังสิต', 'กรุงเทพ', 'หอการค้า', 'ธุรกิจบัณฑิต', 'อัสสัมชัญ', 'สยาม', 'ศรีปทุม', 'อีสเทิร์น', 'เกริก', 'เนชั่น']):
            return 'เอกชน'
        return 'รัฐ/วิจัย'

    df['type'] = df['name_th'].apply(classify)

    # Average per type
    agg = df.groupby('type').agg(
        youtube=('youtube', 'mean'),
        tcas=('tcas', 'mean'),
        mhesi=('mhesi', 'mean'),
        wikipedia=('wikipedia', 'mean'),
        count=('university_id', 'count'),
    ).reset_index()

    # Normalize each metric to 0-100 for comparison
    for col in ['youtube', 'tcas', 'mhesi', 'wikipedia']:
        max_val = agg[col].max()
        if max_val > 0:
            agg[f'{col}_norm'] = agg[col] / max_val * 100

    fig, ax = plt.subplots(figsize=(12, 7))
    x = np.arange(len(agg))
    width = 0.2
    metrics = [('youtube_norm', 'YouTube', '#e74c3c'),
               ('tcas_norm', 'TCAS', '#3498db'),
               ('mhesi_norm', 'MHESI', '#2ecc71'),
               ('wikipedia_norm', 'Wikipedia', '#9b59b6')]

    for i, (col, label, color) in enumerate(metrics):
        if col in agg.columns:
            ax.bar(x + i * width, agg[col], width, label=label, color=color, alpha=0.8)

    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels([f"{t}\n({c} แห่ง)" for t, c in zip(agg['type'], agg['count'])], fontsize=11)
    ax.set_ylabel('คะแนนเปรียบเทียบ (Normalized 0-100)', fontsize=12)
    ax.set_title('เปรียบเทียบประเภทมหาวิทยาลัยจาก 4 แหล่งข้อมูล', fontsize=16, pad=15)
    ax.legend(fontsize=11)
    _save(fig, "13_university_type_comparison.png")


# ═══════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════
def generate_all_plots():
    """Generate all analytical plots."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    plot_01_youtube_views()
    plot_02_tcas_applicants()
    plot_03_mhesi_enrollment()
    plot_04_wikipedia_pageviews()
    plot_05_youtube_vs_tcas()
    plot_06_tcas_competition_ratio()
    plot_07_tcas_trends()
    plot_08_youtube_yearly_growth()
    plot_09_youtube_engagement_rate()
    plot_10_wikipedia_monthly_trends()
    plot_11_wikipedia_vs_mhesi()
    plot_12_cross_source_heatmap()
    plot_13_university_type_comparison()

    log.info("All 13 plots generated in %s", OUTPUT_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("  Generating Comprehensive Data Engineering Insights")
    print("=" * 60)
    generate_all_plots()
    print("=" * 60)
    print("  Done! Check the 'output_plots/' directory.")
