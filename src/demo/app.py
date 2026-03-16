"""
Thai University Admission Checker — Streamlit Demo
Lets students input scores and university choices to see admission chances.

Usage:
    uv run streamlit run src/demo/app.py
"""
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from src.demo.queries import (
    get_university_list,
    get_programs,
    get_admission_stats,
    get_score_range_for_program,
    calculate_admission_chance,
    get_popularity,
    get_all_popularity_ranks,
    get_competition_history,
)

# ─── Page Config ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="เช็กโอกาสติดมหาวิทยาลัย",
    page_icon="🎓",
    layout="wide",
)

st.title("🎓 เช็กโอกาสติดมหาวิทยาลัย")
st.caption("วิเคราะห์โอกาสสอบติดจากข้อมูล TCAS ย้อนหลัง + ความนิยมจาก Wikipedia, YouTube, อว.")


# ─── Load University List ────────────────────────────────────────────
@st.cache_data
def load_universities():
    return get_university_list()

@st.cache_data
def load_popularity_ranks():
    return get_all_popularity_ranks()

uni_df = load_universities()
uni_names = uni_df["name_th"].tolist()

# ─── Sidebar: Student Input ──────────────────────────────────────────
with st.sidebar:
    st.header("📝 กรอกข้อมูลของคุณ")

    student_score = st.number_input(
        "คะแนนของคุณ",
        min_value=0.0,
        max_value=30000.0,
        value=60.0,
        step=0.5,
        help="กรอกคะแนนรวมที่ใช้สมัคร TCAS — บางสาขาใช้สเกล 0-100, บางสาขาใช้คะแนนรวมถ่วงน้ำหนัก (หลักพัน-หมื่น). ดูช่วงคะแนนอ้างอิงหลังกดวิเคราะห์",
    )

    st.divider()
    st.subheader("🏫 เลือกมหาวิทยาลัย (1-3 อันดับ)")

    choices = []
    for i in range(1, 4):
        uni_choice = st.selectbox(
            f"อันดับ {i}",
            options=["-- ไม่เลือก --"] + uni_names,
            key=f"uni_{i}",
        )
        if uni_choice != "-- ไม่เลือก --":
            uni_id = int(uni_df[uni_df["name_th"] == uni_choice]["university_id"].iloc[0])

            # Get programs for this university
            programs = get_programs(uni_id)
            if programs:
                prog_choice = st.selectbox(
                    f"สาขาวิชา (อันดับ {i})",
                    options=["-- ทุกสาขา --"] + programs,
                    key=f"prog_{i}",
                )
                prog = prog_choice if prog_choice != "-- ทุกสาขา --" else None
            else:
                prog = None

            choices.append({"rank": i, "name": uni_choice, "id": uni_id, "program": prog})

    analyze = st.button("🔍 วิเคราะห์", type="primary", use_container_width=True)


# ─── Main Content ────────────────────────────────────────────────────
if not analyze or not choices:
    # Show welcome screen
    st.info("👈 กรอกคะแนนและเลือกมหาวิทยาลัยทางด้านซ้าย แล้วกด **วิเคราะห์**")

    st.divider()
    st.subheader("📊 อันดับความนิยมมหาวิทยาลัย (Top 20)")
    ranks = load_popularity_ranks()
    top20 = ranks.head(20)[["rank", "name_th", "wikipedia", "mhesi", "tcas", "popularity_score"]].copy()
    top20.columns = ["อันดับ", "มหาวิทยาลัย", "Wikipedia Views", "นักศึกษา (อว.)", "ผู้สมัคร TCAS", "คะแนนนิยม"]
    top20["คะแนนนิยม"] = top20["คะแนนนิยม"].round(1)
    st.dataframe(top20, hide_index=True, width="stretch")

else:
    # ── Analysis Results ─────────────────────────────────────────
    comparison_data = []

    for choice in choices:
        uni_id = choice["id"]
        uni_name = choice["name"]
        program = choice["program"]

        st.divider()
        st.header(f"{'🥇🥈🥉'[choice['rank']-1]} อันดับ {choice['rank']}: {uni_name}")
        if program:
            st.caption(f"สาขา: {program}")

        # Get data
        stats = get_admission_stats(uni_id, program)
        pop = get_popularity(uni_id)
        ranks = load_popularity_ranks()
        uni_rank = ranks[ranks["university_id"] == uni_id]

        # ── Row 1: Admission Chance + Competition ────────────────
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 โอกาสที่จะติด")
            chance = calculate_admission_chance(student_score, stats)

            if chance["chance_pct"] is not None:
                # Gauge chart
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=chance["chance_pct"],
                    number={"suffix": "%", "font": {"size": 48}},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": "#2ecc71" if chance["chance_pct"] >= 60
                                else "#f39c12" if chance["chance_pct"] >= 30
                                else "#e74c3c"},
                        "steps": [
                            {"range": [0, 30], "color": "#fadbd8"},
                            {"range": [30, 60], "color": "#fdebd0"},
                            {"range": [60, 100], "color": "#d5f5e3"},
                        ],
                    },
                    title={"text": chance["level"], "font": {"size": 20}},
                ))
                fig.update_layout(height=280, margin=dict(t=60, b=10, l=30, r=30))
                st.plotly_chart(fig, width="stretch", key=f"gauge_{choice['rank']}_{uni_id}")
                st.caption(chance["description"])

                # Score reference
                if stats:
                    st.markdown(f"""
                    | | คะแนน |
                    |---|---|
                    | **คะแนนของคุณ** | **{student_score:.1f}** |
                    | คะแนนต่ำสุดเฉลี่ย | {stats['score_min']:.1f} |
                    | คะแนนสูงสุดเฉลี่ย | {stats['score_max']:.1f} |
                    """)
            else:
                st.warning("ไม่มีข้อมูลคะแนนเพียงพอสำหรับสาขานี้")

        with col2:
            st.subheader("⚔️ อัตราการแข่งขัน")
            if stats:
                ratio = stats["competition_ratio"]
                st.metric(
                    "อัตราแข่งขัน",
                    f"{ratio:.1f} : 1",
                    help="จำนวนผู้สมัครต่อ 1 ที่นั่ง",
                )
                st.metric("ผู้สมัครทั้งหมด", f"{stats['total_applicants']:,} คน")
                st.metric("ที่นั่งทั้งหมด", f"{stats['total_seats']:,} ที่นั่ง")
                st.caption(f"จากข้อมูล {stats['num_rounds']} รอบ TCAS, {stats['num_programs']} หลักสูตร")

                # Competition level
                if ratio > 10:
                    st.error("🔴 การแข่งขันสูงมาก")
                elif ratio > 5:
                    st.warning("🟡 การแข่งขันสูง")
                elif ratio > 2:
                    st.info("🔵 การแข่งขันปานกลาง")
                else:
                    st.success("🟢 การแข่งขันต่ำ")
            else:
                st.warning("ไม่มีข้อมูลการแข่งขัน")

        # ── Row 2: Popularity + Competition History ──────────────
        col3, col4 = st.columns(2)

        with col3:
            st.subheader("🌟 ความนิยม")
            if not uni_rank.empty:
                rank_val = int(uni_rank.iloc[0]["rank"])
                score_val = round(uni_rank.iloc[0]["popularity_score"], 1)
                st.metric("อันดับความนิยม", f"#{rank_val} / 57", help="จาก 57 มหาวิทยาลัย")
                st.metric("คะแนนนิยม", f"{score_val} / 100")

            sub_col1, sub_col2 = st.columns(2)
            with sub_col1:
                if pop["wikipedia_views"] > 0:
                    st.metric("📖 Wikipedia", f"{pop['wikipedia_views']:,} views")
                if pop["youtube_views"] > 0:
                    st.metric("▶️ YouTube", f"{pop['youtube_views']:,} views")
            with sub_col2:
                if pop["mhesi_students"] > 0:
                    st.metric("👨‍🎓 นักศึกษา (อว.)", f"{pop['mhesi_students']:,} คน")
                if pop["tcas_applicants"] > 0:
                    st.metric("📋 ผู้สมัคร TCAS รวม", f"{pop['tcas_applicants']:,} คน")

        with col4:
            st.subheader("📉 แนวโน้มการแข่งขัน")
            history = get_competition_history(uni_id)
            if not history.empty and len(history) > 1:
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Bar(
                    x=history["tcas_round_name"],
                    y=history["applicants"],
                    name="ผู้สมัคร",
                    marker_color="#e74c3c",
                ))
                fig_hist.add_trace(go.Bar(
                    x=history["tcas_round_name"],
                    y=history["seats"],
                    name="ที่นั่ง",
                    marker_color="#3498db",
                ))
                fig_hist.update_layout(
                    barmode="group",
                    height=300,
                    margin=dict(t=10, b=10, l=10, r=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig_hist, width="stretch", key=f"history_{choice['rank']}_{uni_id}")
            else:
                st.info("ข้อมูลไม่เพียงพอสำหรับแสดงแนวโน้ม")

        # Collect for comparison
        comparison_data.append({
            "อันดับ": f"อันดับ {choice['rank']}",
            "มหาวิทยาลัย": uni_name,
            "สาขา": program or "ทุกสาขา",
            "คะแนนต่ำสุด": f"{stats['score_min']:.1f}" if stats and stats.get('score_min') else "-",
            "คะแนนสูงสุด": f"{stats['score_max']:.1f}" if stats and stats.get('score_max') else "-",
            "อัตราแข่งขัน": f"{stats['competition_ratio']:.1f}:1" if stats else "-",
            "โอกาส": f"{chance['chance_pct']}%" if chance.get("chance_pct") else "-",
            "ความนิยม": f"#{int(uni_rank.iloc[0]['rank'])}" if not uni_rank.empty else "-",
        })

    # ── Comparison Table ─────────────────────────────────────────
    if len(comparison_data) >= 2:
        st.divider()
        st.header("📊 ตารางเปรียบเทียบ")
        comp_df = pd.DataFrame(comparison_data)
        st.dataframe(comp_df, hide_index=True, width="stretch")

    # ── Recommendation ───────────────────────────────────────────
    st.divider()
    st.header("💡 คำแนะนำ")

    # Sort by chance percentage
    ranked = sorted(
        comparison_data,
        key=lambda x: float(x["โอกาส"].replace("%", "")) if x["โอกาส"] != "-" else 0,
        reverse=True,
    )

    for i, r in enumerate(ranked):
        chance_str = r["โอกาส"]
        uni_name = r["มหาวิทยาลัย"]
        if chance_str != "-":
            chance_val = float(chance_str.replace("%", ""))
            if chance_val >= 60:
                st.success(f"✅ **{uni_name}** — โอกาส {chance_str} — แนะนำ! คะแนนของคุณอยู่ในเกณฑ์ดี")
            elif chance_val >= 30:
                st.warning(f"⚠️ **{uni_name}** — โอกาส {chance_str} — พอมีโอกาส แต่ควรเตรียมตัวเพิ่ม")
            else:
                st.error(f"❌ **{uni_name}** — โอกาส {chance_str} — โอกาสน้อย ควรพิจารณาตัวเลือกสำรอง")
        else:
            st.info(f"ℹ️ **{uni_name}** — ไม่มีข้อมูลเพียงพอสำหรับประเมิน")

    st.caption("⚠️ ข้อมูลนี้เป็นการประมาณจากสถิติ TCAS ย้อนหลังเท่านั้น ไม่ได้เป็นการรับประกันผลสอบจริง")
