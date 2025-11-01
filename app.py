
# app.py
import streamlit as st
from backend import compare_average_rainfall, top_crops_in_state


st.set_page_config(page_title="Project Samarth - Live Q&A", layout='wide')
st.title("🌾 Project Nirikshan — Live Data.gov.in Q&A System")


st.sidebar.markdown("### ⚙️ Configuration")
st.sidebar.info("This app fetches data directly from data.gov.in APIs.")


col1, col2 = st.columns(2)


with col1:
	st.subheader("Compare Average Rainfall Between Two States")
	sx = st.text_input("State X", "Maharashtra")
	sy = st.text_input("State Y", "Kerala")
	n = st.number_input("Last N Years", 1, 20, 5, key="compare_last_n_years")
	if st.button("Compare Rainfall"):
		with st.spinner("Fetching live data from data.gov.in ..."):
			res = compare_average_rainfall(sx, sy, last_n_years=n)
			st.json(res)


with col2:
	st.subheader("Top Crops by Production in a State")
	state = st.text_input("State Name", "Punjab")
	m = st.number_input("Top M Crops", 1, 20, 3)
	y = st.number_input("Last N Years", 1, 20, 5, key="crops_last_n_years")
	if st.button("Get Top Crops"):
		with st.spinner("Fetching live crop production data ..."):
			res = top_crops_in_state(state, top_m=m, last_n_years=y)
			st.json(res)


st.markdown("---")

st.caption("Data fetched live from data.gov.in (Agriculture & IMD Datasets). All results are source-cited.")
