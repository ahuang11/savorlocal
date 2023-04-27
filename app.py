from pathlib import Path
from typing import List, Iterator, Tuple
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import col
from geopy import Nominatim, distance


DATABASE = "FREE_COMPANY_DATASET"
TABLE = "FREECOMPANYDATASET"


@st.cache_data
def list_industries() -> List:
    return [
        industry.title() for industry in Path("industry.txt").read_text().splitlines()
    ]


def list_sizes() -> List:
    return Path("size.txt").read_text().splitlines()


def get_city_state(address: str) -> Tuple[str, str]:
    geolocator = st.session_state["geolocator"]
    location = geolocator.geocode(address, addressdetails=True)
    st.session_state["address"] = location.address
    st.session_state["coordinates"] = (location.latitude, location.longitude)
    geolocated_address = location.raw["address"]
    return geolocated_address["city"], geolocated_address["state"]


def query_table(
    industry: str,
    size: int,
    locality: str,
    region: str,
) -> Iterator[Row]:
    table = st.session_state["table"]
    filters = [
        col("industry") == industry.lower(),
        col("size") == size,
        col("locality") == locality.lower(),
        col("region") == region.lower(),
    ]
    for filter in filters:
        table = table.filter(filter)
    return table.to_local_iterator()


def display_results(rows: Iterator[Row]):
    count = 0
    for row in rows:
        linkedin = row.LINKEDIN_URL
        website = row.WEBSITE or linkedin
        since = f" since {row.FOUNDED}" if row.FOUNDED else ""
        st.markdown(f"{row.NAME.title()}{since} - [{website}](https://www.{website})")
        count += 1
    if count == 0:
        st.warning("No results found!")

if "table" not in st.session_state:
    session = Session.builder.configs(
        dict(database=DATABASE, **st.secrets["snowflake"])
    ).create()
    table = session.table(TABLE)
    st.session_state["table"] = table


if "geolocator" not in st.session_state:
    st.session_state["geolocator"] = Nominatim(user_agent="savorlocal")

with st.sidebar:
    st.image("logo.png",)
    st.title("Savorlocal")
    address = st.text_input(
        "📍 Address",
        value="98109",
        placeholder="Zip / City, State",
        help="Enter an address, zip code or city and state!",
    )
    industry = st.selectbox("🏭 Industry", list_industries())
    size = st.selectbox("👥 Company Size", options=list_sizes(), index=3)
    submit = st.button("🔍 Discover", use_container_width=True)

if submit:
    if address.strip() == "":
        st.sidebar.error("Please enter an address!")
        st.stop()
    city, state = get_city_state(address)
    rows = query_table(industry=industry, size=size, locality=city, region=state)
    display_results(rows)
