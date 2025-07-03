import streamlit as st
import pandas as pd
from utils.menuplanningquery import (
    run_sales_cogs_by_slot_raw,
    process_sales_cogs_data,
    run_recipes_raw_data,
    process_recipe_data,
    process_recipe_data_calc
)

from utils.commonquery import (
    fetch_hellofresh_weeks,
    blank_repeats
)

from datetime import datetime,timedelta
from streamlit_autorefresh import st_autorefresh

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance Portal",
    page_icon=":bulb:",
    layout="wide"
)

@st.cache_data(show_spinner="Loading sales data...", persist=True)
def get_sales_cogs_by_slot_data(version, week):
    return run_sales_cogs_by_slot_raw(version,week)

@st.cache_data(show_spinner="Loading sales data...", persist=True)
def get_recipe_raw_data(version, week):
    return run_recipes_raw_data(version,week)

@st.cache_data(show_spinner=False)
def get_hellofresh_weeks():
    df = fetch_hellofresh_weeks()
    return df['hellofresh_week'].tolist()

st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)


if not st.user.is_logged_in:
    st.switch_page("home.py")



# --- UI ---


# 사이드바 상단에 Home 버튼 추가
if st.sidebar.button(f"🏠︎"):
    st.switch_page("home.py")


st.sidebar.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)

# Fetch once and reuse across reruns
hellofresh_weeks = get_hellofresh_weeks()


with st.sidebar:
    version_option = st.selectbox(
        "Version", 
        options=["v2", "v3"]
    )
    hellofresh_week_option = st.selectbox(
        "Hello Fresh Week", 
        options=hellofresh_weeks
    )
    entity_option = st.selectbox(
        "Entity", 
        options=["AU","AO","NZ"]
    )
    report_option = st.radio(
            "Select Report Category", 
            options=["By Slot", "By Primary Tag", "By Type"]
        )




df_raw = get_sales_cogs_by_slot_data(version_option, hellofresh_week_option)
df_raw_recipe = get_recipe_raw_data(version_option, hellofresh_week_option)

with st.spinner('⏳ Fetching data...'):
    df_grouped = process_sales_cogs_data(
        df_raw,
        report_option,
        entity_option
    )


if report_option == "By Slot":

    st.header("Summary By Slot")

    if not df_grouped.empty:
        # Get unique slot list early
        unique_slots = sorted(df_grouped['recipe_slot'].dropna().unique().tolist(), key=int)
        unique_slots = [str(s) for s in unique_slots] 

        # Ensure numeric types for aggregation
        df_grouped['total_sales_num'] = pd.to_numeric(df_grouped['total_sales'].replace(',', ''), errors='coerce')
        df_grouped['total_cogs_num'] = pd.to_numeric(df_grouped['total_cogs'].replace(',', ''), errors='coerce')
        df_grouped['box_count_num'] = pd.to_numeric(df_grouped['box_count'].replace(',', ''), errors='coerce')

        # Calculate summary metrics
        total_boxes = df_grouped['box_count_num'].sum()
        total_sales = df_grouped['total_sales_num'].sum()
        total_cogs = df_grouped['total_cogs_num'].sum()
        cogs_ratio = total_cogs / total_sales if total_sales != 0 else 0
        total_aov = total_sales / total_boxes if total_boxes != 0 else 0

        # Display in cards
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric("📦 Total Box Count", f"{int(total_boxes):,}")
        col2.metric("💰 Total Sales", f"${int(total_sales):,}")
        col3.metric("💵 AOV", f"${total_aov:,.2f}")
        col4.metric("🧾 Total COGS", f"${int(total_cogs):,}")
        col5.metric("⚖️ COGS % of GR", f"{cogs_ratio:.1%}")

        df_grouped.drop(columns=['total_sales_num', 'total_cogs_num', 'box_count_num'], inplace=True)


        # Rename columns
        df_grouped.rename(columns={
            'sales_count_kit': 'Kit Counts',
            'box_count': 'Box Counts',
            'recipe_slot': 'Slot', 
            'title' : 'Title', 
            'recipe_family':'Type',
            'total_sales':'Revenue',
            'total_cogs':'Direct Ingr. Cost',
            'cogs_per_sales':'Cost % to GR'
        }, inplace=True)

        st.dataframe(df_grouped.style.format({
            'Kit Counts': '{:,.0f}',
            'Box Counts': '{:,.0f}',
            'Revenue': '{:,.0f}',
            'Direct Ingr. Cost': '{:,.0f}',
            'Cost % to GR': '{:.0%}'
        }),
        hide_index=True,
        height=500)



        # Slot filter selectbox
        st.info("Details by Slot")
        col1, col2, col3 = st.columns([2, 1, 5])  # col2 = 3/9 = 33%

        with col1:
            selected_slot = st.selectbox("🔍 Filter by Recipe Slot", options=["-----Select Slot-----"] + unique_slots)

        # Apply filtering if selected
        if selected_slot != "-----Select Slot-----":
            filtered_raw = df_raw[
                (df_raw['recipe_slot'].astype(str) == selected_slot) &
                (df_raw['country'] == entity_option) &
                (df_raw['hellofresh_week'] == hellofresh_week_option)
            ]


            df_recipe =  process_recipe_data(
                df_raw_recipe,
                report_option,
                entity_option,
                selected_slot
            )


            if not filtered_raw.empty:
                first_row = filtered_raw.iloc[0]
                st.markdown(f"""
                <h4 class="text-primary" style="color:#1f77b4; font-weight:600;">
                    🍽️ {selected_slot} : {first_row['title']} | {first_row['recipe_family']} | {first_row['primary_tag']}
                </h4>
                """, unsafe_allow_html=True)


            # Total sales and adjusted cogs
            filtered_raw['total_sales'] = filtered_raw['core_sales'] + filtered_raw['non_core_sales']
            filtered_raw['total_cogs'] = filtered_raw['cogs'] + filtered_raw['residual_cogs']

            # Adjusted cogs ratio per sales
            filtered_raw['cogs_per_sales'] = (
                filtered_raw['total_cogs'] / filtered_raw['total_sales']
            ).fillna(0)

            # Drop unneeded columns
            filtered_raw.drop(columns=['hellofresh_week','version','country','recipe_slot','title','box_type','product_type','cost','adj_cogs_per_box','adj_cost_per_box','adj_cost_per_kit','residual_cost','cost_per_kit','cogs_per_kit','recipe_family','primary_tag' , 'core_sales', 'cogs', 'non_core_sales', 'residual_cogs'], inplace=True)

            # Sort by dc and recipe_size
            filtered_raw = filtered_raw.sort_values(by=['dc', 'recipe_size'])
            # Desired column order
            first_cols = ['dc', 'recipe_size']
            other_cols = [col for col in filtered_raw.columns if col not in first_cols]
            ordered_cols = first_cols + other_cols

            # Reorder DataFrame
            filtered_raw = filtered_raw[ordered_cols]


            # Rename columns
            filtered_raw.rename(columns={
                'dc': 'DC',
                'recipe_size': 'Recipe Size',
                'sales_count_kit': 'Kit Counts',
                'box_count': 'Box Counts',
                'recipe_slot': 'Slot', 
                'total_sales':'Revenue',
                'total_cogs':'Direct Ingr. Cost',
                'cogs_per_sales':'Cost % to GR',
                'adj_cogs_per_kit':'Cost Per Kit',

            }, inplace=True)

            filtered_raw = blank_repeats(filtered_raw, ['DC', 'Recipe Size'])
            # --- Calculate total row ---
            numeric_cols = filtered_raw.select_dtypes(include='number').columns
            total_row = filtered_raw[numeric_cols].sum().to_frame().T
            total_row.index = ['Total']

            # Fill non-numeric columns
            for col in filtered_raw.columns:
                if col not in numeric_cols:
                    total_row[col] = '—'

            # Reorder columns and append total row
            total_row = total_row[filtered_raw.columns]
            df_with_total = pd.concat([filtered_raw, total_row], axis=0, ignore_index=False)

            # --- Custom row style ---
            def highlight_total_row(row):
                return ['background-color: #003366; font-weight: bold' if row.name == 'Total' else '' for _ in row]

            # --- Format and display in Streamlit ---

            st.dataframe(
                df_with_total
                    .style.format({
                        'Kit Counts': '{:,.0f}',
                        'Box Counts': '{:,.0f}',
                        'Revenue': '{:,.0f}',
                        'Direct Ingr. Cost': '{:,.0f}',
                        'Cost % to GR': '{:.0%}'
                    })
                    .apply(highlight_total_row, axis=1),
                hide_index=True,
                use_container_width=True,
                height=350
            )

            st.info("Recipe Details...")
            group_option = st.radio(
                "Group By:",
                options=["By Recipe", "By DC", "By Size", "By DC & Size", "All"],
                horizontal=True
            )

            df_processed = process_recipe_data_calc(df_recipe.copy(), group_option)

            st.dataframe(
                df_processed.style.format({
                    'weighted_quantity': '{:,.0f}',
                    'weighted_cost': '${:,.0f}',
                    'unit_cost': '${:,.2f}'
                }),
                use_container_width=True,
                hide_index=True,
                height=550
            )



         
    else:
        st.write("No data available for selected options.")

elif report_option == "By Primary Tag" :

    st.header("Summary By Primary Tag")


    if not df_grouped.empty:
        # Get unique slot list early
        # unique_slots = sorted(df_grouped['recipe_slot'].dropna().unique().tolist(), key=int)
        # unique_slots = [str(s) for s in unique_slots] 

        # Ensure numeric types for aggregation
        df_grouped['total_sales_num'] = pd.to_numeric(df_grouped['total_sales'].replace(',', ''), errors='coerce')
        df_grouped['total_cogs_num'] = pd.to_numeric(df_grouped['total_cogs'].replace(',', ''), errors='coerce')
        df_grouped['box_count_num'] = pd.to_numeric(df_grouped['box_count'].replace(',', ''), errors='coerce')

        # Calculate summary metrics
        total_boxes = df_grouped['box_count_num'].sum()
        total_sales = df_grouped['total_sales_num'].sum()
        total_cogs = df_grouped['total_cogs_num'].sum()
        cogs_ratio = total_cogs / total_sales if total_sales != 0 else 0
        total_aov = total_sales / total_boxes if total_boxes != 0 else 0

        # Display in cards
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric("📦 Total Box Count", f"{int(total_boxes):,}")
        col2.metric("💰 Total Sales", f"${int(total_sales):,}")
        col3.metric("💵 AOV", f"${total_aov:,.2f}")
        col4.metric("🧾 Total COGS", f"${int(total_cogs):,}")
        col5.metric("⚖️ COGS % of GR", f"{cogs_ratio:.1%}")

        df_grouped.drop(columns=['total_sales_num', 'total_cogs_num', 'box_count_num'], inplace=True)


        # Rename columns
        df_grouped.rename(columns={
            'sales_count_kit': 'Kit Counts',
            'box_count': 'Box Counts',
            'recipe_slot': 'Slot', 
            'title' : 'Title', 
            'recipe_family':'Type',
            'total_sales':'Revenue',
            'total_cogs':'Direct Ingr. Cost',
            'cogs_per_sales':'Cost % to GR'
        }, inplace=True)
        df_grouped = df_grouped.sort_values(by='Revenue', ascending=False)
        st.dataframe(df_grouped.style.format({
            'Kit Counts': '{:,.0f}',
            'Box Counts': '{:,.0f}',
            'Revenue': '{:,.0f}',
            'Direct Ingr. Cost': '{:,.0f}',
            'Cost % to GR': '{:.0%}'
        }),
        hide_index=True,
        height=600)



        # Slot filter selectbox
        # st.info("Details by Slot")
        # col1, col2, col3 = st.columns([2, 1, 5])  # col2 = 3/9 = 33%

        # with col1:
        #     selected_slot = st.selectbox("🔍 Filter by Recipe Slot", options=["-----Select Slot-----"] + unique_slots)

        # Apply filtering if selected
       
         
    else:
        st.write("No data available for selected options.")


elif report_option == "By Type" :

    st.header("Summary By Type")


    if not df_grouped.empty:
        # Get unique slot list early
        # unique_slots = sorted(df_grouped['recipe_slot'].dropna().unique().tolist(), key=int)
        # unique_slots = [str(s) for s in unique_slots] 

        # Ensure numeric types for aggregation
        df_grouped['total_sales_num'] = pd.to_numeric(df_grouped['total_sales'].replace(',', ''), errors='coerce')
        df_grouped['total_cogs_num'] = pd.to_numeric(df_grouped['total_cogs'].replace(',', ''), errors='coerce')
        df_grouped['box_count_num'] = pd.to_numeric(df_grouped['box_count'].replace(',', ''), errors='coerce')

        # Calculate summary metrics
        total_boxes = df_grouped['box_count_num'].sum()
        total_sales = df_grouped['total_sales_num'].sum()
        total_cogs = df_grouped['total_cogs_num'].sum()
        cogs_ratio = total_cogs / total_sales if total_sales != 0 else 0
        total_aov = total_sales / total_boxes if total_boxes != 0 else 0

        # Display in cards
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.metric("📦 Total Box Count", f"{int(total_boxes):,}")
        col2.metric("💰 Total Sales", f"${int(total_sales):,}")
        col3.metric("💵 AOV", f"${total_aov:,.2f}")
        col4.metric("🧾 Total COGS", f"${int(total_cogs):,}")
        col5.metric("⚖️ COGS % of GR", f"{cogs_ratio:.1%}")

        df_grouped.drop(columns=['total_sales_num', 'total_cogs_num', 'box_count_num'], inplace=True)


        # Rename columns
        df_grouped.rename(columns={
            'sales_count_kit': 'Kit Counts',
            'box_count': 'Box Counts',
            'recipe_slot': 'Slot', 
            'title' : 'Title', 
            'recipe_family':'Type',
            'total_sales':'Revenue',
            'total_cogs':'Direct Ingr. Cost',
            'cogs_per_sales':'Cost % to GR'
        }, inplace=True)
        df_grouped = df_grouped.sort_values(by='Revenue', ascending=False)
        st.dataframe(df_grouped.style.format({
            'Kit Counts': '{:,.0f}',
            'Box Counts': '{:,.0f}',
            'Revenue': '{:,.0f}',
            'Direct Ingr. Cost': '{:,.0f}',
            'Cost % to GR': '{:.0%}'
        }),
        hide_index=True,
        height=250)



        # Slot filter selectbox
        # st.info("Details by Slot")
        # col1, col2, col3 = st.columns([2, 1, 5])  # col2 = 3/9 = 33%

        # with col1:
        #     selected_slot = st.selectbox("🔍 Filter by Recipe Slot", options=["-----Select Slot-----"] + unique_slots)

        # Apply filtering if selected
       
         
    else:
        st.write("No data available for selected options.")












