##########################################
#  TODO                                  #
#  @author: Mahesh Madhusoodanan Pillai  #
#  @email: mahesh.pillai@databricks.com  #
##########################################

import streamlit as st

LOGO = "logos/Data Migration Validator.png"
LOGO_WITH_LABEL = "logos/Data Migration Validator with Label.png"
LOGO_WITH_LABEL_LONG = "logos/Data Migration Validator with Label Long.png"
st.logo(
    LOGO_WITH_LABEL_LONG,
    size="large",
    icon_image=LOGO

)
validation_runner = st.Page("validation_runner.py", title="Runner", icon=":material/directions_run:")
summary_page = st.Page("summary_v3.py", title="Summary", icon=":material/view_module:")
validation_page_v2 = st.Page("validation_v3.py", title="Reports", icon=":material/dashboard:")
validation_conf = st.Page("validation_conf.py", title="Configuration", icon=":material/format_align_justify:")
about = st.Page("about.py", title="About", icon=":material/info:") 


pg = st.navigation({"Validation":[validation_runner, summary_page, validation_page_v2, validation_conf, about]})
st.set_page_config(page_title="Data Migration Validator", page_icon=":material/edit:",layout = "wide")

def add_username():
    user_name = st.context.headers["X-Forwarded-Preferred-Username"]
    st.markdown(
        f"""
        <style>
            [data-testid="stSidebarNav"]::before {{
                content: "{user_name}";
                margin-left: 20px;
                margin-bottom: 4px;
                font-size: 15px;
                position: fixed;
                bottom: 4px;
                color: grey;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
add_username()  
pg.run()
