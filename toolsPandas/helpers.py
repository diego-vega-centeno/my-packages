from flatten_json import unflatten
from IPython.display import display, HTML
import pandas as pd

def peek(df):
    return df.dropna(axis=1, how="all").map(repr)

# return dataframe to original [{k:v, ...},...]
def unflatten(df):
    return [
        unflatten(row.dropna().to_dict(), separator='.')
        for _, row in df.iterrows()
    ]

def df_peek(df,  height=500, width='100%'):
    html = f"""
    <div style="overflow:auto; max-height:{height}px; max-width:{width}; border:1px solid #ccc; padding:4px;">
        {df.to_html()}
    </div>
    """
    display(HTML(html))

def to_df(dict):
    return pd.DataFrame(dict)