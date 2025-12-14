from IPython.display import display, HTML
import pandas as pd
import pandas_flavor as pf

if not hasattr(pd.DataFrame, "peek"):
    @pf.register_dataframe_method
    def peek(df,  height=500, width='100%'):
        html = f"""
            <style>
                .peek-table thead th {{
                    position: sticky;
                    top: 0;
                    background: grey;
                    z-index: 2;
                }}
            </style>

        <div style="overflow:auto; max-height:{height}px; max-width:{width}; border:1px solid #ccc;">
            {df.to_html(classes='peek-table')}
        </div>
        """
        display(HTML(html))

def to_df(dict):
    return pd.DataFrame(dict).T

def get_from_df(df, columns, vals):
    return df[(df[columns] == vals).all(axis=1)]