from flatten_json import unflatten

def peek(df):
  return df.dropna(axis=1, how="all").map(repr)

# return dataframe to original [{k:v, ...},...]
def unflatten(df):
  return [
    unflatten(row.dropna().to_dict(), separator='.')
    for _, row in df.iterrows()
  ]