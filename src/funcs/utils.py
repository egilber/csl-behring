def inOutkeys_to_lists(df):
    first_ids = []
    second_ids = []
    df['inOutkey'] = df['inOutkey'].apply(lambda x: x.replace('[', ''))
    df['inOutkey'] = df['inOutkey'].apply(lambda x: x.replace(']', ''))

    inOutkey_list = list(df['inOutkey'])
    for item in inOutkey_list:
        x = item.split(',')
        first_ids.append(str(x[0]).strip())
        second_ids.append(str(x[1]).strip())

    return first_ids, second_ids


def convert_object_to_category(df):
    df_obj = df.select_dtypes(['object'])

    for col in list(df_obj.columns):
        df[col] = df[col].apply(lambda x: str(x).strip())
        df[col] = df[col].astype('category')
    return df
