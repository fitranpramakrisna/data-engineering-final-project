def tranform_web_kalibrr(df):
    import pandas as pd
    
    print(df)
    df = pd.read_json(df)
    print(df)
    # df['city'] = df['city'].astype(str).apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)

    # standarisasi nama kota
    # df['city'] = df['city'].apply(lambda x: 'Jakarta' if 'Jakarta' in x else x)
    df['city'] = df['city'].apply(lambda x: 'Jakarta' if isinstance(x, str) and 'Jakarta' in x else x)
    df['city'] = df['city'].str.replace(r'\b(Regency|Kota)\b', '', regex=True).str.strip()
    
    # standarisasi level_education
    education_mapping = {
    550: 'S1',
    200: 'SMA',
    350: 'D3',
    450: 'D3'
}

    df['education_min_lvl'] = df['education_min_lvl'].map(education_mapping).fillna('Other')
    
    # standarisasi level_experience
    working_lvl_experience_mapping = {
        200: 'Freshgrad / Junior',
        400: 'Mid-Senior Level Manager',
        300: 'Supervisor / Asisten Manager',
        100: 'Intern'

    }
    df['level_experience'] = df['level_experience'].apply(lambda x: x[0] if isinstance(x, list) and x else 'Other')
    df['level_experience'] = df['level_experience'].map(working_lvl_experience_mapping).fillna('Other')


    # df['level_experience'] = df['level_experience'].map(working_lvl_experience_mapping).fillna('Other')
    # print(df['min_salary'].unique())
    # print(df['max_salary'].unique())
    
    # return df.to_excel('transformed_kalibrr.xlsx', index=False)
    # print(df)
    print(f"total baris : {df.shape[0]}")
    return df.to_json()