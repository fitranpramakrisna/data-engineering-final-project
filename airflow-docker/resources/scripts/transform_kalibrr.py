def tranform_web_kalibrr(df):
    import pandas as pd

    df = pd.read_json(df)
    
    df['city'] = df['city'].apply(lambda x: 'Jakarta' if isinstance(x, str) and 'Jakarta' in x else x)
    df['city'] = df['city'].apply(lambda x: 'Tangerang' if isinstance(x, str) and 'Tangerang' in x else x)
    df['city'] = df['city'].str.replace(r'\b(Regency|Kota|Kabupaten)\b', '', regex=True).str.strip()
    
    # standarisasi level_education
    education_mapping = {
    550: 'S1',
    200: 'SMA',
    350: 'D3',
    450: 'D3'
}

    df['education_min_lvl'] = df['education_min_lvl'].map(education_mapping).fillna('Other')
    
    def mapping_working_lvl(row):
        working_lvl = row['level_experience']
        
        if working_lvl == 200:
            return 'Fresh Grad / Junior'
        elif working_lvl == 400:
            return 'Mid-Senior Level Manager'
        elif working_lvl == 400:
            return 'Supervisor / Asisten Manager'
        elif working_lvl == 100:
            return 'Intern'
        else:
            'Other'

    df['level_experience'] = df.apply(mapping_working_lvl, axis=1)

    return df.to_json()