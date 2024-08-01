import pandas as pd

def merge(load_dt="20240724"):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd',      #영화의 대표코드를 출력합니다.
            'movieNm',      #영화명(국문)을 출력합니다.
            'openDt',       #영화의 개봉일을 출력합니다.
            'audiCnt',      #해당일의 관객수를 출력합니다.
            'load_dt',      # 입수일자
            'multiMovieYn', #다양성영화 유무
            'repNationCd',  #한국외국영화 유무
    ]
    dfs = []
    for col in cols[-3:]:
        read_df[col] = read_df[col].astype('object')
    for i in range(0, len(read_df), 100):
        #100개 단위로 작업 진행
        df = read_df.iloc[i:i+100][cols].copy()
        df['multiMovieYn'] = df.groupby(cols[:-2])['multiMovieYn'].transform('first')
        df['repNationCd'] = df.groupby(cols[:-2])['repNationCd'].transform('first')
        df = df.drop_duplicates()
        dfs.append(df)

    df_merged = pd.concat(dfs)

    return df_merged

merge()
