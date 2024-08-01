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
    df = read_df[cols]
    df['multiMovieYn'] = read_df.groupby(cols[:-2])['multiMovieYn'].transform('first')
    df['repNationCd'] = read_df.groupby(cols[:-2])['repNationCd'].transform('first')
    df = read_df.drop_duplicates()
    print(df.head(5))

    return df

merge()
