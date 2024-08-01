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

        #Groub By로 묶어서 왼쪽열부터 끝에서 두번째까지
        #multiMovieYn, repNationCd를 제외한 값들을 삽입. 그 후 NaN값을 제외한 첫번째 값을 추가로 삽입
        df['multiMovieYn'] = df.groupby(cols[:-2])['multiMovieYn'].transform('first')
        df['repNationCd'] = df.groupby(cols[:-2])['repNationCd'].transform('first')
        # 이 과정이 끝난 다음 해당 행에 중복되는 값들을 삭제
        df = df.drop_duplicates()
        # 처리가 끝난 행을 위에 선언한 임시 dfs 리스트에 추가
        dfs.append(df)

    #모든 처리가 종료된 후 df_merged에 임시로 완성된 리스트값을 대입
    df_merged = pd.concat(dfs)

    #df_merged return
    return df_merged

