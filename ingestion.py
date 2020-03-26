import os
import sys

import detectlanguage
import pandas as pd
import pyorc


DETECT_LANGUAGE_API_KEY = "Get your API Key at https://detectlanguage.com/private"
detectlanguage.configuration.api_key = DETECT_LANGUAGE_API_KEY
ORC_FILE = "./new_data.orc"

source_file_path = sys.argv[1]
if not os.path.isfile(source_file_path):
    print(f"Wrong file path {source_file_path}, exit script.")
    exit()

max_requests = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
offset = int(sys.argv[3]) if len(sys.argv) > 3 else 0

dl_user_status = detectlanguage.user_status()
available_requests = dl_user_status['daily_requests_limit'] - dl_user_status['requests']

df = pd.read_csv(source_file_path, header=0)
nb_lines = min(available_requests, len(df) - offset, max_requests)

df = df[offset:offset + nb_lines].copy().reset_index(drop=True)

response = detectlanguage.detect(df["Text"].values.tolist())
first_languages = list(map(lambda x: x[0] if x is not None else {'isReliable': False, 'confidence': 0, 'language': ''}, response))

new_df = pd.concat([df, pd.DataFrame(first_languages)], axis=1)

with open(ORC_FILE, "wb") as data:
    with pyorc.Writer(
        data,
        "struct<text:string,isSpam:boolean,language:string,isReliable:boolean,confidence:float>",
        compression=pyorc.CompressionKind.ZLIB
    ) as writer:
        for index, row in new_df.iterrows():
            writer.write((row['Text'], row['isSpam'], row['language'], row['isReliable'], row['confidence']))

new_df.to_csv(index=True)


## For the future, to read the dataset
# with open(ORC_FILE, 'rb') as orc_file:
#     reader = pyorc.Reader(orc_file)
    
#     # Read embedded schema
#     print(str(reader.schema))
    
#     # Read all the file at once:
#     rows = reader.read()
#     print(rows)