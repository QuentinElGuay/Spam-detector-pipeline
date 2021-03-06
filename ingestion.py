from datetime import datetime
import os
import sys

import detectlanguage
import pandas as pd
import pyorc


DETECT_LANGUAGE_API_KEY = "c6e79e234e81e160db81454d80ae611d"
detectlanguage.configuration.api_key = DETECT_LANGUAGE_API_KEY
ORC_FILE = "out/spambase_{}.orc"

source_file_path = "/home/quentin/Dev/Spam-detector-pipeline/validation.csv"  # sys.argv[1]
if not os.path.isfile(source_file_path):
    print(f"Wrong file path {source_file_path}, exit script.")
    exit()

max_requests = 1000  # int(sys.argv[2]) if len(sys.argv) > 2 else 1000
offset = 0  # int(sys.argv[3]) if len(sys.argv) > 3 else 0

dl_user_status = detectlanguage.user_status()
available_requests = dl_user_status['daily_requests_limit'] - dl_user_status['requests']
if available_requests == 0:
    print("Quota of requests at DetectLanguage exhausted for today.")
    exit()

df = pd.read_csv(source_file_path, header=0)
nb_lines = min(available_requests, len(df) - offset, max_requests)

df = df[offset:offset + nb_lines].copy().reset_index(drop=True)

response = detectlanguage.detect(df["text"].values.tolist())
first_languages = list(map(lambda x: x[0] if x else {'isReliable': False, 'confidence': 0, 'language': ''}, response))

new_df = pd.concat([df, pd.DataFrame(first_languages)], axis=1)

orc_file = ORC_FILE.format(datetime.now().strftime("%y%m%d"))
with open(orc_file, "wb") as data:
    with pyorc.Writer(
        data,
        "struct<text:string,isSpam:boolean,language:string,isReliable:boolean,confidence:float>",
        compression=pyorc.CompressionKind.ZLIB
    ) as writer:
        for index, row in new_df.iterrows():
            writer.write((row['text'], row['isSpam'], row['language'], row['isReliable'], row['confidence']))

new_df.to_csv(index=True)
print(f"Saved {len(new_df)} messages in {orc_file}.")



## For the future, to read the dataset
# with open(ORC_FILE, 'rb') as orc_file:
#     reader = pyorc.Reader(orc_file)
    
#     # Read embedded schema
#     print(str(reader.schema))
    
#     # Read all the file at once:
#     rows = reader.read()
#     print(rows)