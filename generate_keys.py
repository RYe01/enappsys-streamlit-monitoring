import pickle
from pathlib import Path

import streamlit_authenticator as stauth

names = ["Administrator"]
usernames = ["admin"]
passwords = ["cfUo6rNHmysD0XF9Gvt8"]

hashed_passwords = stauth.Hasher(passwords).generate()

file_path = Path(__file__).parent / "hashed.pw.pkl"
with file_path.open("wb") as file:
    pickle.dump(hashed_passwords, file)