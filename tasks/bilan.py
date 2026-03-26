from pathlib import Path
from config import DOWNLOAD_PATH
from db import connect
from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor


           
def get_nb_files():
    files = 0 
    for path in Path(DOWNLOAD_PATH).rglob("*.txt") :
        files +=1
    return files


def get_nb_empty_files():
    files = 0
    for path in  Path(DOWNLOAD_PATH).rglob("*.txt"):
        if path.stat().st_size < 200:
            files +=1
    return files

def get_nb_files_doubles():
    files = 0
    doubles = set()
    for path in Path(DOWNLOAD_PATH).rglob("*.txt") :
        if path.name in doubles:
            files +=1
        else:
            doubles.add(path.name)
    return files


def get_nb_meta():
    meta = 0
    for path in Path(DOWNLOAD_PATH).rglob("*.xml"):
        meta += 1
    return meta

def get_nb_meta_doubles():
    meta = 0
    doubles = set()
    for path in Path(DOWNLOAD_PATH).rglob("*.xml"):
        if path.name in doubles :
            meta += 1
        else :
            doubles.add(path.name)
    return meta




def get_txt_with_no_meta():
    
    conn = connect()
    df_ref_duckdb = conn.execute(f"SELECT reference FROM metadonnee order by date_signature ").df()

    txt_files = list(Path(DOWNLOAD_PATH).rglob("*.txt"))
    df_txt_files = pd.DataFrame([{"reference": str(path.name).split("-")[0]} for path in txt_files])

    txt_with_no_meta = set(df_txt_files["reference"]) - set(df_ref_duckdb["reference"])

    return txt_with_no_meta

  



def get_meta_with_no_txt():

    conn = connect()
    df_ref_duckdb = conn.execute(f"SELECT reference FROM metadonnee order by date_signature ").df()

    txt_files = list(Path(DOWNLOAD_PATH).rglob("*.txt"))
    df_txt_files = pd.DataFrame([{"reference": str(path.name).split("-")[0]} for path in txt_files])

    meta_with_no_txt = set(df_ref_duckdb["reference"]) - set(df_txt_files["reference"])

    return meta_with_no_txt





def bilan():
    with ProcessPoolExecutor() as executor:
        executor.submit(print, f"Nombre de fichiers .txt : {get_nb_files()}")
        executor.submit(print, f"Nombre de fichiers .txt vides : {get_nb_empty_files()}")
        executor.submit(print, f"Nombre de fichiers .txt en double : {get_nb_files_doubles()}")
        executor.submit(print, f"Nombre de fichiers .xml : {get_nb_meta()}")
        executor.submit(print, f"Nombre de fichiers .xml en double : {get_nb_meta_doubles()}")
        executor.submit(print, f"Fichiers .txt sans métadonnées : {len(get_txt_with_no_meta())}")
        executor.submit(print, f"Métadonnées sans fichiers .txt : {len(get_meta_with_no_txt())}")
        executor.shutdown(wait=True)

