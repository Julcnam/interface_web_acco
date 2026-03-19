import duckdb
from config import DOWNLOAD_PATH
import os
from pathlib import Path
import pandas as pd
import lxml.etree as ET
from alive_progress import alive_bar

# Connexion à la base de données DuckDB (création du fichier s'il n'existe pas)
def connect():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Dossier Bdd
    bdd_dir = os.path.join(base_dir, "bdd")
    os.makedirs(bdd_dir, exist_ok=True)  # crée le dossier si absent

    # Chemin complet vers la base
    db_path = os.path.join(bdd_dir, "metadonnee.duckdb")

    # Connexion (créera la DB si elle n'existe pas)
    conn = duckdb.connect(db_path)

    return conn

# Création de la table metadonnee dans DuckDB pour indexer les documents
def create_table_metadata():
    conn=connect()
    print("Création de la table 'metadonnee' dans DuckDB...")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadonnee (
        reference VARCHAR,    
        titre VARCHAR,
        ape VARCHAR,
        idcc INTEGER,  
        origine VARCHAR,
        nature VARCHAR,
        raison_sociale VARCHAR,
        siret VARCHAR,
        secteur VARCHAR,
        signataires VARCHAR,     
        thematique VARCHAR,    
        date_signature DATE,
        date_publication DATE,
        date_application_debut DATE,
        date_application_fin DATE
    )""")
    print("Table 'metadonnee' créée dans DuckDB si elle n'existait pas.")

# Extraction des métadonnées depuis un fichier XML
def get_text(root, tag):
    el = root.find(f".//{tag}")
    return el.text.strip() if el is not None and el.text else None

# Concaténation des labels multiples
def concat_labels(root, xpath, sep=" | "):
    values = [
        el.text.strip()
        for el in root.findall(xpath)
        if el.text and el.text.strip()
    ]
    return sep.join(values) if values else None

# Lecture d'un fichier XML et conversion en DataFrame pandas
def read_xml_to_df(xml_path: Path) -> pd.DataFrame:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    row = {
        "reference": get_text(root, "NUMERO"),
        "titre": get_text(root, "TITRE_TXT"),
        "ape": get_text(root, "CODE_APE"),
        "idcc": get_text(root, "CODE_IDCC"),
        "origine": get_text(root, "ORIGINE"),
        "nature": get_text(root, "NATURE"),
        "raison_sociale": get_text(root, "RAISON_SOCIALE"),
        "siret": get_text(root, "SIRET"),
        "secteur": get_text(root, "SECTEUR"),
        "signataires": concat_labels(root, ".//SYNDICAT/LIBELLE"),
        "thematique": concat_labels(root, ".//THEME/LIBELLE"),
        "date_signature": get_text(root, "DATE_TEXTE"),
        "date_publication": get_text(root, "DATE_DIFFUSION"),
        "date_application_debut": get_text(root, "DATE_EFFET"),
        "date_application_fin": get_text(root, "DATE_FIN")
    }
    return pd.DataFrame([row])


 

# Insertion des métadonnées dans la table 'metadonnee'
def insert_metadata():
    conn = connect()
    print("Insertion des métadonnées dans la table 'metadonnee'...")
    base_path = Path(DOWNLOAD_PATH)
    xml_files = [p for p in base_path.rglob("*.xml") if p.is_file()]
    if not xml_files:
        print("Aucun fichier XML trouvé.")
        return
    with alive_bar(len(xml_files), title="Insertion...") as bar:
        for path in xml_files:
            try:
                df = read_xml_to_df(path)
                conn.register("df", df)
                conn.execute(""" INSERT INTO metadonnee SELECT * FROM df d WHERE NOT EXISTS ( SELECT 1 FROM metadonnee m WHERE m.reference = d.reference )""")
                conn.unregister("df")
            except Exception as e:
                print(f"Erreur avec {path.name} : {e}")
            finally:
                bar()
    print("Insertion des métadonnées terminée.")

