from config import get_s3
from config import DOWNLOAD_PATH
from pathlib import Path
from alive_progress import alive_bar
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_s3_bucket_name():
    return "juliencnam"


# Création du bucket onyxia si il n'existe pas
def s3_create_bucket():
    s3_client = get_s3()
    s3_bucket = get_s3_bucket_name()

    try:
        s3_client.head_bucket(Bucket=s3_bucket)
        print(f"Bucket '{s3_bucket}' existe déjà")
        return s3_bucket

    except ClientError as err:
        error_code = int(err.response["Error"]["Code"])

        if error_code == 404:
            print(f"Création du bucket '{s3_bucket}'")
            s3_client.create_bucket(Bucket=s3_bucket)
            return s3_bucket
        else:
            raise
        


# Téléversement des fichiers .txt le bucket MinIO onyxia
def s3_upload_files():
    s3_client = get_s3()
    s3_bucket = get_s3_bucket_name()

    print("Téléversement des fichiers vers MinIO Onyxia...")
    base_path = Path(DOWNLOAD_PATH)
    txt_files = list(base_path.rglob("*.txt"))

    if not txt_files:
        print("Aucun fichier à téléverser.")
        return

    # Récupération des objets existants
    existing_objects = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket):
        for obj in page.get("Contents", []):
            existing_objects.add(obj["Key"])

    with alive_bar(len(txt_files), title="Téléversement...") as bar:
       with ThreadPoolExecutor() as executor:  # ajuster celon le réseau/cpu
            futures = {executor.submit(upload_file_if_needed, s3_client, s3_bucket, base_path, path, existing_objects): path for path in txt_files}
            for future in as_completed(futures):
                # message = future.result()  # on peut récupérer le message de chaque upload si besoin
                bar()
            
    print("Téléversement terminé.")


def upload_file_if_needed(s3_client, s3_bucket, base_path, path, existing_objects):
    object_name = "Texts/" + path.relative_to(base_path).name
    if object_name in existing_objects:
        return f"{object_name} existe déjà."
    s3_client.upload_file(str(path), s3_bucket, object_name)
    return f"{object_name} téléversé."



# Suppression des fichiers .txt du Bucket MinIO onyxia
def s3_delete_files():
    s3_client = get_s3()
    s3_bucket = get_s3_bucket_name()
    
    print("Suppression des fichiers en cours...")
    paginator = s3_client.get_paginator("list_objects_v2")

    all_objects = []
    
    for page in  paginator.paginate(Bucket=s3_bucket):
        if "Contents" in page:
            all_objects.extend({"Key": obj["Key"]} for obj in page["Contents"])

    if not all_objects:
            print("Bucket vide")
            return


    with alive_bar(len(all_objects), title="Suppression...") as bar:
        for i in range(0,len(all_objects),1000):
            batch = all_objects[i:i+1000]
            s3_client.delete_objects(Bucket=s3_bucket,Delete={"Objects": batch,"Quiet": True})
            bar(len(batch))

    print("Suppression terminée")



    





