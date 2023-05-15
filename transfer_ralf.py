from skyplane.api.client import SkyplaneClient
from skyplane.api.pipeline import Pipeline


client = SkyplaneClient() 

folders = ["doc_pkl", "doc_xml", "embeddings", "entities", "files", "models", "new_doc_pkl", "new_doc_xml", "old-dump-data", "processed_diffs_title", "processed_paired_diffs", "recentchanges", "simulation_output", "simulation", "edit_diffs"]
for folder in folders:
    pipeline = client.pipeline(max_instances=4, debug=True)
    #pipeline.queue_copy(f"s3://feature-store-datasets/wikipedia/{folder}/", f"gs://feature-store-datasets/wikipedia/{folder}/", recursive=True)
    pipeline.queue_sync(f"s3://feature-store-datasets/wikipedia/{folder}/", f"gs://feature-store-datasets/wikipedia/{folder}/")
    pipeline.start(progress=True)

