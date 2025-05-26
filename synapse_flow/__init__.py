import  synapse_flow.assets
import synapse_flow.documentRecognitionJob
import  synapse_flow.jobs
import synapse_flow.promptJob
from dagster import Definitions
import  synapse_flow.iomanagers
print("__init__.py启动")

defs = Definitions(
    assets=[synapse_flow.assets.render_pdf_pages_with_boxes],
    jobs=[synapse_flow.jobs.process_pdf_job,synapse_flow.promptJob.promptJobPipeLine,synapse_flow.documentRecognitionJob.document_recognition_pipeline],
    resources={
        "sqlite": synapse_flow.iomanagers.sqlite_io_manager,  # SQLite 资源
        "postgres_io_manager": synapse_flow.iomanagers.postgres_io_manager  # PostgreSQL 资源
    }
)
