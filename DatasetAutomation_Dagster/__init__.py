import  DatasetAutomation_Dagster.assets
import  DatasetAutomation_Dagster.jobs
from dagster import Definitions
print("__init__.py启动")

defs = Definitions(
    assets=[DatasetAutomation_Dagster.assets.render_pdf_pages_with_boxes],
    jobs=[DatasetAutomation_Dagster.jobs.render_pdf_job]
)
