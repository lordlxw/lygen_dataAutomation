import  DatasetAutomation_Dagster.assets
import  DatasetAutomation_Dagster.jobs
from dagster import Definitions
import  DatasetAutomation_Dagster.iomanagers
print("__init__.py启动")

defs = Definitions(
    assets=[DatasetAutomation_Dagster.assets.render_pdf_pages_with_boxes],
    jobs=[DatasetAutomation_Dagster.jobs.process_pdf_job],
    resources={
        "sqlite": DatasetAutomation_Dagster.iomanagers.sqlite_io_manager,  # SQLite 资源
        "postgres_io_manager": DatasetAutomation_Dagster.iomanagers.postgres_io_manager  # PostgreSQL 资源
    }
)
