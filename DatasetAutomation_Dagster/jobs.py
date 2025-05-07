# jobs.py
from dagster import job, op, sensor, RunRequest, resource, build_reconstructable_job
import dagster as dg
# 定义你的资产作业
render_pdf_job = dg.define_asset_job(
    name="render_pdf_job", selection=["render_pdf_pages_with_boxes"]
)



# 使用 build_reconstructable_job 将它转换为可重建作业
# reconstructable_render_pdf_job = build_reconstructable_job("render_pdf_job")
