# ocr_utils.py

import os
import json
from alibabacloud_ocr_api20210707.client import Client as ocr_api20210707Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ocr_api20210707 import models as ocr_api_20210707_models
from alibabacloud_tea_util import models as util_models
from synapse_flow.db import get_pg_conn

def load_ocr_config_from_db(key_name: str) -> dict:
    """
    根据 key_name 从 key_info 表读取 key_json_info 并转成 dict 返回
    """
    conn = get_pg_conn()
    print("执行load_ocr_config_from_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT key_json_info FROM key_info WHERE key_name = %s LIMIT 1",
                (key_name,)
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"未找到key_name={key_name}的配置")
            key_json_info = row[0]
            print("key_json_info", key_json_info)
            print("key_json_info type:", type(key_json_info))
            
            if isinstance(key_json_info, dict):
                config_dict = key_json_info
            else:
                config_dict = json.loads(key_json_info)
            print("config_dict", config_dict)
            return config_dict
    finally:
        conn.close()


def create_ocr_client(config_dict: dict = None) -> ocr_api20210707Client:
    """
    传入config_dict字典，包含access_key_id、access_key_secret、endpoint
    如果不传，使用默认硬编码配置
    """
    if config_dict is None:
        config_dict = {
            
        }
    config = open_api_models.Config(
        access_key_id=config_dict.get("access_key_id"),
        access_key_secret=config_dict.get("access_key_secret"),
    )
    config.endpoint = config_dict.get("endpoint", "ocr-api.cn-hangzhou.aliyuncs.com")
    return ocr_api20210707Client(config)

def ocr_image_to_json(image_path: str, config_key: str = "aliyun") -> dict:
    """
    读取指定key配置，创建OCR客户端，识别图片返回json数据
    """
    config_dict = load_ocr_config_from_db(config_key)
    client = create_ocr_client(config_dict)
    with open(image_path, 'rb') as f:
        img_bytes = f.read()

    request = ocr_api_20210707_models.RecognizeMixedInvoicesRequest(body=img_bytes)
    runtime = util_models.RuntimeOptions()

    try:
        response = client.recognize_mixed_invoices_with_options(request, runtime)
        print("response11111",response)
        data = json.loads(response.body.data)
        return data
    except Exception as error:
        raise RuntimeError(f"OCR识别失败: {error.message}")
