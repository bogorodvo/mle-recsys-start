import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
import requests

from recommendations import Recommendations

logger = logging.getLogger("uvicorn.error")

rec_store = Recommendations()

rec_store.load(
    "personal",
    "final_recommendations_feat.parquet",
    columns=["user_id", "item_id", "rank"],
)
rec_store.load(
    "default",
    "top_recs.parquet",
    columns=["item_id", "rank"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)

    return {"recs": recs}


features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"


# @app.post("/recommendations_online")
# async def recommendations_online(user_id: int, k: int = 100):
#     """
#     Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
#     """

#     headers = {"Content-type": "application/json", "Accept": "text/plain"}

#     # получаем последнее событие пользователя
#     params = {"user_id": user_id, "k": 1}
#     resp = requests.post(events_store_url + "/get", headers=headers, params=params)
#     events = resp.json()
#     events = events["events"]

#     # получаем список похожих объектов
#     if len(events) > 0:
#         item_id = events[0]
#         params = {"item_id": item_id, "k": k}
#         item_similar_items = requests.post(
#             features_store_url +"/similar_items", 
#             headers=headers, 
#             params=params
#         ).json()["item_id_2"]
#         recs = item_similar_items[:k]
#     else:
#         recs = []

#     return {"recs": recs}


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids


@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем список последних событий пользователя, возьмём три последних
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"]

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for item_id in events:
        params = {"item_id": item_id, "k": 3}
        item_similar_items = requests.post(
            features_store_url +"/similar_items", 
            headers=headers, 
            params=params
        ).json()
        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    # для старта это приемлемый подход
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)

    return {"recs": recs} 

