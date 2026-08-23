from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient

app = FastAPI(
    title="Traffic Statistics API",
    description="REST API for statistics",
    version="1.0"
)

#connection to mongo

client = MongoClient(
    "mongodb://localhost:27017",
    serverSelectionTimeoutMS=5000
)

db = client["traffic"]

raw_collection = db["raw_data"]
stats_collection = db["stats"]
window_collection = db["window_stats"]

def clean_document(document):
    if document is None:
        return None

    document.pop("_id", None)

    return document

@app.get("/")
def root():
    return {
        "message": "UXSIM Traffic Statistics API",
        "endpoints": [
            "/health",
            "/stats",
            "/stats/{link}",
            "/window-stats",
            "/summary",
            "/docs"
        ]
    }

# chekc if mongo is reachable

@app.get("/health")
def health():

    try:
        client.admin.command("ping")

        return {
            "status": "ok",
            "mongodb": "connected"
        }

    except Exception as error:

        raise HTTPException(
            status_code=503,
            detail=f"MongoDB connection failed: {str(error)}"
        )

# processed spark stats

@app.get("/stats")
def get_stats(
    limit: int = Query(
        default=20,
        ge=1,
        le=500
    )
):

    documents = list(
        stats_collection
        .find({}, {"_id": 0})
        .limit(limit)
    )

    return {
        "count": len(documents),
        "data": documents
    }

# solo link 

@app.get("/stats/{link}")
def get_stats_for_link(
    link: str,
    limit: int = Query(
        default=20,
        ge=1,
        le=500
    )
):

    documents = list(
        stats_collection
        .find(
            {"link": link},
            {"_id": 0}
        )
        .limit(limit)
    )

    if not documents:
        raise HTTPException(
            status_code=404,
            detail=f"No statistics found for link '{link}'"
        )

    return {
        "link": link,
        "count": len(documents),
        "data": documents
    }

# 30 sec wind. stats

@app.get("/window-stats")
def get_window_stats(
    link: str | None = None,
    limit: int = Query(
        default=20,
        ge=1,
        le=500
    )
):

    mongo_filter = {}

    if link is not None:
        mongo_filter["link"] = link

    documents = list(
        window_collection
        .find(
            mongo_filter,
            {"_id": 0}
        )
        .limit(limit)
    )

    return {
        "count": len(documents),
        "data": documents
    }

# aver. stats per link

@app.get("/summary")
def get_summary():

    pipeline = [
        {
            "$group": {
                "_id": "$link",
                "average_speed": {
                    "$avg": "$v"
                },
                "min_speed": {
                    "$min": "$v"
                },
                "max_speed": {
                    "$max": "$v"
                },
                "vehicle_records": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "link": "$_id",
                "average_speed": 1,
                "min_speed": 1,
                "max_speed": 1,
                "vehicle_records": 1
            }
        },
        {
            "$sort": {
                "average_speed": -1
            }
        }
    ]

    results = list(
        raw_collection.aggregate(pipeline)
    )

    return {
        "count": len(results),
        "data": results
    }