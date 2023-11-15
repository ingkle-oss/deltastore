import uvicorn
from fastapi import APIRouter, FastAPI

app = FastAPI()

config = APIRouter(prefix="/config")
authz = APIRouter(prefix="/authz")


@config.get("/filesystems/{filesystem}")
async def get_filesystem(filesystem):
    return {
        "name": "minio",
        "configs": [
            {"key": "fs.s3a.endpoint", "value": "http://localhost:9000"},
            {"key": "fs.s3a.access.key", "value": "minioadmin"},
            {"key": "fs.s3a.secret.key", "value": "minioadmin"},
            {"key": "fs.s3a.path.style.access", "value": "true"},
        ],
    }


@config.get("/shares/{share}/schemas/{schema}/tables/{table}")
async def get_table(share, schema, table):
    return {
        "name": "testset100",
        "schema": "testsets",
        "share": "deltastore",
        "filesystem": "minio",
        "location": "s3a://testset100/",
    }


@authz.post("/list-files")
async def check_list_files():
    return {"success": True, "reason": "", "filters": ['date>="2023-02-01"']}


app.include_router(config)
app.include_router(authz)

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
    )
