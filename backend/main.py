from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Khởi tạo DB (tạo bảng + seed users mặc định) khi server start."""
    from db.database import init_db
    init_db()
    yield


app = FastAPI(title="Lakehouse API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication
from api.routes import auth
app.include_router(auth.router, prefix="/api", tags=["Authentication"])

# Upload + Upload History
from api.routes import upload
app.include_router(upload.router, prefix="/api", tags=["Upload"])

# Catalog (Nessie history / references)
from api.routes import catalog
app.include_router(catalog.router, prefix="/api", tags=["Catalog"])

# Pipeline Data Explorer — prefix="/api/pipeline" để khớp frontend
from api.routes import pipeline_preview
app.include_router(pipeline_preview.router, prefix="/api/pipeline", tags=["Pipeline"])

# User Management (CRUD — admin only, dùng PostgreSQL ORM)
from api.routes import users
app.include_router(users.router, prefix="/api", tags=["Users"])

# Superset Guest Token
from api.routes import superset
app.include_router(superset.router, prefix="/api/superset", tags=["Superset"])

@app.get("/")
async def root():
    return {"message": "Welcome to Lakehouse API!"}