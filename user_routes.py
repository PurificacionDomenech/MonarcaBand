from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from typing import Optional

from auth import (
    authenticate_user, create_user, create_access_token,
    get_current_user, require_user, PLAN_LIMITS, count_vip_users, get_conn
)

router = APIRouter(prefix="/api/auth", tags=["auth"])

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str
    plan: Optional[str] = "free"
    invite_code: Optional[str] = None

class UserOut(BaseModel):
    id: int
    email: str
    name: Optional[str]
    plan: str
    max_watchlist: int
    access_tradingview: bool
    access_indicators: bool

VIP_INVITE_CODE = "TRADINGBAND-VIP"
VIP_MAX_SLOTS = 10

@router.post("/register")
def register(req: RegisterRequest):
    plan = req.plan or "free"

    if plan == "vip":
        if req.invite_code != VIP_INVITE_CODE:
            raise HTTPException(status_code=400, detail="Código de invitación VIP incorrecto")
        if count_vip_users() >= VIP_MAX_SLOTS:
            raise HTTPException(status_code=400, detail="No hay lugares VIP disponibles")

    if plan not in ("free", "vip", "pro"):
        plan = "free"

    try:
        user = create_user(email=req.email, password=req.password, name=req.name, plan=plan)
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail="El correo ya está registrado")
        raise HTTPException(status_code=500, detail="Error al crear usuario")

    token = create_access_token({"sub": str(user["id"])})
    return {"access_token": token, "token_type": "bearer", "user": dict(user)}

@router.post("/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form.username, form.password)
    if not user:
        raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Cuenta inactiva")
    token = create_access_token({"sub": str(user["id"])})
    return {"access_token": token, "token_type": "bearer", "user": {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "plan": user["plan"], "max_watchlist": user["max_watchlist"],
        "access_tradingview": user["access_tradingview"],
        "access_indicators": user["access_indicators"],
    }}

@router.get("/me")
async def me(user=Depends(require_user)):
    return {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "plan": user["plan"], "max_watchlist": user["max_watchlist"],
        "access_tradingview": user["access_tradingview"],
        "access_indicators": user["access_indicators"],
    }

users_router = APIRouter(prefix="/api/users", tags=["users"])

@users_router.get("")
async def list_users(user=Depends(require_user)):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, name, plan, is_active, max_watchlist,
                       access_tradingview, access_indicators, created_at
                FROM users ORDER BY created_at DESC
            """)
            return cur.fetchall()

@users_router.patch("/{user_id}/plan")
async def change_plan(user_id: int, body: dict, user=Depends(require_user)):
    new_plan = body.get("plan")
    if new_plan not in ("free", "vip", "pro"):
        raise HTTPException(status_code=400, detail="Plan no válido")

    if new_plan == "vip" and count_vip_users() >= VIP_MAX_SLOTS:
        raise HTTPException(status_code=400, detail="Cupo VIP lleno (máx 10)")

    limits = PLAN_LIMITS[new_plan]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET plan=%s, max_watchlist=%s,
                    access_tradingview=%s, access_indicators=%s
                WHERE id=%s
                RETURNING id, email, name, plan
            """, (new_plan, limits["max_watchlist"], limits["tradingview"], limits["indicators"], user_id))
            updated = cur.fetchone()
    if not updated:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return updated

@users_router.patch("/{user_id}/active")
async def toggle_active(user_id: int, body: dict, user=Depends(require_user)):
    active = bool(body.get("is_active", True))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET is_active=%s WHERE id=%s RETURNING id, email, is_active",
                        (active, user_id))
            return cur.fetchone()
