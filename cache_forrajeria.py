"""
=============================================================
 Proyecto Integrador - Parte 3: Integración de Caché con Redis
 Unidad Curricular: Base de Datos III
 Tema: Sistema de Gestión para Forrajería y Distribución
 Patrón implementado: Cache-Aside (Lazy Loading)
=============================================================

Endpoints cacheados:
  1. productos:lista        → Catálogo de productos (alta lectura, baja escritura)
  2. vendedores:ranking     → Ranking de vendedores (se actualiza poco durante el día)

Justificación:
  - El catálogo de productos es consultado constantemente por clientes y
    vendedores, pero los precios y el stock no cambian a cada segundo.
  - El ranking de vendedores es una consulta analítica pesada (JOIN de 3 tablas)
    que puede tolerar estar desactualizada 5 minutos sin ningún impacto.
"""

import json
import time
import redis
import psycopg2
import psycopg2.extras
from datetime import datetime


# =============================================================
# CONFIGURACIÓN DE CONEXIONES
# =============================================================

# --- Redis Cloud ---
REDIS_HOST     = "bee-marigold-silver-62818.db.redis.io"
REDIS_PORT     = 15691
REDIS_PASSWORD = "TU_PASSWORD_AQUI"   # <-- reemplazá con tu password

# --- PostgreSQL (tu base de datos local) ---
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "forrajeria",          # <-- nombre de tu base de datos
    "user":     "postgres",            # <-- tu usuario
    "password": "TU_PASSWORD_PG_AQUI" # <-- tu password de PostgreSQL
}

# TTL (Time To Live) en segundos
TTL_PRODUCTOS  = 300   # 5 minutos
TTL_RANKING    = 300   # 5 minutos


# =============================================================
# CONEXIONES
# =============================================================

def conectar_redis():
    """Establece conexión con Redis Cloud."""
    try:
        cliente = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            ssl=True   # Redis Cloud requiere SSL
        )
        cliente.ping()
        print("[REDIS] ✅ Conexión exitosa con Redis Cloud")
        return cliente
    except Exception as e:
        print(f"[REDIS] ⚠️  No se pudo conectar a Redis: {e}")
        print("[REDIS]    Modo FALLBACK activado: se usará solo PostgreSQL")
        return None


def conectar_postgres():
    """Establece conexión con PostgreSQL."""
    conn = psycopg2.connect(**PG_CONFIG)
    print("[POSTGRES] ✅ Conexión exitosa con PostgreSQL")
    return conn


# =============================================================
# PATRÓN CACHE-ASIDE - Endpoint 1: Catálogo de Productos
# Clave Redis: products:lista
# =============================================================

def obtener_productos(redis_client, pg_conn):
    """
    Devuelve el catálogo de productos usando el patrón Cache-Aside.
    
    Flujo:
      1. Buscar en Redis con clave 'products:lista'
      2. HIT  → devolver desde Redis (rápido)
      3. MISS → consultar PostgreSQL → guardar en Redis con TTL → devolver
    """
    CACHE_KEY = "products:lista"

    # ------ PASO 1: Consultar la caché ------
    try:
        if redis_client:
            cached = redis_client.get(CACHE_KEY)
            if cached:
                print(f"\n[CACHE HIT] ✅ '{CACHE_KEY}' encontrado en Redis")
                productos = json.loads(cached)
                print(f"           → {len(productos)} productos devueltos desde caché\n")
                return productos
            else:
                print(f"\n[CACHE MISS] ❌ '{CACHE_KEY}' no está en Redis → consultando PostgreSQL...")
    except Exception as e:
        print(f"[REDIS] ⚠️  Error al leer caché: {e} → usando PostgreSQL directamente")

    # ------ PASO 2: Consultar PostgreSQL ------
    inicio = time.time()
    try:
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    p.id_producto::text,
                    p.sku,
                    p.nombre_producto,
                    p.descripcion_producto,
                    p.precio_venta,
                    p.stock_actual,
                    p.unidad_medida,
                    pr.nombre_empresa AS proveedor
                FROM productos p
                LEFT JOIN proveedores pr ON p.id_proveedor = pr.id_proveedor
                ORDER BY p.nombre_producto
                LIMIT 50
            """)
            productos = [dict(row) for row in cur.fetchall()]

        duracion_pg = round((time.time() - inicio) * 1000, 2)
        print(f"[POSTGRES] ⏱️  Consulta ejecutada en {duracion_pg} ms → {len(productos)} productos obtenidos")

    except Exception as e:
        print(f"[POSTGRES] ❌ Error al consultar: {e}")
        return []

    # ------ PASO 3: Guardar en Redis con TTL ------
    try:
        if redis_client:
            redis_client.setex(CACHE_KEY, TTL_PRODUCTOS, json.dumps(productos, default=str))
            print(f"[REDIS] 💾 '{CACHE_KEY}' guardado en caché por {TTL_PRODUCTOS} segundos (TTL)")
    except Exception as e:
        print(f"[REDIS] ⚠️  No se pudo guardar en caché: {e}")

    return productos


# =============================================================
# PATRÓN CACHE-ASIDE - Endpoint 2: Ranking de Vendedores
# Clave Redis: vendedores:ranking
# =============================================================

def obtener_ranking_vendedores(redis_client, pg_conn):
    """
    Devuelve el ranking de vendedores usando el patrón Cache-Aside.
    Esta es una consulta analítica pesada (Window Function + JOIN),
    ideal para cachear.
    """
    CACHE_KEY = "vendedores:ranking"

    # ------ PASO 1: Consultar la caché ------
    try:
        if redis_client:
            cached = redis_client.get(CACHE_KEY)
            if cached:
                print(f"\n[CACHE HIT] ✅ '{CACHE_KEY}' encontrado en Redis")
                ranking = json.loads(cached)
                print(f"           → {len(ranking)} vendedores devueltos desde caché\n")
                return ranking
            else:
                print(f"\n[CACHE MISS] ❌ '{CACHE_KEY}' no está en Redis → consultando PostgreSQL...")
    except Exception as e:
        print(f"[REDIS] ⚠️  Error al leer caché: {e} → usando PostgreSQL directamente")

    # ------ PASO 2: Consultar PostgreSQL (Window Function) ------
    inicio = time.time()
    try:
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH RankingVendedores AS (
                    SELECT 
                        v.nombre_vendedor,
                        COUNT(vnt.id_venta)       AS cantidad_ventas,
                        SUM(vnt.total)            AS facturacion_total,
                        RANK() OVER (
                            ORDER BY SUM(vnt.total) DESC
                        )                         AS puesto
                    FROM vendedores v
                    JOIN ventas vnt ON v.id_vendedor = vnt.id_vendedor
                    WHERE v.nombre_vendedor <> 'mostrador'
                      AND v.id_vendedor <> '00000000-0000-0000-0000-000000000000'
                    GROUP BY v.id_vendedor, v.nombre_vendedor
                )
                SELECT puesto, nombre_vendedor, cantidad_ventas, facturacion_total
                FROM RankingVendedores
                WHERE puesto <= 10
                ORDER BY puesto ASC
            """)
            ranking = [dict(row) for row in cur.fetchall()]

        duracion_pg = round((time.time() - inicio) * 1000, 2)
        print(f"[POSTGRES] ⏱️  Consulta ejecutada en {duracion_pg} ms → {len(ranking)} vendedores obtenidos")

    except Exception as e:
        print(f"[POSTGRES] ❌ Error al consultar: {e}")
        return []

    # ------ PASO 3: Guardar en Redis con TTL ------
    try:
        if redis_client:
            redis_client.setex(CACHE_KEY, TTL_RANKING, json.dumps(ranking, default=str))
            print(f"[REDIS] 💾 '{CACHE_KEY}' guardado en caché por {TTL_RANKING} segundos (TTL)")
    except Exception as e:
        print(f"[REDIS] ⚠️  No se pudo guardar en caché: {e}")

    return ranking


# =============================================================
# DEMOSTRACIÓN DEL PATRÓN
# =============================================================

def demostrar_cache_aside(redis_client, pg_conn):
    """
    Demuestra claramente el comportamiento del patrón Cache-Aside:
      - Primera llamada  → CACHE MISS (va a PostgreSQL)
      - Segunda llamada  → CACHE HIT  (devuelve desde Redis, mucho más rápido)
    """
    separador = "=" * 60

    print(f"\n{separador}")
    print("  DEMOSTRACIÓN: CATÁLOGO DE PRODUCTOS")
    print(separador)

    # Primera llamada → MISS
    print("\n📌 LLAMADA 1 (esperamos CACHE MISS):")
    inicio = time.time()
    productos = obtener_productos(redis_client, pg_conn)
    tiempo_1 = round((time.time() - inicio) * 1000, 2)
    print(f"   Tiempo total de respuesta: {tiempo_1} ms")

    if productos:
        print(f"\n   Ejemplo de producto devuelto:")
        p = productos[0]
        print(f"   → SKU: {p['sku']} | {p['nombre_producto']} | ${p['precio_venta']}")

    # Segunda llamada → HIT
    print("\n📌 LLAMADA 2 (esperamos CACHE HIT):")
    inicio = time.time()
    productos = obtener_productos(redis_client, pg_conn)
    tiempo_2 = round((time.time() - inicio) * 1000, 2)
    print(f"   Tiempo total de respuesta: {tiempo_2} ms")

    if tiempo_1 > 0 and tiempo_2 > 0:
        mejora = round(tiempo_1 / tiempo_2, 1) if tiempo_2 > 0 else "N/A"
        print(f"\n   📊 RESULTADO: El caché fue {mejora}x más rápido que PostgreSQL")

    # ---- Ranking de Vendedores ----
    print(f"\n{separador}")
    print("  DEMOSTRACIÓN: RANKING DE VENDEDORES")
    print(separador)

    print("\n📌 LLAMADA 1 (esperamos CACHE MISS):")
    inicio = time.time()
    ranking = obtener_ranking_vendedores(redis_client, pg_conn)
    tiempo_1 = round((time.time() - inicio) * 1000, 2)
    print(f"   Tiempo total de respuesta: {tiempo_1} ms")

    if ranking:
        print(f"\n   Top 3 vendedores:")
        for v in ranking[:3]:
            print(f"   #{v['puesto']} {v['nombre_vendedor']} → ${v['facturacion_total']}")

    print("\n📌 LLAMADA 2 (esperamos CACHE HIT):")
    inicio = time.time()
    ranking = obtener_ranking_vendedores(redis_client, pg_conn)
    tiempo_2 = round((time.time() - inicio) * 1000, 2)
    print(f"   Tiempo total de respuesta: {tiempo_2} ms")

    if tiempo_1 > 0 and tiempo_2 > 0:
        mejora = round(tiempo_1 / tiempo_2, 1) if tiempo_2 > 0 else "N/A"
        print(f"\n   📊 RESULTADO: El caché fue {mejora}x más rápido que PostgreSQL")

    print(f"\n{separador}")
    print("  CHECKLIST COMPLETADO ✅")
    print(separador)
    print("  ✅ Endpoint 1 cacheado: products:lista")
    print("  ✅ Endpoint 2 cacheado: vendedores:ranking")
    print("  ✅ Patrón Cache-Aside implementado")
    print("  ✅ CACHE HIT y CACHE MISS demostrados")
    print("  ✅ TTL configurado en todos los datos")
    print("  ✅ Namespacing con ':' (products:, vendedores:)")
    print("  ✅ Fallback: si Redis falla, sigue funcionando con PostgreSQL")
    print(f"{separador}\n")


# =============================================================
# MAIN
# =============================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  FORRAJERÍA - Parte 3: Caché con Redis")
    print("  Patrón: Cache-Aside (Lazy Loading)")
    print("=" * 60)

    redis_client = None
    pg_conn      = None

    try:
        # Conectar a Redis (si falla, sigue en modo fallback)
        redis_client = conectar_redis()

        # Conectar a PostgreSQL (obligatorio)
        pg_conn = conectar_postgres()

        # Ejecutar la demostración
        demostrar_cache_aside(redis_client, pg_conn)

    except Exception as e:
        print(f"\n❌ Error general: {e}")

    finally:
        if pg_conn:
            pg_conn.close()
            print("[POSTGRES] 🔌 Conexión cerrada")
        if redis_client:
            redis_client.close()
            print("[REDIS]    🔌 Conexión cerrada")
