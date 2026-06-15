"""
╔══════════════════════════════════════════════════════════════╗
║           WonderEvolution v2.2 — Arquivo único               ║
║  IA que cria projetos reais a partir do que você escreve     ║
╚══════════════════════════════════════════════════════════════╝
"""

# ══════════════════════════════════════════════════════════════
# IMPORTS
# ══════════════════════════════════════════════════════════════
import os, sys, re, gc, json, time, random, hashlib, sqlite3
import zipfile, tarfile, threading, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Generator, List, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from groq import Groq


# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
ANDROID_STORAGE = Path("/storage/emulated/0")
TERMUX_HOME     = Path.home()

WONDER_DIR    = ANDROID_STORAGE / "wonderevolution"
DB_PATH       = WONDER_DIR / "wonderevolution.db"
OUTPUT_DIR    = WONDER_DIR / "wonder_outputs"
SCRIPTS_DIR   = WONDER_DIR / "wonder_scripts"
CODES_DIR     = WONDER_DIR / "wonder_codes"
STATE_FILE    = WONDER_DIR / ".wonder_state.json"
AGENTS_FILE   = WONDER_DIR / ".wonder_agents.json"
SKILLS_FILE   = WONDER_DIR / ".wonder_skills.json"
SCRIPTS_STATE = WONDER_DIR / ".wonder_scripts_state.json"
THOUGHTS_LOG  = WONDER_DIR / "thoughts.jsonl"

MAX_FILE_SIZE_MB = 2
MAX_FILE_SIZE    = MAX_FILE_SIZE_MB * 1024 * 1024
CHUNK_SIZE       = 512 * 1024

GROQ_MODEL_FAST  = "llama-3.1-8b-instant"
GROQ_MODEL_SMART = "llama-3.3-70b-versatile"
RETRY_DELAYS     = [2, 5, 10, 30, 60]

MAX_IDEAS      = 12
MAX_EVOLUTIONS = 4
MAX_PROJECTS   = 5
MAX_CODES      = 4
MAX_SCRIPTS    = 5
MAX_AGENTS     = 6
MAX_WORKERS    = 3   # paralelas (respeita rate limit Groq)

PORT     = int(os.environ.get("PORT", 7860))
VERSION  = "2.2.0"
APP_NAME = "WonderEvolution"

TEXT_EXTS    = {".txt",".md",".json",".csv",".log",".py",".js",".html",
                ".xml",".yaml",".yml",".rst",".ts",".jsx",".tsx",".sh",".toml"}
ARCHIVE_EXTS = {".zip",".tar",".gz",".tgz"}
SKIP_DIRS    = {"Android","node_modules",".git","__pycache__","proc","sys",
                ".cache","venv",".venv","dist","build","wonderevolution"}


# ══════════════════════════════════════════════════════════════
# BANCO DE DADOS
# ══════════════════════════════════════════════════════════════
def _now() -> str:
    """Horário local — corrige bug de timezone do SQLite (que usa UTC)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _db_conn():
    WONDER_DIR.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

_db_lock = threading.Lock()

def db_init():
    c = _db_conn()
    c.cursor().executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            file_path TEXT, file_size INTEGER,
            status TEXT DEFAULT 'processing', summary TEXT,
            chunks_total INTEGER DEFAULT 0, chunks_done INTEGER DEFAULT 0,
            zip_file TEXT
        );
        CREATE TABLE IF NOT EXISTS thoughts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            question TEXT, context_hint TEXT,
            reasoning TEXT, decision TEXT, confidence TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );
        CREATE TABLE IF NOT EXISTS ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            name TEXT, concept TEXT, category TEXT,
            depth INTEGER DEFAULT 1, parent_id INTEGER, tags TEXT DEFAULT '[]',
            FOREIGN KEY (session_id) REFERENCES sessions(id),
            FOREIGN KEY (parent_id) REFERENCES ideas(id)
        );
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            idea_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            name TEXT, description TEXT, status TEXT DEFAULT 'seed',
            plan TEXT, output_file TEXT,
            FOREIGN KEY (idea_id) REFERENCES ideas(id)
        );
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            name TEXT, language TEXT, description TEXT, output_file TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );
        CREATE TABLE IF NOT EXISTS diagnoses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            date TEXT, emotional_tone TEXT, dominant_themes TEXT DEFAULT '[]',
            cognitive_patterns TEXT, evolution_note TEXT, raw_insight TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );
        CREATE TABLE IF NOT EXISTS evolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            from_idea_id INTEGER, to_idea_id INTEGER, trigger TEXT, narrative TEXT,
            FOREIGN KEY (from_idea_id) REFERENCES ideas(id),
            FOREIGN KEY (to_idea_id) REFERENCES ideas(id)
        );
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            level TEXT DEFAULT 'info', message TEXT, context TEXT DEFAULT '{}'
        );
    """)
    c.commit(); c.close()

def db_log(msg: str, level: str = "info", ctx: dict = None):
    with _db_lock:
        c = _db_conn()
        c.execute("INSERT INTO logs (created_at,level,message,context) VALUES (?,?,?,?)",
                  (_now(), level, msg, json.dumps(ctx or {})))
        c.commit(); c.close()

def db_create_session(file_path: str, file_size: int) -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute("INSERT INTO sessions (created_at,file_path,file_size) VALUES (?,?,?)",
                        (_now(), file_path, file_size))
        sid = cur.lastrowid; c.commit(); c.close(); return sid

def db_update_session(sid: int, **kw):
    if not kw: return
    with _db_lock:
        c = _db_conn()
        c.execute(f"UPDATE sessions SET {', '.join(k+' = ?' for k in kw)} WHERE id=?",
                  list(kw.values())+[sid]); c.commit(); c.close()

def db_save_thought(sid: int, question: str, context_hint: str,
                    reasoning: str, decision: str, confidence: str = "media") -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute(
            "INSERT INTO thoughts (created_at,session_id,question,context_hint,reasoning,decision,confidence) VALUES (?,?,?,?,?,?,?)",
            (_now(), sid, question[:400], context_hint[:300], reasoning, decision, confidence))
        tid = cur.lastrowid; c.commit(); c.close()
    _persist_thought(sid, question, reasoning, decision, confidence)
    return tid

def _persist_thought(sid, question, reasoning, decision, confidence):
    try:
        WONDER_DIR.mkdir(parents=True, exist_ok=True)
        entry = json.dumps({"ts":_now(),"session":sid,"q":question,
                            "r":reasoning,"d":decision,"conf":confidence}, ensure_ascii=False)
        with open(THOUGHTS_LOG, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
    except Exception:
        pass

def db_save_idea(sid, name, concept, category, tags, parent_id=None, depth=1) -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute(
            "INSERT INTO ideas (created_at,session_id,name,concept,category,tags,parent_id,depth) VALUES (?,?,?,?,?,?,?,?)",
            (_now(), sid, name, concept, category, json.dumps(tags), parent_id, depth))
        iid = cur.lastrowid; c.commit(); c.close(); return iid

def db_save_project(idea_id, name, description, plan, output_file) -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute(
            "INSERT INTO projects (created_at,idea_id,name,description,plan,output_file) VALUES (?,?,?,?,?,?)",
            (_now(), idea_id, name, description, plan, output_file))
        pid = cur.lastrowid; c.commit(); c.close(); return pid

def db_save_code(sid, name, language, description, output_file) -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute(
            "INSERT INTO codes (created_at,session_id,name,language,description,output_file) VALUES (?,?,?,?,?,?)",
            (_now(), sid, name, language, description, output_file))
        cid = cur.lastrowid; c.commit(); c.close(); return cid

def db_save_diagnosis(sid, tone, themes, patterns, evolution, raw) -> int:
    with _db_lock:
        c = _db_conn()
        cur = c.execute(
            "INSERT INTO diagnoses (created_at,session_id,date,emotional_tone,dominant_themes,cognitive_patterns,evolution_note,raw_insight) VALUES (?,?,?,?,?,?,?,?)",
            (_now(), sid, _now()[:10], tone, json.dumps(themes), patterns, evolution, raw))
        did = cur.lastrowid; c.commit(); c.close(); return did

def db_save_evolution(from_id, to_id, trigger, narrative):
    with _db_lock:
        c = _db_conn()
        c.execute("INSERT INTO evolutions (created_at,from_idea_id,to_idea_id,trigger,narrative) VALUES (?,?,?,?,?)",
                  (_now(), from_id, to_id, trigger, narrative))
        c.commit(); c.close()

def db_dashboard() -> dict:
    c = _db_conn()
    def q(sql): return [dict(r) for r in c.execute(sql).fetchall()]
    data = dict(
        sessions   = q("SELECT * FROM sessions ORDER BY created_at DESC LIMIT 10"),
        thoughts   = q("SELECT * FROM thoughts ORDER BY created_at DESC LIMIT 25"),
        ideas      = q("SELECT * FROM ideas ORDER BY created_at DESC LIMIT 40"),
        projects   = q("SELECT * FROM projects ORDER BY created_at DESC LIMIT 20"),
        codes      = q("SELECT * FROM codes ORDER BY created_at DESC LIMIT 20"),
        diagnoses  = q("SELECT * FROM diagnoses ORDER BY created_at DESC LIMIT 7"),
        evolutions = q("SELECT * FROM evolutions ORDER BY created_at DESC LIMIT 10"),
        logs       = q("SELECT * FROM logs ORDER BY created_at DESC LIMIT 30"),
        counts     = dict(c.execute("""SELECT
            (SELECT COUNT(*) FROM sessions)  sessions,
            (SELECT COUNT(*) FROM ideas)     ideas,
            (SELECT COUNT(*) FROM projects)  projects,
            (SELECT COUNT(*) FROM diagnoses) diagnoses,
            (SELECT COUNT(*) FROM codes)     codes,
            (SELECT COUNT(*) FROM thoughts)  thoughts
        """).fetchone())
    )
    c.close()
    for i in data["ideas"]:    i["tags"] = json.loads(i.get("tags","[]"))
    for d in data["diagnoses"]: d["dominant_themes"] = json.loads(d.get("dominant_themes","[]"))
    return data


# ══════════════════════════════════════════════════════════════
# MOTOR DE IA
# ══════════════════════════════════════════════════════════════
_groq_client = Groq(api_key=GROQ_API_KEY)
_api_lock    = threading.Semaphore(MAX_WORKERS)  # limita chamadas simultâneas

_ADJ  = ["Espiral","Sombra","Névoa","Pulso","Raiz","Chama","Eco","Vórtex",
         "Deriva","Fissura","Órbita","Crisálida","Limiar","Ressonância",
         "Maré","Cerne","Brecha","Fluxo","Teia","Âmago","Centelha","Labirinto"]
_NOUN = ["Viva","Profunda","Silenciosa","Intensa","Latente","Selvagem",
         "Oculta","Urgente","Nômade","Visceral","Fugaz","Densa","Aberta",
         "Cíclica","Estranha","Fértil","Áspera","Dupla","Íntima","Contínua"]
_JSON_FENCE = re.compile(r'^```(?:json)?\s*', re.IGNORECASE | re.MULTILINE)
_FENCE_END  = re.compile(r'\s*```\s*$')

def human_name(base: str = "") -> str:
    adj = random.choice(_ADJ); noun = random.choice(_NOUN)
    if base and len(base.strip()) > 3:
        return f"{base.strip().split()[0].capitalize()} {noun}"
    return f"{adj} {noun}"

def clean_json(text: str) -> str:
    t = _JSON_FENCE.sub('', text.strip())
    return _FENCE_END.sub('', t).strip()

def parse_json_safe(text: str):
    cleaned = clean_json(text)
    try: return json.loads(cleaned)
    except Exception:
        m = re.search(r'\{[\s\S]*\}', cleaned)
        if m:
            try: return json.loads(m.group(0))
            except Exception: pass
    return None

def _chat(messages: list, model: str = GROQ_MODEL_FAST, max_tokens: int = 2048) -> str:
    last_err = None
    for i, delay in enumerate(RETRY_DELAYS):
        try:
            with _api_lock:
                r = _groq_client.chat.completions.create(
                    model=model, messages=messages,
                    max_tokens=max_tokens, temperature=0.85)
            return r.choices[0].message.content.strip()
        except Exception as e:
            last_err = e
            recoverable = any(w in str(e).lower() for w in
                ["connection","timeout","network","resolve","refused","reset","rate","429","502","503"])
            db_log(f"Tentativa {i+1} falhou: {str(e)[:120]}", "warn")
            if not recoverable:
                raise
            time.sleep(delay)
    raise last_err

# ── Pensamento vivo ────────────────────────────────────────────
def ai_think(session_id: int, question: str, context: str) -> dict:
    """A IA raciocina de verdade antes de agir e salva o pensamento."""
    result = _chat([
        {"role":"system","content":
            "Você é uma IA que pensa em voz alta antes de agir. "
            "Raciocine com profundidade e honestidade. APENAS JSON válido."},
        {"role":"user","content":
            f"Questão: {question}\n\nContexto:\n{context[:2000]}\n\n"
            'JSON:\n{"raciocinio":"passo a passo honesto","decisao":"o que fazer",'
            '"confianca":"alta|media|baixa","observacoes":"o que pode ser importante depois"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=700)
    parsed = parse_json_safe(result) or {}
    reasoning  = parsed.get("raciocinio", result[:300])
    decision   = parsed.get("decisao", "continuar")
    confidence = parsed.get("confianca", "media")
    db_save_thought(session_id, question, context[:200], reasoning, decision, confidence)
    return parsed

# ── Análise de fragmento — foco em conteúdo real ──────────────
def ai_analyze_chunk(chunk_text: str, chunk_idx: int, total: int, ctx: str = "") -> dict:
    result = _chat([
        {"role":"system","content":
            "Analise o texto e extraia informações úteis para CRIAR PROJETOS e CÓDIGO. "
            "Foque em ideias práticas, problemas a resolver, padrões de interesse. "
            "Não faça análise de personalidade ou tipologia. APENAS JSON válido."},
        {"role":"user","content":
            f"Fragmento {chunk_idx+1}/{total}. Contexto anterior: {ctx[:200] or 'Início'}\n\n"
            f"TEXTO:\n{chunk_text[:5000]}\n\n"
            'JSON:\n{"temas":["temas concretos presentes"],'
            '"ideias_para_criar":["ideias de projetos ou ferramentas que emergem"],'
            '"problemas_detectados":["problemas que poderiam ser resolvidos"],'
            '"tecnologias_mencionadas":["linguagens, frameworks, ferramentas mencionadas"],'
            '"emocao_dominante":"como o texto soa (curioso/tenso/energético/reflexivo/...)",'
            '"padroes":["padrões de pensamento ou interesse repetidos"],'
            '"resumo":"síntese em 2 frases do conteúdo concreto"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=800)
    p = parse_json_safe(result)
    if p and isinstance(p, dict):
        return {
            "temas":                [str(x) for x in p.get("temas",[]) if x],
            "ideias_brutas":        [str(x) for x in p.get("ideias_para_criar",[]) if x],
            "problemas_detectados": [str(x) for x in p.get("problemas_detectados",[]) if x],
            "tecnologias":          [str(x) for x in p.get("tecnologias_mencionadas",[]) if x],
            "emocao_dominante":     str(p.get("emocao_dominante","neutro")),
            "padroes":              [str(x) for x in p.get("padroes",[]) if x],
            "resumo":               str(p.get("resumo", result[:200])),
        }
    return {"temas":[],"ideias_brutas":[],"problemas_detectados":[],"tecnologias":[],
            "emocao_dominante":"neutro","padroes":[],"resumo":result[:200]}

# ── Síntese — foco em criar projetos reais ────────────────────
def ai_synthesize(analyses: list) -> dict:
    combined = json.dumps(analyses, ensure_ascii=False)[:10000]
    result = _chat([
        {"role":"system","content":
            "Você vai criar projetos reais e código a partir do que foi analisado. "
            "NÃO faça análise de personalidade, tipologia MBTI, ou diagnóstico psicológico. "
            "Foque em: o que pode ser CONSTRUÍDO, CRIADO, PROGRAMADO, ESCRITO com base no conteúdo. "
            "Seja concreto, específico, acionável. APENAS JSON válido."},
        {"role":"user","content":
            f"Análises:\n{combined}\n\n"
            "Retorne JSON com pelo menos 10 ideias e 5 projetos CONCRETOS:\n"
            '{\n'
            '  "tom_geral": "como o conteúdo soa no geral (1 linha)",\n'
            '  "interesses_reais": ["interesse concreto 1", "interesse 2"],\n'
            '  "o_que_quer_construir": "descrição do que a pessoa claramente quer criar ou resolver",\n'
            '  "evolucao_detectada": "o que está mudando ou crescendo no pensamento",\n'
            '  "ideias_para_criar": [\n'
            '    {"nome": "nome único", "conceito": "descrição concreta em 2 frases",\n'
            '     "categoria": "app|ferramenta|script|site|sistema|jogo|api|bot|extensao|automacao|escrita|outro",\n'
            '     "linguagem_sugerida": "python|javascript|html|bash|nenhuma",\n'
            '     "tags": ["tag1","tag2"]}\n'
            '  ],\n'
            '  "projetos_sugeridos": [\n'
            '    {"nome": "nome do projeto", "descricao": "o que é e pra que serve",\n'
            '     "tipo_saida": "codigo|documento|sistema|api|ferramenta|escrita",\n'
            '     "tecnologia": "Python/FastAPI|React|Bash|Markdown|etc",\n'
            '     "plano": "passo 1\\npasso 2\\npasso 3"}\n'
            '  ],\n'
            '  "oportunidades_codigo": [\n'
            '    {"nome": "NomeDoArquivo", "linguagem": "python|javascript|bash|html",\n'
            '     "descricao": "o que este código vai fazer exatamente"}\n'
            '  ],\n'
            '  "mensagem_direta": "frase direta e honesta sobre o que o conteúdo revela que a pessoa quer fazer"\n'
            '}'}
    ], model=GROQ_MODEL_SMART, max_tokens=5000)
    p = parse_json_safe(result)
    return p if (p and isinstance(p, dict)) else {
        "tom_geral":"","interesses_reais":[],"o_que_quer_construir":"",
        "evolucao_detectada":"","ideias_para_criar":[],"projetos_sugeridos":[],
        "oportunidades_codigo":[],"mensagem_direta":""
    }

# ── Planejamento inteligente ───────────────────────────────────
def ai_plan_creation(session_id: int, synthesis: dict, temas: list) -> dict:
    context = (
        f"O que quer construir: {synthesis.get('o_que_quer_construir','')}\n"
        f"Interesses reais: {', '.join(temas[:8])}\n"
        f"Projetos sugeridos: {len(synthesis.get('projetos_sugeridos',[]))}\n"
        f"Oportunidades de código: {len(synthesis.get('oportunidades_codigo',[]))}"
    )
    thought = ai_think(session_id,
        f"Dado este perfil, quais {MAX_AGENTS} tipos de agentes especializados maximizariam "
        f"a criação de projetos e código úteis? Que linguagens priorizar?",
        context)
    result = _chat([
        {"role":"system","content":"Planejador de criação. APENAS JSON válido."},
        {"role":"user","content":
            f"Contexto:\n{context}\nRaciocínio: {thought.get('raciocinio','')[:250]}\n\n"
            'JSON:\n{"n_ideias":10,"n_projetos":4,'
            '"linguagens_codigo":["python","javascript"],'
            '"n_scripts":4,"foco":"o que priorizar nesta sessão"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=400)
    plan = parse_json_safe(result) or {}
    plan.setdefault("n_ideias", MAX_IDEAS)
    plan.setdefault("n_projetos", MAX_PROJECTS)
    plan.setdefault("linguagens_codigo", ["python","markdown"])
    plan.setdefault("n_scripts", MAX_SCRIPTS)
    return plan

# ── Geração de conteúdo de projeto ────────────────────────────
def ai_generate_project(name: str, description: str, plan: str,
                        tech: str, context: str) -> str:
    return _chat([
        {"role":"system","content":
            f"Crie documentação técnica e conteúdo REAL para o projeto '{name}'. "
            f"Seja concreto: inclua estrutura de arquivos, exemplos de código, "
            f"decisões técnicas, próximos passos. Mínimo 600 palavras. Sem rodeios."},
        {"role":"user","content":
            f"Projeto: {name}\nDescrição: {description}\nTecnologia: {tech}\n"
            f"Plano:\n{plan}\n\nContexto do criador:\n{context[:1500]}\n\n"
            "Crie o documento do projeto. Seja técnico e útil."}
    ], model=GROQ_MODEL_SMART, max_tokens=4000)

# ── Geração de código real ─────────────────────────────────────
def ai_generate_code(name: str, language: str, description: str, context: str) -> str:
    ext_hint = {"python":"# Python","javascript":"// JS","bash":"#!/bin/bash",
                "html":"<!DOCTYPE html>","markdown":"# Markdown"}.get(language,"")
    return _chat([
        {"role":"system","content":
            f"Gere código {language} REAL, funcional e completo para '{name}'. "
            f"O código deve funcionar. Inclua comentários úteis. Sem markdown wrapper."},
        {"role":"user","content":
            f"Nome: {name}\nDescrição: {description}\n"
            f"Contexto de uso: {context[:1200]}\n\n"
            f"Gere apenas o código {language} completo agora."}
    ], model=GROQ_MODEL_FAST, max_tokens=2500)

# ── Evolução de ideia ──────────────────────────────────────────
def ai_evolve_idea(name: str, concept: str, existing: list) -> dict:
    others = "\n".join(f"- {i['name']}: {i['concept'][:60]}" for i in existing[-6:])
    result = _chat([
        {"role":"system","content":"Evolua a ideia em direção mais concreta e construível. APENAS JSON."},
        {"role":"user","content":
            f'Ideia "{name}": {concept}\nJá existem:\n{others or "Nenhuma"}\n\n'
            'JSON:\n{"novo_nome":"nome único","novo_conceito":"versão mais desenvolvida e construível",'
            '"categoria":"app|ferramenta|script|sistema|escrita","tags":[],'
            '"narrativa_da_evolucao":"como cresceu — 1 frase"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=500)
    p = parse_json_safe(result)
    return p if (p and isinstance(p, dict)) else {
        "novo_nome":human_name(name),"novo_conceito":f"Evolução prática de: {concept[:120]}",
        "categoria":"projeto","tags":[],"narrativa_da_evolucao":"cresceu em complexidade"
    }


# ══════════════════════════════════════════════════════════════
# LEITOR DE ARQUIVOS
# ══════════════════════════════════════════════════════════════
def fr_load_state() -> dict:
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except Exception: pass
    return {}

def fr_save_state(state: dict):
    try:
        WONDER_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    except Exception: pass

def fr_hash(path: Path, n: int = 4096) -> str:
    h = hashlib.md5(); size = path.stat().st_size
    h.update(str(size).encode())
    with open(path,"rb") as f:
        h.update(f.read(n))
        if size > n*2: f.seek(size-n); h.update(f.read(n))
    return h.hexdigest()

def fr_new_offset(path: Path, state: dict) -> int:
    key = str(path)
    if key not in state: return 0
    prev = state[key].get("size",0); cur = path.stat().st_size
    return prev if cur > prev else 0

def fr_update_state(path: Path, state: dict):
    try:
        state[str(path)] = {"size":path.stat().st_size,
                            "last_read":_now(),"hash":fr_hash(path)}
        fr_save_state(state)
    except Exception: pass

def fr_list_files(search_paths=None) -> list:
    if search_paths is None: search_paths = [ANDROID_STORAGE, TERMUX_HOME]
    out = []
    for base in search_paths:
        base = Path(base)
        if not base.exists(): continue
        try:
            for item in base.rglob("*"):
                if any(p in item.parts for p in SKIP_DIRS): continue
                if not item.is_file(): continue
                try: size = item.stat().st_size
                except Exception: continue
                if size == 0 or size > MAX_FILE_SIZE: continue
                ext = item.suffix.lower()
                if ext in TEXT_EXTS or ext in ARCHIVE_EXTS:
                    out.append({"path":str(item),"name":item.name,"size":size,
                                "size_mb":round(size/1024/1024,2),"ext":ext,
                                "modified":datetime.fromtimestamp(item.stat().st_mtime).isoformat()})
        except PermissionError: continue
    out.sort(key=lambda x: x["modified"], reverse=True)
    return out

def fr_scan_folder(folder_path: str) -> List[dict]:
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir(): return []
    found = []
    try:
        for item in sorted(folder.rglob("*")):
            if any(p in item.parts for p in SKIP_DIRS): continue
            if not item.is_file(): continue
            ext = item.suffix.lower()
            if ext not in TEXT_EXTS and ext not in ARCHIVE_EXTS: continue
            try: size = item.stat().st_size
            except Exception: continue
            if size == 0 or size > MAX_FILE_SIZE: continue
            found.append({"path":str(item),"name":item.name,"size":size,
                          "size_mb":round(size/1024/1024,2),"ext":ext,
                          "modified":datetime.fromtimestamp(item.stat().st_mtime).isoformat(),
                          "rel":str(item.relative_to(folder))})
    except (PermissionError, OSError): pass
    found.sort(key=lambda x: x["modified"], reverse=True)
    return found

def _extract_archive(path: Path) -> str:
    texts = []
    try:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path,"r") as zf:
                for name in zf.namelist():
                    if any(name.endswith(e) for e in [".txt",".md",".py",".js",".json",".csv"]):
                        try: texts.append(f"\n=== {name} ===\n{zf.read(name).decode('utf-8',errors='replace')}")
                        except Exception: pass
        elif path.suffix.lower() in (".tar",".gz",".tgz"):
            with tarfile.open(path,"r:*") as tf:
                for m in tf.getmembers():
                    if m.isfile() and any(m.name.endswith(e) for e in [".txt",".md",".py"]):
                        try:
                            f = tf.extractfile(m)
                            if f: texts.append(f"\n=== {m.name} ===\n{f.read().decode('utf-8',errors='replace')}")
                        except Exception: pass
    except Exception as e: return f"[Erro ao extrair: {e}]"
    return "\n".join(texts)

def fr_read_chunks(path: Path, from_offset: int = 0) -> Generator:
    path = Path(path); ext = path.suffix.lower()
    if ext in ARCHIVE_EXTS:
        text = _extract_archive(path)[from_offset:]
        for i in range(0, max(len(text),1), CHUNK_SIZE): yield text[i:i+CHUNK_SIZE]
        return
    try:
        with open(path,"rb") as f:
            if from_offset > 0: f.seek(from_offset)
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk: break
                yield chunk.decode("utf-8", errors="replace")
    except (PermissionError, OSError) as e:
        yield f"[Erro: {e}]"

def fr_read_folder_chunks(folder_path: str, max_files: int = 200) -> Generator:
    files = fr_scan_folder(folder_path)
    if not files: return
    for idx, finfo in enumerate(files[:max_files]):
        path = Path(finfo["path"]); ext = finfo["ext"]
        header = f"\n\n{'='*55}\n📄 {finfo['rel']}\n{'='*55}\n"
        try:
            if ext in ARCHIVE_EXTS:
                text = _extract_archive(path)
                combined = header + text
                for i in range(0, max(len(combined),1), CHUNK_SIZE):
                    yield combined[i:i+CHUNK_SIZE], finfo, idx, len(files)
            else:
                first = True
                with open(path,"rb") as f:
                    while True:
                        raw = f.read(CHUNK_SIZE)
                        if not raw: break
                        text = raw.decode("utf-8",errors="replace")
                        if first: text = header + text; first = False
                        yield text, finfo, idx, len(files)
        except (PermissionError, OSError):
            yield header+"[sem permissão]", finfo, idx, len(files)
        except Exception as e:
            yield header+f"[erro: {e}]", finfo, idx, len(files)

def fr_save_output(content: str, name: str, subfolder: str = "") -> Path:
    folder = (OUTPUT_DIR/subfolder) if subfolder else OUTPUT_DIR
    folder.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in name)
    path = folder / f"{safe}_{ts}.md"
    path.write_text(content, encoding="utf-8")
    return path

def fr_save_code_file(code: str, name: str, language: str, ext: str) -> Path:
    CODES_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
    path = CODES_DIR / f"{safe}_{ts}{ext}"
    path.write_text(code, encoding="utf-8")
    return path


# ══════════════════════════════════════════════════════════════
# AGENTES — Arquitetura ReAct + Memória + Ferramentas + Meta-Agentes
#
# Baseado em padrões reais de agentes IA:
# • ReAct  (Reason → Act → Observe → loop)
# • Memória episódica persistida em JSON
# • Registro de habilidades (skills) estático + dinâmico
# • Meta-agentes: agentes que criam outros agentes e habilidades
# • Criação paralela com ThreadPoolExecutor
# ══════════════════════════════════════════════════════════════

# ── Habilidades base disponíveis para agentes ─────────────────
SKILLS_REGISTRY = {
    "escrever":        "Criar textos ricos: documentos, ensaios, narrativas, tutoriais",
    "codificar":       "Gerar código funcional em Python, JS, Bash, HTML",
    "planejar":        "Criar planos de ação concretos com etapas numeradas",
    "analisar":        "Analisar texto, código, dados e extrair padrões úteis",
    "sintetizar":      "Comprimir múltiplas fontes em síntese densa e útil",
    "questionar":      "Gerar perguntas profundas que revelam o que está implícito",
    "criticar":        "Encontrar falhas, riscos e pontos cegos em ideias e planos",
    "mapear":          "Criar mapas de conexões entre conceitos e ideias",
    "refletir":        "Auto-avaliar output e melhorar iterativamente",
    "criar_agente":    "[META] Criar um novo agente especializado filho",
    "criar_habilidade":"[META] Definir uma nova habilidade customizada",
    "colaborar":       "Coordenar com outros agentes para tarefas complexas",
    "executar_script": "Solicitar execução de script Python seguro",
    "pesquisar":       "Buscar padrões e referências dentro do conteúdo analisado",
    "prototipar":      "Criar versão mínima viável de um projeto rapidamente",
}

# ── Arquétipos base com habilidades pré-atribuídas ────────────
AGENT_ARCHETYPES = {
    "estrategista": {
        "descricao": "Pensa em sistemas, sequências e prioridades. Cria roteiros.",
        "habilidades": ["planejar","mapear","sintetizar","questionar","colaborar"],
        "pode_criar_agentes": False,
    },
    "codificador": {
        "descricao": "Traduz qualquer ideia em código funcional. Pensa em sistemas.",
        "habilidades": ["codificar","planejar","criticar","prototipar","executar_script"],
        "pode_criar_agentes": False,
    },
    "escritor": {
        "descricao": "Transforma ideias em documentos, tutoriais e textos com voz.",
        "habilidades": ["escrever","sintetizar","refletir","colaborar"],
        "pode_criar_agentes": False,
    },
    "arquiteto": {
        "descricao": "Estrutura projetos e sistemas caóticos em arquitetura clara.",
        "habilidades": ["planejar","mapear","codificar","criticar","prototipar"],
        "pode_criar_agentes": False,
    },
    "crítico": {
        "descricao": "Questiona tudo, encontra falhas, propõe o contrário.",
        "habilidades": ["criticar","questionar","analisar","refletir"],
        "pode_criar_agentes": False,
    },
    "pesquisador": {
        "descricao": "Aprofunda temas, busca conexões não-óbvias, cria referências.",
        "habilidades": ["analisar","pesquisar","mapear","sintetizar","escrever"],
        "pode_criar_agentes": False,
    },
    "construtor": {
        "descricao": "Vai direto ao protótipo. Prefere código funcionando a planos.",
        "habilidades": ["prototipar","codificar","executar_script","refletir"],
        "pode_criar_agentes": False,
    },
    "conector": {
        "descricao": "Liga pontos entre projetos, ideias e agentes. Orquestra.",
        "habilidades": ["mapear","colaborar","sintetizar","questionar"],
        "pode_criar_agentes": True,   # pode criar agentes filhos
    },
    "curador": {
        "descricao": "Seleciona, organiza e contextualiza outputs em coleções.",
        "habilidades": ["sintetizar","escrever","mapear","pesquisar"],
        "pode_criar_agentes": False,
    },
    "explorador": {
        "descricao": "Detecta territórios inexplorados. Propõe o que ninguém pediu.",
        "habilidades": ["questionar","analisar","prototipar","criar_habilidade"],
        "pode_criar_agentes": True,   # meta-agente
    },
    "engenheiro": {
        "descricao": "Resolve problemas técnicos concretos com código e sistemas.",
        "habilidades": ["codificar","planejar","criticar","executar_script","prototipar"],
        "pode_criar_agentes": False,
    },
    "mentor": {
        "descricao": "Documenta o que foi criado, ensina e cria tutoriais.",
        "habilidades": ["escrever","sintetizar","questionar","colaborar","criar_habilidade"],
        "pode_criar_agentes": True,   # pode definir novas habilidades
    },
}


def ag_load_skills() -> dict:
    base = dict(SKILLS_REGISTRY)
    if SKILLS_FILE.exists():
        try:
            custom = json.loads(SKILLS_FILE.read_text())
            base.update(custom)
        except Exception: pass
    return base

def ag_save_skills(skills: dict):
    SKILLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    custom = {k:v for k,v in skills.items() if k not in SKILLS_REGISTRY}
    SKILLS_FILE.write_text(json.dumps(custom, indent=2, ensure_ascii=False))

def ag_load() -> list:
    if AGENTS_FILE.exists():
        try: return json.loads(AGENTS_FILE.read_text())
        except Exception: pass
    return []

def ag_save(agents: list):
    AGENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    AGENTS_FILE.write_text(json.dumps(agents, indent=2, ensure_ascii=False))

def _ag_add_memory(agent: dict, tarefa: str, resultado: str):
    mem = agent.setdefault("memoria", [])
    mem.append({"ts":_now(),"tarefa":tarefa[:200],"resultado":resultado[:300]})
    agent["memoria"] = mem[-10:]  # mantém só 10 últimas

# ── ReAct: núcleo do raciocínio dos agentes ───────────────────
def ag_react(agent: dict, task: str, context: str,
             max_steps: int = 2, heavy: bool = False) -> str:
    """
    Loop ReAct: Reason → Act → Observe → Reason...
    O agente raciocina antes de cada ação e observa o resultado.
    """
    model      = GROQ_MODEL_SMART if heavy else GROQ_MODEL_FAST
    skills     = ag_load_skills()
    avail      = [s for s in agent.get("habilidades",[]) if s in skills]
    mem_ctx    = json.dumps(agent.get("memoria",[])[-3:], ensure_ascii=False)[:400]
    history    = []
    final_result = ""

    for step in range(max_steps + 1):
        is_last = (step == max_steps)

        # ── REASON ──
        reason_resp = _chat([
            {"role":"system","content":
                f"Você é {agent['nome']}, agente {agent['tipo']}.\n"
                f"Sua missão: {agent.get('missao','')}\n"
                f"Habilidades: {avail}\n"
                f"Memória recente: {mem_ctx}\n"
                "Raciocine e decida o próximo passo. APENAS JSON válido."},
            {"role":"user","content":
                f"Tarefa: {task}\nContexto: {context[:1200]}\n"
                f"Passos já dados: {json.dumps(history,ensure_ascii=False)[:400]}\n\n"
                'JSON:\n{"raciocinio":"passo a passo",'
                '"proxima_acao":"' + '|'.join(avail[:8]) + '|concluir",'
                '"o_que_fazer":"instrução concreta para esta ação"}'}
        ], model=GROQ_MODEL_FAST, max_tokens=350)

        reason = parse_json_safe(reason_resp) or {}
        action = reason.get("proxima_acao","concluir")
        instruction = reason.get("o_que_fazer", task)

        if action == "concluir" or is_last:
            # ── ACT FINAL: produz resultado ──
            final_result = _chat([
                {"role":"system","content":
                    f"Você é {agent['nome']}. Personalidade: {agent.get('personalidade','')}\n"
                    f"Especializade: {agent.get('especialidade','')}\n"
                    f"Raciocínio que fez: {reason.get('raciocinio','')}\n"
                    "Produza o resultado final com sua voz. Seja concreto e útil."},
                {"role":"user","content":
                    f"Tarefa: {task}\nContexto: {context[:2500]}\n"
                    f"Passos anteriores: {json.dumps(history,ensure_ascii=False)[:500]}"}
            ], model=model, max_tokens=2000)
            break

        # ── ACT ──
        act_resp = _chat([
            {"role":"system","content":
                f"Execute a ação: {action} ({skills.get(action,'')})\n"
                f"Você é {agent['nome']}. Seja específico e útil."},
            {"role":"user","content":
                f"Instrução: {instruction}\nContexto: {context[:1000]}"}
        ], model=GROQ_MODEL_FAST, max_tokens=700)

        history.append({"passo":step+1,"acao":action,"observacao":act_resp[:250]})

    return final_result

# ── Reflexão: agente avalia seu próprio output ────────────────
def ag_reflect(agent: dict, task: str, output: str) -> str:
    """Agente lê seu output e decide se precisa melhorar."""
    resp = _chat([
        {"role":"system","content":
            f"Você é {agent['nome']}. Avalie seu próprio output e melhore se necessário."},
        {"role":"user","content":
            f"Tarefa original: {task}\n\nSeu output:\n{output[:1500]}\n\n"
            "O output atende a tarefa com qualidade? "
            "Se sim, retorne-o melhorado. Se não, reescreva completamente."}
    ], model=GROQ_MODEL_FAST, max_tokens=2000)
    return resp

# ── Meta: agente define nova habilidade ───────────────────────
def ag_define_skill(agent: dict, purpose: str, context: str) -> Optional[dict]:
    """Agente com 'criar_habilidade' define uma nova skill customizada."""
    if "criar_habilidade" not in agent.get("habilidades",[]):
        return None
    resp = _chat([
        {"role":"system","content":
            f"Você é {agent['nome']}. Defina uma nova habilidade para o sistema. APENAS JSON."},
        {"role":"user","content":
            f"Propósito: {purpose}\nContexto: {context[:600]}\n\n"
            'JSON:\n{"nome":"slug_curto","titulo":"Título da Habilidade",'
            '"descricao":"o que faz em 1 linha","quando_usar":"situações de uso"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=350)
    skill = parse_json_safe(resp) or {}
    if not skill.get("nome"): return None
    skill["criado_por"] = agent.get("nome","?")
    skill["created_at"] = _now()
    all_skills = ag_load_skills()
    all_skills[skill["nome"]] = skill.get("descricao","")
    ag_save_skills(all_skills)
    db_log(f"Nova habilidade: {skill.get('titulo',skill.get('nome',''))}", "info")
    return skill

# ── Meta: agente cria agente filho ────────────────────────────
def ag_create_child(parent: dict, task: str, context: str) -> Optional[dict]:
    """Meta-agente cria um agente filho especializado para a tarefa."""
    if not parent.get("pode_criar_agentes"): return None
    resp = _chat([
        {"role":"system","content":
            f"Você é {parent['nome']}, criando um agente filho especializado. APENAS JSON."},
        {"role":"user","content":
            f"Para realizar: {task}\nContexto: {context[:600]}\n\n"
            'JSON:\n{"nome":"nome humano único","tipo_custom":"tipo-especialista",'
            '"missao":"missão específica e concreta",'
            '"habilidades":["habilidade1","habilidade2","habilidade3"],'
            '"personalidade":"como age e se comunica",'
            '"especialidade":"em que é excepcional"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=450)
    spec = parse_json_safe(resp) or {}
    if not spec.get("nome"): return None
    habs_valid = [h for h in spec.get("habilidades",[]) if h in ag_load_skills()]
    child = {
        "id": f"custom_{int(time.time())}_{random.randint(100,999)}",
        "nome": spec["nome"],
        "tipo": spec.get("tipo_custom","especialista"),
        "missao": spec.get("missao",""),
        "habilidades": habs_valid or ["analisar","escrever"],
        "personalidade": spec.get("personalidade","direto e focado"),
        "especialidade": spec.get("especialidade",""),
        "limitacoes": "criado para uma tarefa específica",
        "memoria": [],
        "filhos": [],
        "criado_por": parent.get("id","sistema"),
        "custom": True,
        "pode_criar_agentes": False,
        "runs": 0,
        "active": True,
        "created_at": _now(),
    }
    parent.setdefault("filhos",[]).append(child["id"])
    agents = ag_load()
    agents.append(child)
    ag_save(agents)
    db_log(f"Agente filho criado: {child['nome']} ({child['tipo']}) por {parent['nome']}", "info")
    return child

# ── Spawn: cria múltiplos agentes em paralelo ─────────────────
def ag_spawn(session_id: int, synthesis: dict, analyses: list) -> list:
    existing       = ag_load()
    existing_names = {a.get("nome") for a in existing}
    existing_tipos = {a.get("tipo") for a in existing}

    temas    = list({t for a in analyses for t in a.get("temas",[])})
    interesses = synthesis.get("interesses_reais",[])
    construir  = synthesis.get("o_que_quer_construir","")
    context_snip = (
        f"O que quer construir: {construir[:300]}\n"
        f"Interesses: {', '.join(interesses[:6])}\n"
        f"Temas: {', '.join(temas[:6])}"
    )

    # A IA decide quais agentes criar (incluindo tipos customizados)
    thought = ai_think(session_id,
        f"Quais {MAX_AGENTS} agentes especializados criar para maximizar a produção de "
        f"projetos e código úteis para este perfil? Considere archetypes disponíveis "
        f"({list(AGENT_ARCHETYPES.keys())}) e tipos completamente novos.",
        context_snip)

    plan_resp = _chat([
        {"role":"system","content":
            "Planeje quais agentes criar. Pode incluir tipos novos além dos archetypes. "
            "APENAS JSON válido."},
        {"role":"user","content":
            f"Contexto:\n{context_snip}\n"
            f"Archetypes disponíveis: {list(AGENT_ARCHETYPES.keys())}\n"
            f"Já existem tipos: {list(existing_tipos)}\n"
            f"Raciocínio: {thought.get('raciocinio','')[:300]}\n\n"
            f"Crie {MAX_AGENTS} agentes únicos (pode misturar archetypes com tipos novos).\n"
            'JSON:\n{"agentes":[{"tipo":"arquétipo_ou_novo",'
            '"missao_especifica":"missão concreta para este projeto",'
            '"habilidades_extras":["habilidade adicional"]}]}'}
    ], model=GROQ_MODEL_FAST, max_tokens=700)

    agent_plans = (parse_json_safe(plan_resp) or {}).get("agentes",[])
    if not agent_plans:
        # fallback: usa archetypes que não existem ainda
        fallbacks = [t for t in AGENT_ARCHETYPES if t not in existing_tipos]
        random.shuffle(fallbacks)
        agent_plans = [{"tipo":t,"missao_especifica":"","habilidades_extras":[]}
                       for t in fallbacks[:MAX_AGENTS]]

    skills_all = ag_load_skills()
    new_agents = []
    new_names_lock = threading.Lock()

    def _build_one(plan: dict) -> Optional[dict]:
        tipo  = plan.get("tipo","estrategista")
        base  = AGENT_ARCHETYPES.get(tipo, {
            "descricao": f"Agente especializado em {tipo}",
            "habilidades": ["analisar","escrever","planejar"],
            "pode_criar_agentes": False,
        })
        missao_extra = plan.get("missao_especifica","")
        habs_extra   = [h for h in plan.get("habilidades_extras",[]) if h in skills_all]
        habs = list(dict.fromkeys(base.get("habilidades",[]) + habs_extra))

        resp = _chat([
            {"role":"system","content":
                "Crie um agente com personalidade única e real. "
                "Nome humano inventado — não robótico. APENAS JSON válido."},
            {"role":"user","content":
                f"Tipo: {tipo} — {base.get('descricao','')}\n"
                f"Contexto do criador:\n{context_snip[:700]}\n"
                f"Missão específica: {missao_extra or 'definir com base no contexto'}\n"
                f"Habilidades: {habs}\n\n"
                'JSON:\n{"nome":"nome humano criativo (ex: Renata Pulso)",'
                '"missao":"missão concreta e específica para este projeto",'
                '"personalidade":"como age — direto, irreverente, metódico, curioso...","'
                'especialidade":"em que é excepcional neste contexto",'
                '"limitacoes":"o que não faz propositalmente"}'}
        ], model=GROQ_MODEL_FAST, max_tokens=400)

        spec = parse_json_safe(resp) or {}
        nome = spec.get("nome","")
        if not nome or len(nome) < 3: return None

        with new_names_lock:
            if nome in existing_names: return None
            existing_names.add(nome)

        agent = {
            "id": f"{tipo}_{int(time.time())}_{random.randint(10,99)}",
            "nome": nome, "tipo": tipo,
            "missao": spec.get("missao",""),
            "personalidade": spec.get("personalidade",""),
            "especialidade": spec.get("especialidade",""),
            "limitacoes": spec.get("limitacoes",""),
            "habilidades": habs,
            "pode_criar_agentes": base.get("pode_criar_agentes", False),
            "memoria": [],
            "filhos": [],
            "criado_por": "sistema",
            "custom": tipo not in AGENT_ARCHETYPES,
            "runs": 0, "active": True,
            "created_at": _now(),
        }
        db_log(f"Agente criado: {nome} ({tipo})", "info")
        return agent

    # Criação paralela
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_build_one, p) for p in agent_plans[:MAX_AGENTS+2]]
        for fut in as_completed(futures):
            try:
                a = fut.result()
                if a: new_agents.append(a)
            except Exception as e:
                db_log(f"Agente falhou: {e}", "warn")

    # Meta-agentes criam filhos se ainda abaixo do mínimo
    if len(new_agents) < 2 and existing:
        meta = next((a for a in existing if a.get("pode_criar_agentes")), None)
        if meta:
            child = ag_create_child(meta,
                f"ajudar a construir: {construir[:200]}", context_snip)
            if child: new_agents.append(child); return new_agents

    ag_save(existing + new_agents)
    return new_agents

# ── Executa agente com memória e reflexão ─────────────────────
def ag_run(agent: dict, task: str, context: str, heavy: bool = False,
           reflect: bool = False) -> str:
    result = ag_react(agent, task, context, max_steps=2, heavy=heavy)
    if reflect and result:
        result = ag_reflect(agent, task, result)
    _ag_add_memory(agent, task, result)
    # salva memória atualizada
    agents = ag_load()
    for a in agents:
        if a.get("id") == agent.get("id"):
            a["memoria"]  = agent.get("memoria",[])
            a["runs"]     = a.get("runs",0) + 1
            a["last_run"] = _now()
    ag_save(agents)
    return result

# ── Colaboração: projeto criado por múltiplos agentes ─────────
def ag_create_project(name: str, desc: str, plan: str,
                      tech: str, context: str) -> dict:
    agents    = ag_load()
    agent_map = {a.get("tipo"): a for a in agents if a.get("active")}
    sections  = {}

    tasks = {
        "arquiteto":   (f"Estruture o projeto '{name}' ({tech}) em módulos e fases", False),
        "codificador": (f"Crie um exemplo de código real para '{name}' em {tech}", True),
        "estrategista":(f"Defina o roadmap de execução de '{name}' — o que fazer primeiro", False),
        "escritor":    (f"Escreva a documentação de '{name}' com exemplos e casos de uso", True),
        "crítico":     (f"Questione as premissas de '{name}' — o que pode dar errado?", False),
        "construtor":  (f"Crie o MVP mínimo de '{name}' — versão 0.1 funcional", True),
        "engenheiro":  (f"Resolva o problema técnico central de '{name}' em {tech}", True),
    }

    def run_one(tipo_task):
        tipo, (task, heavy) = tipo_task
        if tipo not in agent_map: return tipo, None
        ctx = f"Projeto: {name}\n{desc}\nPlano:\n{plan}\nContexto:\n{context[:800]}"
        try:
            return tipo, ag_run(agent_map[tipo], task, ctx, heavy=heavy)
        except Exception as e:
            db_log(f"Agente {tipo} no projeto: {e}", "warn")
            return tipo, None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(run_one, tasks.items()))

    for tipo, content in results:
        if content: sections[tipo] = content

    # fallback se nenhum agente disponível
    if not sections:
        sections["conteudo"] = ai_generate_project(name, desc, plan, tech, context)

    ts  = datetime.now().strftime("%d/%m/%Y %H:%M")
    out = f"# {name}\n\n> {desc}\n\n**Tecnologia:** {tech}  \n**Criado:** {ts}\n\n---\n\n"
    order = ["arquiteto","estrategista","codificador","construtor","escritor","crítico","engenheiro"]
    for tipo in order + [t for t in sections if t not in order]:
        if tipo in sections:
            label = tipo.replace("_"," ").capitalize()
            out += f"## {label}\n\n{sections[tipo]}\n\n---\n\n"
    out += f"\n_WonderEvolution v{VERSION} · {ts}_\n"

    # meta-agente cria filho especializado no projeto
    meta = next((a for a in agents if a.get("pode_criar_agentes")), None)
    if meta:
        try:
            ag_create_child(meta, f"especializar em {name}", f"{desc}\n{context[:400]}")
        except Exception: pass

    return {"content": out, "sections": list(sections.keys())}


# ══════════════════════════════════════════════════════════════
# SCRIPTFORGE
# ══════════════════════════════════════════════════════════════
MAX_SCRIPT_RT = 30
DANGEROUS = ["subprocess","os.system","exec(","eval(","__import__",
             "shutil.rmtree","os.remove","os.unlink","socket","urllib","requests"]

def sf_load_state() -> dict:
    if SCRIPTS_STATE.exists():
        try: return json.loads(SCRIPTS_STATE.read_text())
        except Exception: pass
    return {}

def sf_save_state(s: dict):
    SCRIPTS_STATE.parent.mkdir(parents=True, exist_ok=True)
    SCRIPTS_STATE.write_text(json.dumps(s, indent=2, ensure_ascii=False))

def sf_generate(context: str, purpose: str) -> dict:
    result = _chat([
        {"role":"system","content":
            "Crie um script Python útil usando apenas stdlib. "
            "Deve funcionar de verdade. APENAS JSON válido."},
        {"role":"user","content":
            f"Contexto do projeto: {context[:1500]}\nPropósito: {purpose}\n\n"
            'JSON:\n{"nome":"nome_slug","titulo_humano":"Título do Script",'
            '"descricao":"o que faz em 1 linha","codigo":"código Python completo e funcional"}'}
    ], model=GROQ_MODEL_FAST, max_tokens=2000)
    return parse_json_safe(result) or {}

def sf_save(data: dict) -> Path:
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    nome = data.get("nome","script").replace(" ","_")
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    header = (f"# WonderEvolution — {_now()}\n"
              f"# {data.get('titulo_humano','')}\n"
              f"# {data.get('descricao','')}\n\n")
    path = SCRIPTS_DIR / f"{nome}_{ts}.py"
    path.write_text(header + data.get("codigo",""), encoding="utf-8")
    return path

def sf_observe(path: Path) -> dict:
    code   = path.read_text(encoding="utf-8", errors="replace")
    issues = [d for d in DANGEROUS if d in code]
    return {"safe":not issues,"issues":issues,"lines":code.count("\n"),
            "hash":hashlib.md5(code.encode()).hexdigest()[:10]}

def sf_run(path: Path) -> dict:
    obs = sf_observe(path)
    if not obs["safe"]:
        db_log(f"Script bloqueado: {path.name}","warn",{"issues":obs["issues"]})
        return {"success":False,"reason":"unsafe","issues":obs["issues"]}
    state = sf_load_state(); key = str(path)
    if state.get(key,{}).get("runs",0) >= 3:
        return {"success":False,"reason":"max_runs_reached"}
    try:
        r = subprocess.run([sys.executable,str(path)],capture_output=True,
                           text=True,timeout=MAX_SCRIPT_RT,cwd=str(TERMUX_HOME))
        state[key] = {"runs":state.get(key,{}).get("runs",0)+1,
                      "last_run":_now(),"last_success":r.returncode==0}
        sf_save_state(state)
        return {"success":r.returncode==0,"output":r.stdout[:2000],"error":r.stderr[:1000]}
    except subprocess.TimeoutExpired:
        return {"success":False,"reason":"timeout"}
    except Exception as e:
        return {"success":False,"reason":str(e)}

def sf_list() -> list:
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    state = sf_load_state()
    out = []
    for p in sorted(SCRIPTS_DIR.glob("*.py"),key=lambda x:x.stat().st_mtime,reverse=True):
        obs = sf_observe(p)
        out.append({"path":str(p),"name":p.name,"size":p.stat().st_size,
                    "modified":datetime.fromtimestamp(p.stat().st_mtime).isoformat(),
                    "safe":obs["safe"],"lines":obs["lines"],
                    "runs":state.get(str(p),{}).get("runs",0)})
    return out

def sf_forge(session_id: int, synthesis: dict, analyses: list) -> list:
    context = " ".join(a.get("resumo","") for a in analyses[:4])
    construir = synthesis.get("o_que_quer_construir","")
    temas     = synthesis.get("interesses_reais",[])

    thought = ai_think(session_id,
        f"Quais {MAX_SCRIPTS} scripts Python seriam mais úteis para construir o que a pessoa quer?",
        f"O que quer: {construir[:300]}\nTemas: {temas[:5]}")

    purposes = [
        f"organizar/indexar arquivos e projetos relacionados a: {construir[:150]}",
        f"automatizar tarefa relacionada a: {', '.join(temas[:3])}",
        "criar estrutura de pastas e arquivos para um novo projeto",
        f"processar e analisar dados do projeto: {construir[:100]}",
        f"script utilitário específico: {thought.get('decisao','')[:150]}",
    ]

    generated = []
    sf_lock = threading.Lock()

    def _gen_one(purpose):
        try:
            data = sf_generate(context, purpose)
            if data and data.get("codigo"):
                path = sf_save(data)
                obs  = sf_observe(path)
                with sf_lock:
                    generated.append({"path":str(path),"name":path.name,
                                      "titulo":data.get("titulo_humano",""),
                                      "descricao":data.get("descricao",""),
                                      "safe":obs["safe"]})
                db_log(f"Script gerado: {path.name}", "info")
        except Exception as e:
            db_log(f"Erro script: {e}", "warn")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(_gen_one, purposes[:MAX_SCRIPTS]))

    return generated


# ══════════════════════════════════════════════════════════════
# ZIPPER
# ══════════════════════════════════════════════════════════════
def zipper_pack(session_id: int) -> str:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = WONDER_DIR / f"wonder_{ts}.zip"
    WONDER_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as zf:
        for folder, prefix in [(OUTPUT_DIR,"wonder_outputs"),
                               (SCRIPTS_DIR,"wonder_scripts"),
                               (CODES_DIR,"wonder_codes")]:
            if folder.exists():
                for f in folder.rglob("*"):
                    if f.is_file():
                        zf.write(f, prefix+"/"+f.relative_to(folder).as_posix())
        if THOUGHTS_LOG.exists(): zf.write(THOUGHTS_LOG,"thoughts.jsonl")
    db_log(f"ZIP criado: {zip_path.name}","info",{"session_id":session_id})
    db_update_session(session_id, zip_file=str(zip_path))
    return str(zip_path)


# ══════════════════════════════════════════════════════════════
# MOTOR DE EVOLUÇÃO — com paralelismo
# ══════════════════════════════════════════════════════════════
def _finalize(session_id, summaries, all_themes, all_patterns,
              all_raw_ideas, first_emotion, is_new_content,
              source_label, state, path, emit) -> dict:

    emit("✨ Síntese final (modelo completo)...", 62)
    compact = [{
        "resumo":         " | ".join(summaries[-25:]),
        "temas":          list(set(all_themes[:25])),
        "padroes":        list(set(all_patterns[:12])),
        "ideias_brutas":  list(set(all_raw_ideas[:18])),
        "emocao_dominante": first_emotion,
    }]
    synthesis = ai_synthesize(compact)
    del compact, summaries; gc.collect()

    temas_unicos = list(set(all_themes))[:12]
    tom          = synthesis.get("tom_geral", first_emotion)
    construir    = synthesis.get("o_que_quer_construir","")
    evolucao     = synthesis.get("evolucao_detectada","")
    mensagem     = synthesis.get("mensagem_direta","")
    interesses   = synthesis.get("interesses_reais",[])

    # Diagnóstico reutiliza campos semânticos novos
    emit("💜 Salvando análise...", 65)
    db_save_diagnosis(session_id, tom, temas_unicos,
                      f"Quer construir: {construir}", evolucao, mensagem)

    # Planejamento pela IA
    emit("🧠 Planejando criação...", 66)
    plan = ai_plan_creation(session_id, synthesis, temas_unicos)
    n_ideias   = min(int(plan.get("n_ideias", MAX_IDEAS)), MAX_IDEAS)
    n_projetos = min(int(plan.get("n_projetos", MAX_PROJECTS)), MAX_PROJECTS)
    langs_code = plan.get("linguagens_codigo", ["python","markdown"])[:MAX_CODES]
    emit(f"📋 {n_ideias} ideias · {n_projetos} projetos · {len(langs_code)} código(s)", 67)

    # Ideias orgânicas (sequencial — rápido)
    emit(f"💡 Criando {n_ideias} ideias...", 69)
    idea_ids = []
    for raw in synthesis.get("ideias_para_criar",[])[:n_ideias]:
        iid = db_save_idea(session_id,
                           raw.get("nome", human_name()),
                           raw.get("conceito",""),
                           raw.get("categoria","projeto"),
                           raw.get("tags",[]), depth=1)
        idea_ids.append(iid)

    # Evoluções em paralelo
    if len(idea_ids) >= 2:
        emit(f"🌱 Evoluindo {MAX_EVOLUTIONS} ideias em paralelo...", 72)
        c_tmp = _db_conn()
        existing_ideas = [dict(r) for r in c_tmp.execute(
            "SELECT name,concept FROM ideas ORDER BY created_at DESC LIMIT 20").fetchall()]
        rows = {pid: dict(r) for pid in idea_ids[:MAX_EVOLUTIONS]
                if (r := c_tmp.execute(
                    "SELECT name,concept,category FROM ideas WHERE id=?", (pid,)).fetchone())}
        c_tmp.close()

        evo_lock = threading.Lock()
        def _evolve_one(pid):
            row = rows.get(pid)
            if not row: return
            try:
                evo = ai_evolve_idea(row["name"], row["concept"], existing_ideas)
                cid = db_save_idea(session_id,
                                   evo.get("novo_nome", human_name()),
                                   evo.get("novo_conceito",""),
                                   evo.get("categoria","projeto"),
                                   evo.get("tags",[]),
                                   parent_id=pid, depth=2)
                db_save_evolution(pid, cid, "análise", evo.get("narrativa_da_evolucao",""))
                with evo_lock:
                    existing_ideas.append({"name":evo.get("novo_nome",""),
                                           "concept":evo.get("novo_conceito","")})
            except Exception as e: db_log(f"Evolução: {e}", "warn")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            list(ex.map(_evolve_one, idea_ids[:MAX_EVOLUTIONS]))

    # Agentes em paralelo (internamente paralelo via ag_spawn)
    emit("🤖 Criando agentes...", 75)
    fake_analyses = [{"temas":temas_unicos,"resumo":construir[:400],
                      "padroes":list(set(all_patterns))[:8],"emocao_dominante":tom}]
    new_agents = []
    try:
        new_agents = ag_spawn(session_id, synthesis, fake_analyses)
        if new_agents: emit(f"🤖 {len(new_agents)} agente(s) criados!", 78)
    except Exception as e: db_log(f"Agentes: {e}","warn")

    # Projetos em paralelo
    emit(f"📁 Criando {n_projetos} projetos em paralelo...", 79)
    context_snip = f"{construir}\n\n{mensagem}".strip()[:1200]
    output_paths = []
    proj_lock    = threading.Lock()
    projetos     = synthesis.get("projetos_sugeridos",[])[:n_projetos]

    def _create_proj(proj):
        pname = proj.get("nome", human_name())
        pdesc = proj.get("descricao","")
        pplan = proj.get("plano","")
        ptech = proj.get("tecnologia","Python")
        idea_ref = idea_ids[0] if idea_ids else db_save_idea(
            session_id, pname, pdesc, "projeto", [])
        try:
            result  = ag_create_project(pname, pdesc, pplan, ptech, context_snip)
            content = result.get("content","")
        except Exception:
            try: content = ai_generate_project(pname, pdesc, pplan, ptech, context_snip)
            except Exception as e2: db_log(f"Projeto {pname}: {e2}","warn"); content = ""
        if content:
            out = fr_save_output(content, pname, subfolder="projetos")
            db_save_project(idea_ref, pname, pdesc, json.dumps(pplan), str(out))
            with proj_lock: output_paths.append(str(out))
        gc.collect()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(_create_proj, projetos))

    # Código em paralelo
    ext_map = {"python":".py","javascript":".js","bash":".sh",
               "html":".html","markdown":".md"}
    emit(f"💻 Gerando {len(langs_code)} arquivo(s) de código em paralelo...", 86)
    code_paths = []
    code_lock  = threading.Lock()
    oportunidades = synthesis.get("oportunidades_codigo",[])

    def _create_code(args):
        i, lang = args
        ext  = ext_map.get(lang,".txt")
        if i < len(oportunidades):
            op    = oportunidades[i]
            cname = op.get("nome", human_name())
            cdesc = op.get("descricao","")
        else:
            cname = human_name()
            cdesc = f"Código {lang} para: {construir[:150]}"
        try:
            code    = ai_generate_code(cname, lang, cdesc, context_snip)
            path_c  = fr_save_code_file(code, cname, lang, ext)
            db_save_code(session_id, cname, lang, cdesc, str(path_c))
            with code_lock: code_paths.append(str(path_c))
            db_log(f"Código: {path_c.name} ({lang})", "info")
        except Exception as e: db_log(f"Código {cname}: {e}","warn")
        gc.collect()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(_create_code, enumerate(langs_code[:MAX_CODES])))

    # Scripts em paralelo
    emit(f"⚙️ Forjando {MAX_SCRIPTS} scripts...", 91)
    generated_scripts = []
    try:
        generated_scripts = sf_forge(session_id, synthesis, fake_analyses)
    except Exception as e: db_log(f"Scripts: {e}","warn")

    # Salva e ZIP
    if path is not None: fr_update_state(path, state)
    db_update_session(session_id, status="done", summary=construir[:500])

    emit("📦 Criando ZIP...", 95)
    zip_path = ""
    try:
        zip_path = zipper_pack(session_id)
        emit(f"✅ ZIP: {Path(zip_path).name}", 98)
    except Exception as e: db_log(f"ZIP: {e}","warn")

    emit(f"✅ {len(idea_ids)} ideias · {len(output_paths)} projetos · "
         f"{len(code_paths)} códigos · {len(new_agents)} agentes · "
         f"{len(generated_scripts)} scripts · ZIP pronto", 100)
    gc.collect()

    return {
        "session_id":      session_id,
        "ideas_created":   len(idea_ids),
        "projects_created":len(output_paths),
        "codes_created":   len(code_paths),
        "agents_created":  len(new_agents),
        "scripts_created": len(generated_scripts),
        "output_files":    output_paths,
        "code_files":      code_paths,
        "zip_file":        zip_path,
        "diagnosis":       {"tone":tom,"themes":temas_unicos[:5],
                            "evolution":evolucao,"message":mensagem},
        "is_new_content":  is_new_content,
    }


def evo_run_file(file_path: str, status_callback=None) -> dict:
    def emit(msg, pct=0):
        db_log(msg,"info")
        if status_callback: status_callback({"message":msg,"percent":pct})
    path = Path(file_path)
    if not path.exists(): return {"error":f"Não encontrado: {file_path}"}
    state = fr_load_state(); size = path.stat().st_size
    offset = fr_new_offset(path, state); is_new = offset > 0
    new_bytes = size - offset
    if is_new and new_bytes <= 0:
        emit("⚡ Nenhum conteúdo novo.",100)
        return {"session_id":None,"message":"Sem conteúdo novo"}
    emit(f"📂 {round(new_bytes/1024/1024 if is_new else size/1024/1024,1)}MB...", 5)
    sid   = db_create_session(file_path, size)
    total = sum(1 for _ in fr_read_chunks(path, from_offset=offset))
    db_update_session(sid, chunks_total=total)
    if total == 0:
        fr_update_state(path, state)
        return {"session_id":sid,"message":"Sem conteúdo legível"}
    emit(f"🧠 {total} fragmento(s)...", 10)
    summaries, themes, patterns, raw_ideas, first_emo = [], [], [], [], "neutro"
    for i, chunk in enumerate(fr_read_chunks(path, from_offset=offset)):
        emit(f"🔍 {i+1}/{total}...", 10+int((i/total)*50))
        a = ai_analyze_chunk(chunk, i, total, summaries[-1] if summaries else "")
        summaries.append(a.get("resumo","")[:300])
        themes.extend(a.get("temas",[])); patterns.extend(a.get("padroes",[]))
        raw_ideas.extend(a.get("ideias_brutas",[]))
        if i == 0: first_emo = a.get("emocao_dominante","neutro")
        del a, chunk; gc.collect()
        db_update_session(sid, chunks_done=i+1)
    return _finalize(sid, summaries, themes, patterns, raw_ideas, first_emo,
                     is_new, str(path), state, path, emit)


def evo_run_folder(folder_path: str, status_callback=None) -> dict:
    def emit(msg, pct=0):
        db_log(msg,"info")
        if status_callback: status_callback({"message":msg,"percent":pct})
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        return {"error":f"Pasta inválida: {folder_path}"}
    emit("🔎 Escaneando...", 2)
    files = fr_scan_folder(folder_path)
    if not files: return {"error":"Nenhum arquivo encontrado"}
    total_mb = sum(f["size_mb"] for f in files)
    emit(f"📂 {len(files)} arquivo(s) · {round(total_mb,1)}MB", 5)
    sid = db_create_session(folder_path, int(total_mb*1024*1024))

    # Coleta chunks sequencialmente (análise por file em paralelo)
    all_chunks_by_file: dict = {}
    for chunk, finfo, fidx, ftotal in fr_read_folder_chunks(folder_path):
        fname = finfo["name"]
        all_chunks_by_file.setdefault(fname, []).append((chunk, finfo, fidx, ftotal))

    total_chunks = sum(len(v) for v in all_chunks_by_file.values())
    db_update_session(sid, chunks_total=total_chunks)
    if total_chunks == 0:
        return {"session_id":sid,"message":"Pasta sem conteúdo legível"}
    emit(f"🧠 {total_chunks} fragmento(s) de {len(files)} arquivo(s)...", 8)

    summaries, themes, patterns, raw_ideas = [], [], [], []
    first_emo = "neutro"; useful = []; skipped = []
    chunk_count = 0; sum_lock = threading.Lock()

    def _analyze_file_chunks(fname_chunks):
        fname, chunks = fname_chunks
        file_summaries = []
        file_useful = False
        for chunk, finfo, fidx, ftotal in chunks:
            ctx = file_summaries[-1] if file_summaries else ""
            a = ai_analyze_chunk(chunk, 0, len(chunks), ctx)
            if a.get("temas") or a.get("ideias_brutas") or a.get("padroes"):
                file_useful = True
            with sum_lock:
                summaries.append(a.get("resumo","")[:300])
                themes.extend(a.get("temas",[]))
                patterns.extend(a.get("padroes",[]))
                raw_ideas.extend(a.get("ideias_brutas",[]))
            file_summaries.append(a.get("resumo","")[:200])
            del a, chunk; gc.collect()
        return fname, file_useful

    emit(f"⚡ Analisando {len(all_chunks_by_file)} arquivo(s) em paralelo...", 10)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_analyze_file_chunks, item): item[0]
                for item in all_chunks_by_file.items()}
        done = 0
        for fut in as_completed(futs):
            fname, file_useful = fut.result()
            done += 1
            if file_useful: useful.append(fname)
            else: skipped.append(fname)
            pct = 10 + int((done/len(all_chunks_by_file))*50)
            emit(f"📄 {done}/{len(all_chunks_by_file)} arquivos analisados...", pct)
            db_update_session(sid, chunks_done=min(done*10, total_chunks))

    if summaries: first_emo = "variado"
    emit(f"✅ {len(useful)} úteis · {len(skipped)} ignorados", 62)
    result = _finalize(sid, summaries, themes, patterns, raw_ideas, first_emo,
                       False, folder_path, fr_load_state(), None, emit)
    result.update({"useful_files":useful,"skipped_files":skipped,
                   "total_files_scanned":len(files)})
    return result


# ══════════════════════════════════════════════════════════════
# APP FASTAPI + DASHBOARD
# ══════════════════════════════════════════════════════════════
_progress: dict = {"message":"Aguardando...","percent":0,"running":False}
_prog_lock = threading.Lock()
def _set_prog(d):
    with _prog_lock: _progress.update(d)
def _get_prog():
    with _prog_lock: return dict(_progress)

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()
    for d in [OUTPUT_DIR, SCRIPTS_DIR, CODES_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    db_log(f"{APP_NAME} v{VERSION} iniciado","info")
    yield

app = FastAPI(title=APP_NAME, version=VERSION, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

def _bg_file(fp):
    _set_prog({"running":True,"percent":0,"message":"Iniciando..."})
    try:
        r = evo_run_file(fp, status_callback=lambda d: _set_prog({**d,"running":True}))
        _set_prog({"running":False,"percent":100,"message":"✅ Concluído!","result":r})
    except Exception as e:
        _set_prog({"running":False,"percent":0,"message":f"❌ {e}","error":str(e)})
        db_log(f"Erro: {e}","error")

def _bg_folder(fp):
    _set_prog({"running":True,"percent":0,"message":"Escaneando..."})
    try:
        r = evo_run_folder(fp, status_callback=lambda d: _set_prog({**d,"running":True}))
        _set_prog({"running":False,"percent":100,"message":"✅ Pasta analisada!","result":r})
    except Exception as e:
        _set_prog({"running":False,"percent":0,"message":f"❌ {e}","error":str(e)})
        db_log(f"Erro pasta: {e}","error")

# ── Endpoints ──────────────────────────────────────────────────
@app.get("/api/status")
def status():
    return {"status":"ok","version":VERSION,"groq":bool(GROQ_API_KEY),
            "storage":str(WONDER_DIR),"agents":len(ag_load()),
            "skills":len(ag_load_skills())}

@app.get("/api/progress")
def progress(): return _get_prog()

@app.get("/api/files")
def list_files():
    try:
        files = fr_list_files(); state = fr_load_state()
        for f in files:
            k = f["path"]; info = state.get(k,{})
            f["already_read"] = k in state
            f["new_bytes"] = max(0,f["size"]-info.get("size",0)) if k in state else f["size"]
            f["new_mb"]    = round(f["new_bytes"]/1024/1024,2)
        return files
    except Exception as e:
        return JSONResponse(status_code=500,content={"error":str(e)})

@app.get("/api/scan-folder")
def scan_folder(path: str = ""):
    if not path: return []
    try:
        files = fr_scan_folder(path)
        return {"folder":path,"files":files,"count":len(files),
                "total_mb":round(sum(f["size_mb"] for f in files),2)}
    except Exception as e:
        return JSONResponse(status_code=500,content={"error":str(e)})

class AnalyzeReq(BaseModel): file_path: str
class FolderReq(BaseModel):  folder_path: str

@app.post("/api/analyze")
def analyze(req: AnalyzeReq, bg: BackgroundTasks):
    if _get_prog().get("running"): raise HTTPException(409,"Análise em andamento")
    if not Path(req.file_path).exists(): raise HTTPException(404,"Arquivo não encontrado")
    bg.add_task(_bg_file, req.file_path)
    return {"message":"Análise iniciada","file":req.file_path}

@app.post("/api/analyze-folder")
def analyze_folder(req: FolderReq, bg: BackgroundTasks):
    if _get_prog().get("running"): raise HTTPException(409,"Análise em andamento")
    if not Path(req.folder_path).exists(): raise HTTPException(404,"Pasta não encontrada")
    bg.add_task(_bg_folder, req.folder_path)
    return {"message":"Análise iniciada","folder":req.folder_path}

@app.get("/api/dashboard")
def dashboard(): return db_dashboard()

@app.get("/api/thoughts")
def get_thoughts(limit: int = 40):
    c = _db_conn()
    rows = [dict(r) for r in c.execute(
        "SELECT * FROM thoughts ORDER BY created_at DESC LIMIT ?",(limit,)).fetchall()]
    c.close(); return rows

@app.get("/api/agents")
def list_agents(): return ag_load()

@app.get("/api/skills")
def list_skills(): return ag_load_skills()

class AgentTaskReq(BaseModel):
    agent_id: str = ""; agent_nome: str = ""
    task: str; context: str = ""; use_heavy: bool = False

@app.post("/api/agents/run")
def run_agent_task(req: AgentTaskReq):
    agents = ag_load()
    agent = next((a for a in agents if
                  a.get("id")==req.agent_id or a.get("nome")==req.agent_nome), None)
    if not agent: raise HTTPException(404,"Agente não encontrado")
    result = ag_run(agent, req.task, req.context, req.use_heavy)
    return {"result":result,"agent":agent.get("nome")}

class CreateChildReq(BaseModel):
    parent_id: str; task: str; context: str = ""

@app.post("/api/agents/create-child")
def create_child_agent(req: CreateChildReq):
    agents = ag_load()
    parent = next((a for a in agents if a.get("id")==req.parent_id), None)
    if not parent: raise HTTPException(404,"Agente pai não encontrado")
    child = ag_create_child(parent, req.task, req.context)
    if not child: raise HTTPException(400,"Este agente não pode criar filhos")
    return child

class DefineSkillReq(BaseModel):
    agent_id: str; purpose: str; context: str = ""

@app.post("/api/agents/define-skill")
def define_skill(req: DefineSkillReq):
    agents = ag_load()
    agent = next((a for a in agents if a.get("id")==req.agent_id), None)
    if not agent: raise HTTPException(404,"Agente não encontrado")
    skill = ag_define_skill(agent, req.purpose, req.context)
    if not skill: raise HTTPException(400,"Este agente não tem a habilidade 'criar_habilidade'")
    return skill

@app.get("/api/scripts")
def list_scripts(): return sf_list()

class RunScriptReq(BaseModel): path: str

@app.post("/api/scripts/run")
def run_script(req: RunScriptReq):
    p = Path(req.path)
    if not p.exists(): raise HTTPException(404,"Script não encontrado")
    return sf_run(p)

@app.get("/api/zips")
def list_zips():
    if not WONDER_DIR.exists(): return []
    return [{"name":f.name,"path":str(f),
             "size_mb":round(f.stat().st_size/1024/1024,2),
             "created":datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")}
            for f in sorted(WONDER_DIR.glob("wonder_*.zip"),
                            key=lambda x: x.stat().st_mtime,reverse=True)]

@app.get("/api/logs")
def get_logs(limit: int = 60):
    c = _db_conn()
    rows = [dict(r) for r in c.execute(
        "SELECT * FROM logs ORDER BY created_at DESC LIMIT ?",(limit,)).fetchall()]
    c.close(); return rows


# ══════════════════════════════════════════════════════════════
# DASHBOARD HTML
# ══════════════════════════════════════════════════════════════
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>WonderEvolution</title>
<style>
  :root{--bg:#06060f;--bg2:#0d0d1a;--card:rgba(255,255,255,.04);--cb:rgba(255,255,255,.07);
    --p:#7c3aed;--v:#8b5cf6;--pk:#a855f7;--cy:#06b6d4;--gr:#10b981;--rd:#ef4444;--am:#f59e0b;
    --tx:#e2e8f0;--mt:#64748b;--r:16px;--rs:10px;
    --font:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
  *{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
  body{background:var(--bg);color:var(--tx);font-family:var(--font);min-height:100vh;
    background-image:radial-gradient(ellipse at 20% 20%,rgba(124,58,237,.15) 0%,transparent 60%),
    radial-gradient(ellipse at 80% 80%,rgba(6,182,212,.08) 0%,transparent 60%)}
  .nav{position:sticky;top:0;z-index:100;background:rgba(6,6,15,.92);
    backdrop-filter:blur(20px);border-bottom:1px solid var(--cb);padding:12px 16px;
    display:flex;align-items:center;justify-content:space-between}
  .nav-brand{display:flex;align-items:center;gap:10px}
  .nav-logo{width:32px;height:32px;border-radius:8px;
    background:linear-gradient(135deg,var(--p),var(--cy));
    display:flex;align-items:center;justify-content:center;font-size:16px}
  .nav-title{font-size:16px;font-weight:700;
    background:linear-gradient(90deg,var(--v),var(--cy));
    -webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .nav-badge{font-size:10px;background:var(--p);color:#fff;padding:2px 8px;
    border-radius:20px;font-weight:600}
  .tabs{display:flex;gap:4px;overflow-x:auto;padding:10px 16px 0;scrollbar-width:none}
  .tabs::-webkit-scrollbar{display:none}
  .tab{flex-shrink:0;padding:7px 13px;border-radius:20px;border:1px solid transparent;
    background:var(--card);color:var(--mt);font-size:12px;font-weight:500;
    cursor:pointer;transition:all .2s}
  .tab.active{background:linear-gradient(135deg,var(--p),var(--v));color:#fff}
  .tab:hover:not(.active){color:var(--tx);border-color:var(--cb)}
  .page{display:none;padding:14px;animation:fi .2s ease}
  .page.active{display:block}
  @keyframes fi{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}
  .card{background:var(--card);border:1px solid var(--cb);border-radius:var(--r);
    padding:14px;margin-bottom:10px}
  .ct{font-size:11px;font-weight:600;color:var(--mt);text-transform:uppercase;
    letter-spacing:.08em;margin-bottom:10px;display:flex;align-items:center;gap:6px}
  .ct .dot{width:6px;height:6px;border-radius:50%;
    background:linear-gradient(var(--p),var(--cy))}
  .stats{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px}
  .stat{background:var(--card);border:1px solid var(--cb);border-radius:var(--rs);
    padding:12px;text-align:center}
  .sv{font-size:24px;font-weight:800;background:linear-gradient(135deg,var(--v),var(--cy));
    -webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .sl{font-size:10px;color:var(--mt);margin-top:2px}
  .pw{background:rgba(255,255,255,.06);border-radius:99px;height:5px;overflow:hidden;margin:6px 0}
  .pb{height:100%;border-radius:99px;background:linear-gradient(90deg,var(--p),var(--cy));
    transition:width .4s ease;width:0%}
  .pc{background:linear-gradient(135deg,rgba(124,58,237,.1),rgba(6,182,212,.05));
    border:1px solid rgba(124,58,237,.2);border-radius:var(--r);padding:14px;margin-bottom:10px}
  .pm{font-size:13px;color:var(--tx);margin-bottom:4px;min-height:18px;word-break:break-word}
  .pp{font-size:11px;color:var(--mt);text-align:right}
  .mode-toggle{display:flex;gap:6px;margin-bottom:12px}
  .mode-btn{flex:1;padding:9px;border-radius:var(--rs);border:1px solid var(--cb);
    background:var(--card);color:var(--mt);font-size:12px;font-weight:600;
    cursor:pointer;transition:all .2s;text-align:center}
  .mode-btn.active{background:linear-gradient(135deg,var(--p),var(--v));color:#fff;border-color:transparent}
  .folder-s{background:linear-gradient(135deg,rgba(124,58,237,.06),rgba(6,182,212,.04));
    border:1px solid rgba(124,58,237,.18);border-radius:var(--rs);padding:12px;margin-bottom:10px}
  .fhint{font-size:11px;color:var(--mt);margin-top:7px;line-height:1.5}
  .fprev{margin-top:8px;max-height:26vh;overflow-y:auto}
  .ff{display:flex;align-items:center;gap:7px;padding:5px 7px;
    border-bottom:1px solid var(--cb);font-size:11px}
  .ff:last-child{border:none}
  .ffn{flex:1;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .ffs{color:var(--mt);flex-shrink:0;font-size:10px}
  .ffe{font-size:9px;padding:1px 5px;border-radius:99px;
    background:rgba(124,58,237,.15);color:var(--v);flex-shrink:0}
  .file-list{display:flex;flex-direction:column;gap:6px;max-height:42vh;overflow-y:auto}
  .fi{display:flex;align-items:center;gap:9px;padding:10px;background:var(--card);
    border:1px solid var(--cb);border-radius:var(--rs);cursor:pointer;transition:all .2s}
  .fi:hover{border-color:var(--p);background:rgba(124,58,237,.1)}
  .fi.sel{border-color:var(--v);background:rgba(124,58,237,.15)}
  .fn{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .fm{font-size:11px;color:var(--mt);margin-top:2px}
  .fb{font-size:10px;padding:2px 6px;border-radius:99px;font-weight:600;flex-shrink:0}
  .bn{background:rgba(16,185,129,.2);color:var(--gr)}
  .bd{background:rgba(124,58,237,.2);color:var(--v)}
  .br{background:rgba(100,116,139,.15);color:var(--mt)}
  .btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;
    padding:11px 18px;border-radius:var(--rs);border:none;font-size:13px;
    font-weight:600;cursor:pointer;transition:all .2s;width:100%}
  .btn-p{background:linear-gradient(135deg,var(--p),var(--v));color:#fff}
  .btn-p:hover{opacity:.9;transform:scale(.98)}
  .btn-p:disabled{opacity:.4;cursor:not-allowed;transform:none}
  .btn-g{background:var(--card);color:var(--tx);border:1px solid var(--cb)}
  .btn-g:hover{border-color:var(--p)}
  .btn-sm{padding:6px 11px;font-size:11px;border-radius:8px;width:auto}
  textarea,input,select{background:rgba(255,255,255,.05);border:1px solid var(--cb);
    color:var(--tx);border-radius:var(--rs);padding:9px 11px;width:100%;
    font-family:var(--font);font-size:12px;resize:vertical;outline:none;
    transition:border-color .2s}
  textarea:focus,input:focus,select:focus{border-color:var(--p)}
  select option{background:var(--bg2)}
  label{font-size:11px;color:var(--mt);display:block;margin-bottom:5px;font-weight:500}
  .idea-c{background:var(--card);border:1px solid var(--cb);
    border-radius:var(--rs);padding:12px;margin-bottom:7px}
  .idea-h{display:flex;align-items:flex-start;justify-content:space-between;
    gap:7px;margin-bottom:5px}
  .idea-n{font-size:13px;font-weight:700;background:linear-gradient(90deg,var(--v),var(--pk));
    -webkit-background-clip:text;-webkit-text-fill-color:transparent}
  .idea-cat{font-size:9px;padding:2px 7px;border-radius:99px;
    background:rgba(139,92,246,.15);color:var(--v);font-weight:600;flex-shrink:0}
  .idea-tx{font-size:11px;color:var(--mt);line-height:1.5}
  .tags{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
  .tag{font-size:9px;padding:2px 6px;border-radius:99px;
    background:rgba(255,255,255,.05);color:var(--mt)}
  .agent-c{background:linear-gradient(135deg,rgba(124,58,237,.06),rgba(6,182,212,.04));
    border:1px solid rgba(124,58,237,.15);border-radius:var(--rs);padding:12px;margin-bottom:7px}
  .agent-n{font-size:14px;font-weight:700;color:var(--tx);margin-bottom:2px}
  .agent-t{font-size:10px;color:var(--v);font-weight:600;text-transform:uppercase;letter-spacing:.05em}
  .agent-m{font-size:11px;color:var(--mt);margin-top:5px;line-height:1.5}
  .agent-ft{display:flex;align-items:center;justify-content:space-between;margin-top:8px;flex-wrap:wrap;gap:4px}
  .skill-chip{font-size:9px;padding:2px 6px;border-radius:99px;
    background:rgba(6,182,212,.1);color:var(--cy);margin:2px;display:inline-block}
  .meta-badge{background:rgba(168,85,247,.2);color:var(--pk);font-size:9px;
    padding:2px 7px;border-radius:99px;font-weight:700}
  .sc-c{background:var(--card);border:1px solid var(--cb);
    border-radius:var(--rs);padding:10px;margin-bottom:7px}
  .sc-n{font-size:11px;font-weight:600;color:var(--tx);word-break:break-all;margin-bottom:3px}
  .sd{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:3px}
  .sy{background:var(--gr)}.sn{background:var(--rd)}
  .dc{background:linear-gradient(135deg,rgba(168,85,247,.08),rgba(6,182,212,.04));
    border:1px solid rgba(168,85,247,.15);border-radius:var(--r);padding:14px;margin-bottom:10px}
  .dt{font-size:12px;font-weight:700;color:var(--pk);margin-bottom:7px}
  .dtx{font-size:12px;color:var(--tx);line-height:1.6;margin-bottom:6px}
  .dm{font-size:12px;color:var(--mt);font-style:italic;line-height:1.6;
    border-left:2px solid var(--p);padding-left:10px}
  .thought-c{background:rgba(124,58,237,.05);border:1px solid rgba(124,58,237,.12);
    border-radius:var(--rs);padding:12px;margin-bottom:7px}
  .thought-q{font-size:11px;font-weight:700;color:var(--v);margin-bottom:6px}
  .thought-r{font-size:11px;color:var(--tx);line-height:1.6;margin-bottom:6px}
  .thought-d{font-size:11px;color:var(--cy);font-style:italic;
    border-left:2px solid var(--cy);padding-left:8px}
  .conf-alta{background:rgba(16,185,129,.15);color:var(--gr);font-size:9px;
    padding:2px 6px;border-radius:99px;font-weight:600;margin-left:5px}
  .conf-media{background:rgba(245,158,11,.15);color:var(--am);font-size:9px;
    padding:2px 6px;border-radius:99px;font-weight:600;margin-left:5px}
  .conf-baixa{background:rgba(239,68,68,.15);color:var(--rd);font-size:9px;
    padding:2px 6px;border-radius:99px;font-weight:600;margin-left:5px}
  .code-c{background:var(--card);border:1px solid var(--cb);
    border-radius:var(--rs);padding:12px;margin-bottom:7px}
  .code-lang{font-size:9px;padding:2px 7px;border-radius:99px;font-weight:600;
    background:rgba(6,182,212,.15);color:var(--cy);display:inline-block;margin-bottom:4px}
  .zip-c{background:linear-gradient(135deg,rgba(16,185,129,.06),rgba(6,182,212,.04));
    border:1px solid rgba(16,185,129,.15);border-radius:var(--rs);padding:12px;
    margin-bottom:7px;display:flex;align-items:center;justify-content:space-between}
  .li{display:flex;gap:7px;padding:7px;border-bottom:1px solid var(--cb);
    font-size:11px;align-items:flex-start}
  .ll{font-size:9px;font-weight:700;padding:2px 5px;border-radius:4px;flex-shrink:0;margin-top:1px}
  .li-i{background:rgba(6,182,212,.15);color:var(--cy)}
  .li-w{background:rgba(245,158,11,.15);color:var(--am)}
  .li-e{background:rgba(239,68,68,.15);color:var(--rd)}
  .rb{background:rgba(0,0,0,.3);border:1px solid var(--cb);border-radius:var(--rs);
    padding:12px;font-size:11px;line-height:1.7;color:var(--tx);
    white-space:pre-wrap;word-break:break-word;max-height:45vh;overflow-y:auto;margin-top:8px}
  .empty{text-align:center;padding:28px 14px;color:var(--mt)}
  .ei{font-size:36px;margin-bottom:10px;opacity:.4}
  .chip{display:inline-flex;align-items:center;gap:3px;font-size:10px;padding:3px 8px;
    border-radius:99px;background:rgba(124,58,237,.15);color:var(--v);font-weight:500;margin:2px}
  .toast{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(80px);
    background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.3);
    color:var(--gr);padding:9px 18px;border-radius:99px;font-size:12px;
    font-weight:500;transition:transform .3s;z-index:200;backdrop-filter:blur(20px)}
  .toast.show{transform:translateX(-50%) translateY(0)}
  .toast.err{background:rgba(239,68,68,.15);border-color:rgba(239,68,68,.3);color:var(--rd)}
  @keyframes spin{to{transform:rotate(360deg)}}
  .spin{animation:spin 1s linear infinite;display:inline-block}
  .tp{background:var(--card);border:1px solid var(--cb);border-radius:var(--r);
    padding:14px;margin-top:10px}
</style>
</head>
<body>
<div class="nav">
  <div class="nav-brand">
    <div class="nav-logo">✦</div>
    <span class="nav-title">WonderEvolution</span>
  </div>
  <span class="nav-badge" id="groq-status">v2.2</span>
</div>
<div class="tabs">
  <button class="tab active" onclick="showTab('home')">Início</button>
  <button class="tab" onclick="showTab('analyze')">Analisar</button>
  <button class="tab" onclick="showTab('thoughts')">🧠 Pensamentos</button>
  <button class="tab" onclick="showTab('ideas')">Ideias</button>
  <button class="tab" onclick="showTab('projects')">Projetos</button>
  <button class="tab" onclick="showTab('codes')">Códigos</button>
  <button class="tab" onclick="showTab('agents')">Agentes</button>
  <button class="tab" onclick="showTab('scripts')">Scripts</button>
  <button class="tab" onclick="showTab('diagnoses')">Análise</button>
  <button class="tab" onclick="showTab('zips')">ZIPs</button>
  <button class="tab" onclick="showTab('logs')">Logs</button>
</div>

<!-- HOME -->
<div class="page active" id="page-home">
  <div class="stats" id="stats-grid">
    <div class="stat"><div class="sv" id="s-ideas">–</div><div class="sl">Ideias</div></div>
    <div class="stat"><div class="sv" id="s-projects">–</div><div class="sl">Projetos</div></div>
    <div class="stat"><div class="sv" id="s-codes">–</div><div class="sl">Códigos</div></div>
    <div class="stat"><div class="sv" id="s-sessions">–</div><div class="sl">Sessões</div></div>
    <div class="stat"><div class="sv" id="s-diagnoses">–</div><div class="sl">Análises</div></div>
    <div class="stat"><div class="sv" id="s-thoughts">–</div><div class="sl">Pensamentos</div></div>
  </div>
  <div class="card" id="home-progress" style="display:none">
    <div class="ct"><span class="dot"></span>Em andamento</div>
    <div class="pc">
      <div class="pm" id="home-prog-msg">...</div>
      <div class="pw"><div class="pb" id="home-prog-bar"></div></div>
      <div class="pp" id="home-prog-pct">0%</div>
    </div>
  </div>
  <div class="card" id="last-diag-card" style="display:none">
    <div class="ct"><span class="dot"></span>Última análise</div>
    <div id="last-diag-content"></div>
  </div>
  <div class="card">
    <div class="ct"><span class="dot"></span>Últimas ideias</div>
    <div id="home-ideas"></div>
  </div>
</div>

<!-- ANALISAR -->
<div class="page" id="page-analyze">
  <div class="mode-toggle">
    <button class="mode-btn active" id="btn-mode-folder" onclick="setMode('folder')">📁 Pasta</button>
    <button class="mode-btn" id="btn-mode-file" onclick="setMode('file')">📄 Arquivo único</button>
  </div>
  <div id="section-folder">
    <div class="folder-s">
      <label>📂 Caminho da pasta</label>
      <input type="text" id="folder-path" placeholder="/storage/emulated/0/Documentos" style="margin-bottom:7px" oninput="clearFolderPreview()">
      <div style="display:flex;gap:6px">
        <button class="btn btn-g btn-sm" style="flex:1" onclick="previewFolder()">🔍 Ver</button>
        <button class="btn btn-g btn-sm" style="flex:1" onclick="setFolderPath('/storage/emulated/0')">📱 Storage</button>
        <button class="btn btn-g btn-sm" style="flex:1" onclick="setFolderPath('~')">🏠 Home</button>
      </div>
      <div class="fhint">💡 Máx 2MB por arquivo · txt md py js json csv html yaml sh</div>
      <div id="folder-preview-wrap" style="display:none">
        <div style="display:flex;justify-content:space-between;align-items:center;margin:8px 0 5px">
          <span id="folder-preview-count" style="font-size:11px;color:var(--v);font-weight:600"></span>
          <span id="folder-preview-size" style="font-size:10px;color:var(--mt)"></span>
        </div>
        <div class="fprev" id="folder-preview"></div>
      </div>
    </div>
    <button class="btn btn-p" id="btn-folder-analyze" onclick="startFolderAnalysis()">✦ Analisar pasta com IA</button>
  </div>
  <div id="section-file" style="display:none">
    <div class="card">
      <div class="ct"><span class="dot"></span>Selecionar arquivo</div>
      <div style="display:flex;gap:6px;margin-bottom:10px">
        <button class="btn btn-g btn-sm" style="flex:1" onclick="loadFiles()"><span id="files-spin">↻</span> Buscar</button>
        <button class="btn btn-g btn-sm" style="flex:1" onclick="toggleManual()">✏️ Digitar</button>
      </div>
      <div id="manual-input" style="display:none;margin-bottom:10px">
        <label>Caminho completo</label>
        <input type="text" id="manual-path" placeholder="/storage/emulated/0/notas.txt">
      </div>
      <div class="file-list" id="file-list">
        <div class="empty"><div class="ei">📂</div><div class="et">Clique em Buscar</div></div>
      </div>
    </div>
    <div id="selected-info" style="display:none" class="card">
      <div class="ct"><span class="dot"></span>Arquivo selecionado</div>
      <div id="selected-details" style="font-size:12px;color:var(--mt);margin-bottom:10px"></div>
      <button class="btn btn-p" id="btn-analyze" onclick="startFileAnalysis()">✦ Analisar com IA</button>
    </div>
  </div>
  <div id="analyze-progress" style="display:none;margin-top:10px" class="pc">
    <div class="pm" id="prog-msg">Iniciando...</div>
    <div class="pw"><div class="pb" id="prog-bar"></div></div>
    <div class="pp" id="prog-pct">0%</div>
  </div>
  <div id="analyze-result" style="display:none;margin-top:10px" class="card">
    <div class="ct"><span class="dot"></span>Resultado</div>
    <div id="result-content" style="font-size:12px;line-height:1.7"></div>
  </div>
</div>

<!-- PENSAMENTOS -->
<div class="page" id="page-thoughts">
  <div class="card">
    <div class="ct"><span class="dot"></span>Raciocínio da IA antes de cada decisão</div>
    <p style="font-size:11px;color:var(--mt)">Cada pensamento registrado antes de uma ação — o raciocínio real.</p>
  </div>
  <div id="thoughts-list"><div class="empty"><div class="ei">🧠</div><div class="et">Nenhum pensamento ainda</div></div></div>
</div>

<!-- IDEIAS -->
<div class="page" id="page-ideas">
  <div id="ideas-list"><div class="empty"><div class="ei">💡</div><div class="et">Analise para gerar ideias</div></div></div>
</div>

<!-- PROJETOS -->
<div class="page" id="page-projects">
  <div id="projects-list"><div class="empty"><div class="ei">🗂️</div><div class="et">Nenhum projeto ainda</div></div></div>
</div>

<!-- CÓDIGOS -->
<div class="page" id="page-codes">
  <div class="card">
    <div class="ct"><span class="dot"></span>Código gerado pela IA</div>
  </div>
  <div id="codes-list"><div class="empty"><div class="ei">💻</div><div class="et">Nenhum código ainda</div></div></div>
</div>

<!-- AGENTES -->
<div class="page" id="page-agents">
  <div class="card">
    <div class="ct"><span class="dot"></span>Agentes ativos</div>
    <p style="font-size:11px;color:var(--mt);margin-bottom:6px">Agentes com arquitetura ReAct — raciocinam antes de agir, têm memória e podem criar filhos.</p>
    <button class="btn btn-g btn-sm" style="width:auto" onclick="renderAgents()">↻ Atualizar</button>
  </div>
  <div id="agents-list"><div class="empty"><div class="ei">🤖</div><div class="et">Analise primeiro para criar agentes</div></div></div>
  <div class="tp" id="agent-task-panel" style="display:none">
    <div class="ct"><span class="dot"></span>Executar tarefa com agente</div>
    <div style="margin-bottom:8px"><label>Agente</label><select id="agent-select"></select></div>
    <div style="margin-bottom:8px"><label>Tarefa</label>
      <textarea id="agent-task" rows="3" placeholder="O que você quer que o agente faça?"></textarea></div>
    <div style="margin-bottom:8px"><label>Contexto (opcional)</label>
      <textarea id="agent-context" rows="2" placeholder="Informação adicional..."></textarea></div>
    <button class="btn btn-p" onclick="runAgentTask()">▶ Executar agente (ReAct)</button>
    <div id="agent-result" style="display:none" class="rb"></div>
  </div>
</div>

<!-- SCRIPTS -->
<div class="page" id="page-scripts">
  <div class="card">
    <div class="ct"><span class="dot"></span>Scripts Python gerados</div>
    <p style="font-size:11px;color:var(--mt)">Verificados antes de executar.</p>
  </div>
  <div id="scripts-list"><div class="empty"><div class="ei">⚙️</div><div class="et">Nenhum script ainda</div></div></div>
</div>

<!-- ANÁLISE -->
<div class="page" id="page-diagnoses">
  <div id="diagnoses-list"><div class="empty"><div class="ei">🔬</div><div class="et">Nenhuma análise ainda</div></div></div>
</div>

<!-- ZIPs -->
<div class="page" id="page-zips">
  <div class="card">
    <div class="ct"><span class="dot"></span>ZIPs de sessões</div>
    <p style="font-size:11px;color:var(--mt)">/storage/emulated/0/wonderevolution/wonder_*.zip</p>
  </div>
  <div id="zips-list"></div>
</div>

<!-- LOGS -->
<div class="page" id="page-logs">
  <div class="card">
    <div class="ct"><span class="dot"></span>Logs</div>
    <button class="btn btn-g btn-sm" style="width:auto;margin-bottom:8px" onclick="loadLogs()">↻ Atualizar</button>
    <div id="logs-list"></div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
let selectedFile=null,pollInterval=null,currentMode='folder';
const ICONS={'.txt':'📄','.md':'📝','.json':'📋','.csv':'📊','.log':'📃',
             '.zip':'🗜️','.py':'🐍','.js':'⚡','.html':'🌐','.sh':'⚙️'};

function showTab(name){
  const names=['home','analyze','thoughts','ideas','projects','codes',
               'agents','scripts','diagnoses','zips','logs'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',names[i]===name));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  if(['home','ideas','projects','codes','diagnoses'].includes(name)) loadDashboard();
  if(name==='agents') renderAgents();
  if(name==='scripts') loadScripts();
  if(name==='thoughts') loadThoughts();
  if(name==='zips') loadZips();
  if(name==='logs') loadLogs();
}

function setMode(m){
  currentMode=m;
  document.getElementById('btn-mode-folder').classList.toggle('active',m==='folder');
  document.getElementById('btn-mode-file').classList.toggle('active',m==='file');
  document.getElementById('section-folder').style.display=m==='folder'?'':'none';
  document.getElementById('section-file').style.display=m==='file'?'':'none';
}

function toast(msg,isErr=false,dur=2800){
  const el=document.getElementById('toast');
  el.textContent=msg;el.classList.toggle('err',isErr);el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'),dur);
}

function esc(s){
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

async function loadDashboard(){
  try{
    const d=await(await fetch('/api/dashboard')).json();
    const c=d.counts||{};
    document.getElementById('s-ideas').textContent=c.ideas??0;
    document.getElementById('s-projects').textContent=c.projects??0;
    document.getElementById('s-codes').textContent=c.codes??0;
    document.getElementById('s-sessions').textContent=c.sessions??0;
    document.getElementById('s-diagnoses').textContent=c.diagnoses??0;
    document.getElementById('s-thoughts').textContent=c.thoughts??0;
    renderHomeIdeas(d.ideas||[]);
    renderLastDiag(d.diagnoses||[]);
    renderIdeas(d.ideas||[]);
    renderProjects(d.projects||[]);
    renderCodes(d.codes||[]);
    renderDiagnoses(d.diagnoses||[]);
  }catch(e){}
}

function renderLastDiag(diagnoses){
  if(!diagnoses.length)return;
  const d=diagnoses[0];
  document.getElementById('last-diag-card').style.display='';
  document.getElementById('last-diag-content').innerHTML=
    `<div class="dt">Tom: ${esc(d.emotional_tone||'–')}</div>
     <div class="dtx">${esc((d.cognitive_patterns||'').slice(0,250))}</div>
     ${d.raw_insight?'<div class="dm">'+esc(d.raw_insight.slice(0,200))+'</div>':''}`;
}

function ideaHTML(i){
  let tags=[];try{tags=Array.isArray(i.tags)?i.tags:JSON.parse(i.tags||'[]')}catch{}
  return`<div class="idea-c">
    <div class="idea-h"><div class="idea-n">${esc(i.name||'')}</div>
      <div class="idea-cat">${esc(i.category||'ideia')}</div></div>
    <div class="idea-tx">${esc((i.concept||'').slice(0,180))}</div>
    ${tags.length?'<div class="tags">'+tags.map(t=>'<span class="tag">#'+esc(t)+'</span>').join('')+'</div>':''}
  </div>`;
}

function renderHomeIdeas(ideas){
  document.getElementById('home-ideas').innerHTML=ideas.length
    ?ideas.slice(0,4).map(ideaHTML).join('')
    :'<div class="empty"><div class="ei">💡</div><div class="et">Analise para gerar ideias</div></div>';
}
function renderIdeas(ideas){
  document.getElementById('ideas-list').innerHTML=ideas.length
    ?ideas.map(ideaHTML).join('')
    :'<div class="empty"><div class="ei">💡</div><div class="et">Analise para gerar ideias</div></div>';
}
function renderProjects(projects){
  const el=document.getElementById('projects-list');
  if(!projects.length){el.innerHTML='<div class="empty"><div class="ei">🗂️</div><div class="et">Nenhum projeto</div></div>';return}
  el.innerHTML=projects.map(p=>`<div class="card">
    <div class="idea-h"><div class="idea-n">${esc(p.name||'')}</div>
      <span class="chip">${esc(p.status||'seed')}</span></div>
    <div class="idea-tx">${esc((p.description||'').slice(0,200))}</div>
    ${p.output_file?'<div style="font-size:10px;color:var(--mt);margin-top:6px">📄 '+esc(p.output_file)+'</div>':''}
  </div>`).join('');
}
function renderCodes(codes){
  const el=document.getElementById('codes-list');
  if(!codes.length){el.innerHTML='<div class="empty"><div class="ei">💻</div><div class="et">Nenhum código</div></div>';return}
  el.innerHTML=codes.map(c=>`<div class="code-c">
    <div style="font-size:13px;font-weight:700;color:var(--tx);margin-bottom:2px">${esc(c.name||'')}</div>
    <span class="code-lang">${esc(c.language||'')}</span>
    <div style="font-size:11px;color:var(--mt)">${esc((c.description||'').slice(0,160))}</div>
    ${c.output_file?'<div style="font-size:10px;color:var(--mt);margin-top:5px">📄 '+esc(c.output_file)+'</div>':''}
  </div>`).join('');
}
function renderDiagnoses(diagnoses){
  const el=document.getElementById('diagnoses-list');
  if(!diagnoses.length){el.innerHTML='<div class="empty"><div class="ei">🔬</div><div class="et">Nenhuma análise</div></div>';return}
  el.innerHTML=diagnoses.map(d=>{
    let themes=[];try{themes=Array.isArray(d.dominant_themes)?d.dominant_themes:JSON.parse(d.dominant_themes||'[]')}catch{}
    return`<div class="dc">
      <div class="dt">📅 ${(d.date||'').slice(0,10)} · ${esc(d.emotional_tone||'')}</div>
      <div class="dtx">${esc((d.cognitive_patterns||'').slice(0,300))}</div>
      ${d.evolution_note?'<div class="dtx" style="color:var(--v);margin-top:5px">🌱 '+esc(d.evolution_note.slice(0,180))+'</div>':''}
      ${d.raw_insight?'<div class="dm">'+esc(d.raw_insight.slice(0,250))+'</div>':''}
      ${themes.length?'<div class="tags" style="margin-top:6px">'+themes.map(t=>'<span class="tag">'+esc(t)+'</span>').join('')+'</div>':''}
    </div>`;
  }).join('');
}

async function loadThoughts(){
  try{
    const thoughts=await(await fetch('/api/thoughts?limit=40')).json();
    const el=document.getElementById('thoughts-list');
    if(!thoughts.length){el.innerHTML='<div class="empty"><div class="ei">🧠</div><div class="et">Nenhum pensamento ainda</div></div>';return}
    el.innerHTML=thoughts.map(t=>`<div class="thought-c">
      <div class="thought-q">❓ ${esc((t.question||'').slice(0,140))}
        <span class="conf-${t.confidence||'media'}">${t.confidence||'?'}</span>
      </div>
      <div class="thought-r">${esc((t.reasoning||'').slice(0,300))}</div>
      <div class="thought-d">→ ${esc((t.decision||'').slice(0,200))}</div>
      <div style="font-size:9px;color:var(--mt);margin-top:5px">${(t.created_at||'').slice(0,16)}</div>
    </div>`).join('');
  }catch(e){}
}

async function renderAgents(){
  try{
    const agents=await(await fetch('/api/agents')).json();
    const skills=await(await fetch('/api/skills')).json();
    const el=document.getElementById('agents-list');
    if(!agents.length){
      el.innerHTML='<div class="empty"><div class="ei">🤖</div><div class="et">Analise primeiro para criar agentes</div></div>';
      document.getElementById('agent-task-panel').style.display='none';return;
    }
    el.innerHTML=agents.map(a=>{
      const habs=(a.habilidades||[]).slice(0,5).map(h=>`<span class="skill-chip">${esc(h)}</span>`).join('');
      const isMeta=a.pode_criar_agentes;
      const isCustom=a.custom;
      return`<div class="agent-c">
        <div style="display:flex;justify-content:space-between;align-items:flex-start">
          <div>
            <div class="agent-t">${esc(a.tipo||'')}${isMeta?' <span class="meta-badge">META</span>':''}${isCustom?' <span class="meta-badge" style="background:rgba(6,182,212,.2);color:var(--cy)">CUSTOM</span>':''}</div>
            <div class="agent-n">${esc(a.nome||'')}</div>
          </div>
          <span style="font-size:9px;color:var(--mt)">${a.runs||0} runs</span>
        </div>
        <div class="agent-m">${esc((a.missao||'').slice(0,180))}</div>
        <div style="margin-top:6px">${habs}</div>
        ${(a.filhos||[]).length?'<div style="font-size:10px;color:var(--am);margin-top:4px">👶 '+a.filhos.length+' filho(s) criado(s)</div>':''}
        ${(a.memoria||[]).length?'<div style="font-size:10px;color:var(--mt);margin-top:3px">💾 '+a.memoria.length+' memória(s)</div>':''}
      </div>`;
    }).join('');
    document.getElementById('agent-task-panel').style.display='';
    document.getElementById('agent-select').innerHTML=agents.map(a=>
      `<option value="${esc(a.id||a.nome)}">${esc(a.nome)} (${esc(a.tipo)})</option>`).join('');
  }catch(e){}
}

async function runAgentTask(){
  const sel=document.getElementById('agent-select').value;
  const task=document.getElementById('agent-task').value.trim();
  const ctx=document.getElementById('agent-context').value.trim();
  if(!task){toast('Digite uma tarefa!',true);return}
  const btn=document.querySelector('#agent-task-panel .btn-p');
  btn.disabled=true;btn.textContent='⏳ ReAct em andamento...';
  try{
    const r=await fetch('/api/agents/run',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({agent_id:sel,task,context:ctx})});
    const d=await r.json();
    const box=document.getElementById('agent-result');
    box.style.display='';box.textContent=d.result||d.error||JSON.stringify(d);
    toast(`✓ ${d.agent||'Agente'} executou!`);
    renderAgents();
  }catch(e){toast('Erro: '+e.message,true)}
  btn.disabled=false;btn.textContent='▶ Executar agente (ReAct)';
}

async function loadScripts(){
  try{
    const scripts=await(await fetch('/api/scripts')).json();
    const el=document.getElementById('scripts-list');
    if(!scripts.length){el.innerHTML='<div class="empty"><div class="ei">⚙️</div><div class="et">Nenhum script</div></div>';return}
    el.innerHTML=scripts.map(s=>`<div class="sc-c">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
        <div class="sc-n">${esc(s.name)}</div>
        <button class="btn btn-g btn-sm" onclick="runScript(${JSON.stringify(s.path)})">▶</button>
      </div>
      <div style="font-size:10px;color:var(--mt)">
        <span class="sd ${s.safe?'sy':'sn'}"></span>
        ${s.safe?'Seguro':'⚠ Bloqueado'} · ${s.lines} linhas · ${s.runs} execuções</div>
    </div>`).join('');
  }catch(e){}
}

async function runScript(path){
  try{
    const r=await fetch('/api/scripts/run',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({path})});
    const d=await r.json();
    toast(d.success?'✓ Script executado!':'⚠ '+(d.reason||'Erro'),!d.success);
    loadScripts();
  }catch(e){toast('Erro: '+e.message,true)}
}

async function loadZips(){
  try{
    const zips=await(await fetch('/api/zips')).json();
    const el=document.getElementById('zips-list');
    if(!zips.length){el.innerHTML='<div class="empty"><div class="ei">📦</div><div class="et">Aparece após análise</div></div>';return}
    el.innerHTML=zips.map(z=>`<div class="zip-c">
      <div>
        <div style="font-size:12px;font-weight:600;color:var(--tx)">📦 ${esc(z.name)}</div>
        <div style="font-size:10px;color:var(--mt)">${z.size_mb}MB · ${(z.created||'').slice(0,16)}</div>
        <div style="font-size:9px;color:var(--mt);margin-top:2px">${esc(z.path)}</div>
      </div>
    </div>`).join('');
  }catch(e){}
}

async function loadLogs(){
  try{
    const logs=await(await fetch('/api/logs?limit=60')).json();
    document.getElementById('logs-list').innerHTML=logs.map(l=>
      `<div class="li"><span class="ll li-${l.level||'info'}">${(l.level||'info').slice(0,4).toUpperCase()}</span>
       <span style="color:var(--mt);flex:1;word-break:break-word;line-height:1.4">${esc(l.message)}</span>
       <span style="color:var(--mt);opacity:.5;flex-shrink:0;font-size:9px">${(l.created_at||'').slice(11,16)}</span></div>`
    ).join('')||'<div class="empty"><div class="et">Sem logs</div></div>';
  }catch(e){}
}

function toggleManual(){
  const el=document.getElementById('manual-input');
  el.style.display=el.style.display==='none'?'':'none';
}

async function loadFiles(){
  const spin=document.getElementById('files-spin');spin.classList.add('spin');
  const el=document.getElementById('file-list');
  el.innerHTML='<div class="empty"><div class="ei spin">⟳</div></div>';
  try{
    const files=await(await fetch('/api/files')).json();
    el.innerHTML=files.length?files.map(fileHTML).join('')
      :'<div class="empty"><div class="ei">📂</div><div class="et">Nenhum arquivo. Digite o caminho.</div></div>';
    toast(`✓ ${files.length} arquivo(s)`);
  }catch(e){el.innerHTML='<div class="empty"><div class="et">Erro. Use o caminho manual.</div></div>';toggleManual()}
  spin.classList.remove('spin');
}

function fileHTML(f){
  const icon=ICONS[f.ext]||'📄';
  const badge=!f.already_read?'<span class="fb bn">Novo</span>'
    :f.new_mb>0?`<span class="fb bd">+${f.new_mb}MB</span>`
    :'<span class="fb br">Lido</span>';
  return`<div class="fi" onclick='selectFile(${JSON.stringify(f)})'>
    <span style="font-size:18px">${icon}</span>
    <div style="flex:1;min-width:0">
      <div class="fn">${esc(f.name)}</div>
      <div class="fm">${f.size_mb}MB · ${(f.modified||'').slice(0,10)}</div>
    </div>${badge}</div>`;
}

function selectFile(f){
  selectedFile=f;
  document.querySelectorAll('.fi').forEach(el=>el.classList.remove('sel'));
  event?.currentTarget?.classList.add('sel');
  document.getElementById('selected-info').style.display='';
  document.getElementById('selected-details').innerHTML=
    `📄 <strong>${esc(f.name)}</strong><br>${f.size_mb}MB<br>
     <span style="font-size:10px;opacity:.6">${esc(f.path)}</span>`;
}

function setFolderPath(p){
  document.getElementById('folder-path').value=
    p==='~'?'/data/data/com.termux/files/home':p;
  clearFolderPreview();
}
function clearFolderPreview(){document.getElementById('folder-preview-wrap').style.display='none'}

async function previewFolder(){
  const path=document.getElementById('folder-path').value.trim();
  if(!path){toast('Digite o caminho',true);return}
  const wrap=document.getElementById('folder-preview-wrap');
  const prev=document.getElementById('folder-preview');
  prev.innerHTML='<div style="text-align:center;padding:7px;font-size:11px;color:var(--mt)"><span class="spin">⟳</span></div>';
  wrap.style.display='';
  try{
    const r=await fetch('/api/scan-folder?path='+encodeURIComponent(path));
    const d=await r.json();
    if(d.error){prev.innerHTML='<div style="color:var(--rd);font-size:11px;padding:7px">❌ '+esc(d.error)+'</div>';return}
    document.getElementById('folder-preview-count').textContent=`${d.count} arquivo(s)`;
    document.getElementById('folder-preview-size').textContent=`${d.total_mb}MB`;
    prev.innerHTML=(d.files||[]).slice(0,50).map(f=>`<div class="ff">
      <span>${ICONS[f.ext]||'📄'}</span>
      <span class="ffn" title="${esc(f.rel)}">${esc(f.rel||f.name)}</span>
      <span class="ffe">${esc(f.ext)}</span>
      <span class="ffs">${f.size_mb}MB</span>
    </div>`).join('')+(d.count>50?`<div style="font-size:10px;color:var(--mt);padding:7px;text-align:center">…+${d.count-50}</div>`:'');
    toast(`✓ ${d.count} arquivo(s) · ${d.total_mb}MB`);
  }catch(e){prev.innerHTML='<div style="color:var(--rd);font-size:11px;padding:7px">❌ '+esc(e.message)+'</div>'}
}

async function startFolderAnalysis(){
  const path=document.getElementById('folder-path').value.trim();
  if(!path){toast('Digite o caminho!',true);return}
  const btn=document.getElementById('btn-folder-analyze');btn.disabled=true;
  document.getElementById('analyze-progress').style.display='';
  document.getElementById('analyze-result').style.display='none';
  try{
    const r=await fetch('/api/analyze-folder',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({folder_path:path})});
    if(!r.ok){const d=await r.json();throw new Error(d.detail||'Erro')}
    startPolling();toast('✦ Análise iniciada!');
  }catch(e){toast('Erro: '+e.message,true);btn.disabled=false;
    document.getElementById('analyze-progress').style.display='none'}
}

async function startFileAnalysis(){
  const path=selectedFile?.path||document.getElementById('manual-path')?.value.trim();
  if(!path){toast('Selecione um arquivo!',true);return}
  const btn=document.getElementById('btn-analyze');btn.disabled=true;
  document.getElementById('analyze-progress').style.display='';
  document.getElementById('analyze-result').style.display='none';
  try{
    const r=await fetch('/api/analyze',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({file_path:path})});
    if(!r.ok){const d=await r.json();throw new Error(d.detail||'Erro')}
    startPolling();toast('✦ Análise iniciada!');
  }catch(e){toast('Erro: '+e.message,true);btn.disabled=false;
    document.getElementById('analyze-progress').style.display='none'}
}

function startPolling(){
  if(pollInterval)clearInterval(pollInterval);
  pollInterval=setInterval(pollProgress,1500);
  document.getElementById('home-progress').style.display='';
}

async function pollProgress(){
  try{
    const d=await(await fetch('/api/progress')).json();
    ['prog-msg','home-prog-msg'].forEach(id=>{const e=document.getElementById(id);if(e)e.textContent=d.message||'...'});
    ['prog-bar','home-prog-bar'].forEach(id=>{const e=document.getElementById(id);if(e)e.style.width=(d.percent||0)+'%'});
    ['prog-pct','home-prog-pct'].forEach(id=>{const e=document.getElementById(id);if(e)e.textContent=(d.percent||0)+'%'});
    if(!d.running&&d.percent>=100){
      clearInterval(pollInterval);
      ['btn-analyze','btn-folder-analyze'].forEach(id=>{const b=document.getElementById(id);if(b)b.disabled=false});
      document.getElementById('home-progress').style.display='none';
      const r=d.result;
      if(r&&!r.error){
        document.getElementById('analyze-result').style.display='';
        const diag=r.diagnosis||{};
        let html=`<div style="margin-bottom:8px">
          <span class="chip">✦ ${r.ideas_created||0} ideias</span>
          <span class="chip">🗂 ${r.projects_created||0} projetos</span>
          <span class="chip">💻 ${r.codes_created||0} códigos</span>
          <span class="chip">🤖 ${r.agents_created||0} agentes</span>
          <span class="chip">⚙️ ${r.scripts_created||0} scripts</span>
        </div>`;
        if(r.zip_file)html+=`<div style="font-size:11px;color:var(--gr);margin-bottom:7px">📦 ${esc(r.zip_file)}</div>`;
        if(diag.message)html+=`<div style="border-left:2px solid var(--p);padding-left:10px;color:var(--mt);font-style:italic;margin-top:8px">${esc(diag.message)}</div>`;
        document.getElementById('result-content').innerHTML=html;
        toast('✅ Completo!');loadDashboard();
      }else if(r?.message){
        document.getElementById('analyze-result').style.display='';
        document.getElementById('result-content').innerHTML=`<div style="color:var(--mt)">${esc(r.message)}</div>`;
      }else if(d.error){
        document.getElementById('analyze-result').style.display='';
        document.getElementById('result-content').innerHTML=`<div style="color:var(--rd)">❌ ${esc(d.error)}</div>`;
        toast('Erro na análise',true);
      }
    }
  }catch(e){}
}

async function init(){
  try{
    const d=await(await fetch('/api/status')).json();
    const b=document.getElementById('groq-status');
    b.textContent=`v${d.version||'2.2'} · ${d.agents||0} agentes · ${d.groq?'Groq ✓':'Groq ✗'}`;
    if(!d.groq)b.style.background='var(--rd)';
  }catch(e){}
  loadDashboard();
  try{
    const d=await(await fetch('/api/progress')).json();
    if(d.running){document.getElementById('home-progress').style.display='';startPolling()}
  }catch(e){}
  setInterval(()=>{
    const a=document.querySelector('.tab.active');
    if(a&&a.textContent==='Logs')loadLogs();
  },6000);
}
init();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def root(): return HTMLResponse(DASHBOARD_HTML)


if __name__ == "__main__":
    import uvicorn
    if not GROQ_API_KEY:
        print("⚠  GROQ_API_KEY não definida!")
        print("   export GROQ_API_KEY='gsk_...'")
        print()
    print(f"✦  {APP_NAME} v{VERSION}")
    print(f"   Dashboard  → http://localhost:{PORT}")
    print(f"   Storage    → {WONDER_DIR}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
