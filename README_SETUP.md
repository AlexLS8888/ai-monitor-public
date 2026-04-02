# AI Monitor — Setup Guide (Split Repo Architecture)
### Private repo → collects + stores · Public repo → dashboard only

---

## Architecture

```
ai-monitor-private (PRIVÉ)          ai-monitor-public (PUBLIC)
──────────────────────────          ──────────────────────────
ai_monitor_collector.py             dashboard/
data/ai_monitor.db                  ├── index.html
exports/*.csv                       └── data/*.csv  ← pushed by private
.github/workflows/                  .github/workflows/
├── daily_gpu.yml                   └── deploy_pages.yml
└── weekly_rankings.yml
Secrets:
  RUNPOD_API_KEY
  VAST_API_KEY
  PUBLIC_REPO_TOKEN   ← PAT pour écrire dans le repo public
  PUBLIC_REPO_NAME    ← "username/ai-monitor-public"
```

**Flux de données :**
```
RunPod GraphQL ──┐
Vast.ai REST  ──→│─ collector ─→ data/ai_monitor.db  (privé)
OpenRouter HTML─┘               exports/*.csv         (privé)
                                      │
                                      └── push CSVs ──→ public/dashboard/data/
                                                              │
                                                        GitHub Pages ──→ URL publique
```

---

## Setup en 6 étapes

### 1. Créer les deux repos GitHub

```bash
# Repo privé
cd ai-monitor-private/
git init && git add . && git commit -m "feat: AI Monitor private"
# Sur GitHub : créer repo PRIVÉ "ai-monitor-private"
git remote add origin https://github.com/TON_USERNAME/ai-monitor-private.git
git push -u origin main

# Repo public
cd ../ai-monitor-public/
git init && git add . && git commit -m "feat: AI Monitor public dashboard"
# Sur GitHub : créer repo PUBLIC "ai-monitor-public"
git remote add origin https://github.com/TON_USERNAME/ai-monitor-public.git
git push -u origin main
```

---

### 2. Créer le Personal Access Token (PAT)

Le repo privé a besoin d'une clé pour écrire dans le repo public.

**github.com → Settings → Developer settings → Personal access tokens → Tokens (classic)**
- Note : `AI Monitor cross-repo push`
- Expiration : No expiration (ou 1 an)
- Scopes : ☑ **repo** (full control of private repositories)

Copier le token généré `ghp_...`

---

### 3. Ajouter les secrets dans le repo **privé**

**github.com → ai-monitor-private → Settings → Secrets and variables → Actions**

| Secret | Valeur | Source |
|---|---|---|
| `RUNPOD_API_KEY` | `rpa_...` | runpod.io → Settings → API Keys |
| `VAST_API_KEY` | `...` | vast.ai → Account → API Keys |
| `PUBLIC_REPO_TOKEN` | `ghp_...` | PAT créé à l'étape 2 |
| `PUBLIC_REPO_NAME` | `TON_USERNAME/ai-monitor-public` | Nom exact du repo public |

> `RUNPOD_API_KEY` et `VAST_API_KEY` sont gratuits.
> Sans `VAST_API_KEY`, Vast.ai est skippé sans erreur.

---

### 4. Activer GitHub Pages sur le repo **public**

**github.com → ai-monitor-public → Settings → Pages**
- Source : **GitHub Actions**
- Save

URL du dashboard : `https://TON_USERNAME.github.io/ai-monitor-public/`

---

### 5. Autoriser les write permissions dans les deux repos

Pour que les Actions puissent commiter :

**Pour chaque repo → Settings → Actions → General → Workflow permissions**
→ Sélectionner **"Read and write permissions"** → Save

---

### 6. Premier run manuel

**Sur ai-monitor-private → Actions :**

1. `Weekly Rankings Collection` → **Run workflow**
   - Charge les 127 semaines d'historique OpenRouter
   - Pushes les CSVs vers le repo public
   - (~3-5 min)

2. `Daily GPU Collection` → **Run workflow**
   - Première lecture RunPod + Vast.ai
   - (~2 min)

Vérifier que le repo public reçoit bien les CSVs dans `dashboard/data/`.
Le workflow `Deploy Dashboard to GitHub Pages` se déclenche automatiquement.

---

## Schedule automatique

| Workflow (privé) | Quand | Durée | Ce que ça fait |
|---|---|---|---|
| `daily_gpu.yml` | Tous les jours 08:00 UTC | ~2 min | RunPod + Vast.ai + 7d metrics → push CSVs GPU vers public |
| `weekly_rankings.yml` | Lundi 08:00 UTC | ~4 min | OpenRouter → push TOUS les CSVs vers public |

| Workflow (public) | Quand | Durée |
|---|---|---|
| `deploy_pages.yml` | À chaque push sur main | ~1 min |

**Latence totale :** collecte (~3 min) + push vers public + deploy Pages (~1 min) = **~5 min**

---

## Ce qui est public vs privé

| Élément | Privé | Public |
|---|---|---|
| `ai_monitor_collector.py` | ✓ | ✗ |
| `data/ai_monitor.db` | ✓ | ✗ |
| `RUNPOD_API_KEY` | secret | ✗ |
| `VAST_API_KEY` | secret | ✗ |
| `exports/*.csv` | ✓ (source) | ✗ |
| `dashboard/data/*.csv` | ✗ | ✓ (copies propres) |
| `dashboard/index.html` | ✗ | ✓ |

---

## Audit local

```bash
git clone https://github.com/TON_USERNAME/ai-monitor-private.git
cd ai-monitor-private
pip install requests beautifulsoup4
export VAST_API_KEY="..."
export RUNPOD_API_KEY="..."

python ai_monitor_collector.py --audit
python ai_monitor_collector.py --collect-all   # run manual complet
```

---

## Troubleshooting

**"Permission denied" au git push**
→ Settings → Actions → General → Workflow permissions → Read and write permissions

**"Error cloning public repo"**
→ Vérifier que `PUBLIC_REPO_TOKEN` a bien le scope `repo`
→ Vérifier que `PUBLIC_REPO_NAME` est au format `username/repo-name` (pas d'URL)

**Dashboard vide après le premier run**
→ Vérifier que les CSVs sont dans `ai-monitor-public/dashboard/data/`
→ Attendre 2-5 min que GitHub Pages se mette à jour
→ Vider le cache du navigateur (Ctrl+Shift+R)

**Vast.ai : No VAST_API_KEY**
→ Normal si le secret n'est pas encore ajouté. Vast.ai est skippé proprement.
