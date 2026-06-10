# TP n8n — Assistant automatisé d'inscription à un événement

## Cas d'usage choisi
**Gestion d'inscriptions à un événement par email.**

Un participant envoie un email avec le sujet contenant "inscription" et un message décrivant
son nom, l'événement et la date souhaitée. n8n :

1. Détecte le nouvel email (Gmail Trigger).
2. Utilise une IA (OpenAI) pour extraire les infos clés (nom, email, événement, date, message) en JSON.
3. Met en forme les données (node Code).
4. Enregistre l'inscription dans **Google Sheets** (suivi/stockage).
5. Crée un **événement Google Calendar** correspondant.
6. Répond automatiquement à l'expéditeur (confirmation par email).
7. Notifie l'équipe sur **Slack**.

## Interactions couvertes (exigences TP)
- Boîte mail : Gmail (trigger + réponse)
- n8n : extraction IA + mise en forme
- Service de suivi/stockage : Google Sheets
- Autre service externe : Google Calendar + Slack

## Lancer n8n
Service `n8n` ajouté au `docker-compose.yml` (port 5678).

```bash
docker compose up -d n8n
```

Puis ouvrir http://localhost:5678 et créer le compte admin (premier lancement).

## Import du workflow
1. Ouvrir n8n → Workflows → Import from File.
2. Sélectionner `workflow.json`.
3. Configurer les credentials (placeholders à remplacer dans n8n) :
   - `Gmail account` (OAuth2)
   - `OpenAi account` (API key)
   - `Google Sheets account` (OAuth2) + ID de la feuille (`GOOGLE_SHEET_ID`), feuille nommée `Inscriptions` avec colonnes :
     `Nom | Email | Evenement | Date evenement | Message | Date inscription`
   - `Google Calendar account` (OAuth2)
   - `Slack account` (OAuth2/Bot token) + ID du channel (`SLACK_CHANNEL_ID`)
4. Activer le workflow.

## Email de test (démo)
**À :** adresse Gmail connectée
**Objet :** Inscription à l'atelier découverte

**Corps :**
```
Bonjour,

Je souhaite m'inscrire à l'atelier "Découverte n8n" prévu le 2026-06-15.

Cordialement,
Jean Dupont
jean.dupont@example.com
```

## Résultat attendu
- Une ligne ajoutée dans Google Sheets `Inscriptions`.
- Un événement créé dans Google Calendar le 2026-06-15.
- Une réponse automatique envoyée à `jean.dupont@example.com`.
- Un message de notification dans le channel Slack configuré.

## Checklist avant démo
1. `docker compose up -d n8n` puis ouvrir http://localhost:5678.
2. Importer `workflow.json`.
3. Créer/lier credentials : Gmail OAuth2, OpenAI API key, Google Sheets OAuth2, Google Calendar OAuth2, Slack.
4. Créer Google Sheet "Inscriptions" avec entêtes : `Nom | Email | Evenement | Date evenement | Message | Date inscription`. Coller son ID dans le node Google Sheets (`GOOGLE_SHEET_ID`).
5. Récupérer ID du channel Slack, le mettre dans node Slack (`SLACK_CHANNEL_ID`).
6. Activer le workflow (toggle "Active").
7. Envoyer l'email de test depuis une autre adresse vers le compte Gmail connecté.
8. Vérifier : ligne Sheets ajoutée, event Calendar créé, réponse reçue dans la boîte de l'expéditeur, message Slack posté.

## Plan de présentation orale
1. **Contexte** : assistant automatisé pour gérer les inscriptions à un événement reçues par email.
2. **Architecture** : Gmail (entrée) → n8n (orchestration + IA) → Google Sheets (suivi) + Google Calendar + Slack (services externes) → Gmail (réponse auto).
3. **Démo** : envoyer l'email de test, montrer l'exécution dans n8n (vue "Executions"), montrer la ligne ajoutée dans Sheets, l'event dans Calendar, le message Slack et la réponse reçue.
4. **Explication technique** : rôle de chaque node, extraction IA en JSON, fan-out vers 3 services en parallèle.
5. **Limites/améliorations** : gestion des erreurs IA (JSON invalide), filtrage anti-spam, classification multi-types d'emails.
