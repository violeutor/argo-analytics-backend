# Argo Analytics – Backend

## Purpose
Das Backend stellt die API, Datenpipelines, Scoring-Logik, Unternehmensanalyse und Supabase-Integration bereit. Es dient als Motor der gesamten Plattform.

## Tech Stack
- Node.js (Express) oder Python (FastAPI)
- Supabase (Postgres)
- REST API
- Railway / Fly.io Deployment

## Structure
/src  
  /routes – API-Endpunkte  
  /controllers – Request-Handling  
  /services – Business-Logik  
  /models – Datenmodelle  
  /pipelines – Analyse- und Scoring-Pipelines  
  /integrations – externe APIs (z. B. Finanzdaten)  
/tests – Unit Tests  
/docs – API-Dokumentation  

## Roadmap
- Grundgerüst API
- Supabase CRUD
- Scoring-Pipeline
- Unternehmensanalyse
- Alerts & Signals
- Deployment auf Railway/Fly.io

